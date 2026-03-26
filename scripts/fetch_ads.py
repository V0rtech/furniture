#!/usr/bin/env python3
"""
fetch_ads.py
Concurrent Facebook Ads Library scraper — one browser per brand, running in parallel.

Saves to: campaigns/{slug}/ads.json
                          snapshot_urls.txt
                          images/

Edit the CONFIG block and BRANDS list at the top, then run:
    python fetch_ads.py
"""

import json
import re
import sys
import time
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# Force UTF-8 output on Windows (avoids charmap UnicodeEncodeError)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
# ─── PATHS & CONFIG ───────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
DATA_DIR      = BASE_DIR / "data"
CAMPAIGNS_DIR = BASE_DIR / "campaigns"

WORKERS      = 5                # Concurrent browsers
MAX_IMAGES   = 1000              # Max images downloaded per brand
MAX_SCROLLS  = 80               # Safety cap on scroll iterations
SCROLL_PAUSE = 2.5              # Seconds between scrolls
HEADLESS     = True
COUNTRIES    = ["US", "CA", "GB"]   
START_DATE   = "2023-01-01"
MIN_DATE     = datetime(2023, 1, 1)

# Load brands from centralized JSON
BRANDS_FILE = DATA_DIR / "brands.json"
if BRANDS_FILE.exists():
    with open(BRANDS_FILE, "r", encoding="utf-8") as f:
        BRANDS = json.load(f)
else:
    BRANDS = []

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_lock = threading.Lock()


def log(brand: str, msg: str):
    with _lock:
        print(f"[{brand:<20}] {msg}", flush=True)


# ─── URL BUILDER ──────────────────────────────────────────────────────────────

def build_url(brand: dict, country: str) -> str:
    """Build the Ads Library URL for a brand, with date + country filters baked in."""
    params = [
        ("active_status", "all"),           # active AND previously active
        ("ad_type",       "all"),
        ("country",       country),         
        ("is_targeted_country", "false"),
        ("media_type",    "all"),
        ("start_date[min]", START_DATE),    # Jan 2023 floor
        ("sort_data[direction]", "desc"),
        ("sort_data[mode]",      "total_impressions"),
    ]
    if brand.get("page_id"):
        params += [("search_type", "page"), ("view_all_page_id", brand["page_id"])]
    else:
        params += [("search_type", "keyword_unordered"), ("q", brand["name"])]

    qs = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params)
    return "https://www.facebook.com/ads/library/?" + qs


# ─── EXTRACTION JS (runs inside the browser) ──────────────────────────────────

SCRAPE_JS = """
() => {
    const results = [];

    // Each ad card reliably contains exactly one text node "Library ID: XXXXXXXX"
    const idSpans = [...document.querySelectorAll('span, div')]
        .filter(el => el.childElementCount === 0
                   && el.textContent.trim().startsWith('Library ID:'));

    idSpans.forEach((span, idx) => {
        // Walk up the DOM to find the card container (first ancestor taller than 200px)
        let card = span;
        for (let i = 0; i < 15; i++) {
            card = card.parentElement;
            if (!card) break;
            if (card.offsetHeight > 200) break;
        }
        if (!card) return;

        const ad = {
            index: idx + 1,
            id: '',
            body: '',
            images: [],
            snapshot_url: '',
            started: '',
            active: null,
        };

        // Library / Ad ID
        const m = span.textContent.match(/Library ID:\\s*(\\d+)/);
        ad.id = m ? m[1] : ('card_' + String(idx + 1).padStart(4, '0'));

        // Body text — Facebook wraps ad copy in style="white-space: pre-wrap"
        const bodyEls = card.querySelectorAll('[style*="white-space: pre-wrap"]');
        ad.body = [...bodyEls].map(el => el.innerText.trim()).filter(Boolean).join(' | ');

        // Full text fallback (truncated)
        ad.text = card.innerText.slice(0, 600).trim();

        // Images — Facebook serves creatives from the scontent CDN
        // Require a valid image extension in the path (before '?') to filter out
        // truncated URLs that Facebook's JS hasn't finished constructing yet
        ad.images = [...card.querySelectorAll('img')]
            .map(img => img.src.replaceAll('&amp;', '&'))
            .filter(src => {
                if (!src || !src.includes('scontent')) return false;
                const path = src.split('?')[0];
                return /\.(jpg|jpeg|png|webp)$/i.test(path) && src.includes('oh=');
            });

        // Snapshot / detail link
        const links = [...card.querySelectorAll('a[href*="/ads/"]')];
        ad.snapshot_url = links.length ? links[0].href : '';

        // Active status — appears as a standalone line before Library ID
        const activeM = card.innerText.match(/\\n(Active|Inactive)\\n/);
        ad.active = activeM ? activeM[1] === 'Active' : null;

        // Start date (parsed from card text)
        const dateM = card.innerText.match(/Started running on (.+)/);
        ad.started = dateM ? dateM[1].trim() : '';

        results.push(ad);
    });

    return results;
}
"""


# ─── DATE FILTER ──────────────────────────────────────────────────────────────

def parse_started(ad: dict) -> datetime | None:
    text = (ad.get("started") or "") + " " + (ad.get("text") or "")
    m = re.search(r"Started running on (\w+ \d+, \d{4})", text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%B %d, %Y")
        except ValueError:
            pass
    return None


# ─── IMAGE DOWNLOAD ───────────────────────────────────────────────────────────

def detect_ext(url: str) -> str:
    suffix = Path(url.split("?")[0]).suffix.lower()
    return ".png" if suffix == ".png" else ".jpg"


def download_image(url: str, dest: Path) -> bool:
    for attempt in range(3):
        try:
            r = requests.get(
                url, timeout=40, stream=True,
                headers={"User-Agent": UA, "Referer": "https://www.facebook.com/"}
            )
            if r.status_code == 429:
                wait = 2 ** attempt * 3  # 3s, 6s, 12s
                warnings.warn(f"Rate limited — waiting {wait}s before retry")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                warnings.warn(f"HTTP {r.status_code} for {url[:80]}")
                return False
            if r.url != url:
                # Redirected — hash mismatch, Facebook returned error/placeholder
                warnings.warn(f"Redirected (bad hash) for {url[:80]}")
                return False
            ct = r.headers.get("Content-Type", "")
            if "image" not in ct and "octet" not in ct:
                warnings.warn(f"Unexpected Content-Type '{ct}' for {url[:80]}")
                return False
            with open(dest, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
            if dest.stat().st_size < 3_000:
                dest.unlink(missing_ok=True)
                warnings.warn(f"File too small (likely placeholder) for {url[:80]}")
                return False
            return True
        except requests.exceptions.Timeout:
            warnings.warn(f"Timeout on attempt {attempt+1} for {url[:80]}")
            if dest.exists():
                dest.unlink(missing_ok=True)
        except Exception as exc:
            warnings.warn(f"Download error: {exc}")
            if dest.exists():
                dest.unlink(missing_ok=True)
            return False
    return False


# ─── POPUP DISMISSAL ──────────────────────────────────────────────────────────

def dismiss_popups(page):
    for sel in [
        'button:has-text("Allow all cookies")',
        'button:has-text("Decline optional cookies")',
        '[data-testid="cookie-policy-dialog-button"]',
        'button:has-text("Accept")',
        '[aria-label="Close"]',
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=400):
                btn.click()
                time.sleep(0.4)
        except Exception:
            pass


# ─── SCRAPE ONE BRAND ─────────────────────────────────────────────────────────

def scrape_brand(brand: dict, country: str) -> list[dict]:
    name = brand["name"]
    url  = build_url(brand, country)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        ctx = browser.new_context(
            user_agent=UA,
            viewport={"width": 1440, "height": 900},
            locale="en-US",
        )
        page = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        log(name, f"Opening Ads Library ({country})…")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except PWTimeout:
            log(name, "Load timed out — continuing anyway")

        time.sleep(4)
        dismiss_popups(page)

        # ── Scroll until MAX_ADS reached or page exhausted ────────────────────
        prev_count  = 0
        stable      = 0

        for i in range(MAX_SCROLLS):
            page.evaluate("var el = document.scrollingElement || document.body; if (el) window.scrollTo(0, el.scrollHeight)")
            time.sleep(SCROLL_PAUSE)
            dismiss_popups(page)

            count = page.locator('div:has-text("Library ID:")').count()
            log(name, f"Scroll {i+1}: ~{count} cards visible")

            if count == prev_count:
                stable += 1
                if stable >= 3: break
            else:
                stable = 0
            prev_count = count

        # ── Stabilise and Extract ─────────────────────────────────────────────
        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        time.sleep(4)

        # If Facebook redirected us away (login wall, consent) the context dies.
        # Check we're still on the ads library before extracting.
        current_url = page.url
        if "ads/library" not in current_url:
            log(name, f"Redirected — navigating back")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                time.sleep(3)
                dismiss_popups(page)
                page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass

        # ── Extract card data via JS ───────────────────────────────────────────
        raw: list = []
        for attempt in range(3):
            try:
                raw = page.evaluate(SCRAPE_JS) or []
                break
            except Exception as exc:
                log(name, f"JS attempt {attempt+1} failed: {exc}")
                time.sleep(3)

        browser.close()

    # ── Post-process: date filter + cap ───────────────────────────────────────
    out = []
    for ad in raw:
        # Date gate: skip ads older than MIN_DATE (when date is parseable)
        dt = parse_started(ad)
        if dt and dt < MIN_DATE:
            continue

        out.append(ad)

    log(name, f"Collected {len(out)} ads from {country} after filters.")
    return out


# ─── SAVE ONE BRAND ───────────────────────────────────────────────────────────

def save_brand(brand: dict, new_ads: list[dict], country: str):
    name    = brand["name"]
    out_dir = CAMPAIGNS_DIR / brand["slug"]
    img_dir = out_dir / "images"
    ads_file = out_dir / "ads.json"
    
    out_dir.mkdir(parents=True, exist_ok=True)
    img_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load existing ads for merging
    existing_ads = []
    if ads_file.exists():
        try:
            with open(ads_file, "r", encoding="utf-8") as f:
                existing_ads = json.load(f)
        except Exception:
            existing_ads = []

    # Map by ID for quick check
    ad_map = {str(ad.get("id")): ad for ad in existing_ads}

    # 2. Merge new ads
    for ad in new_ads:
        ad_id = str(ad.get("id"))
        if ad_id in ad_map:
            # Update country list
            if "countries" not in ad_map[ad_id]:
                ad_map[ad_id]["countries"] = ["US"]  # assume US for legacy data
            if country not in ad_map[ad_id]["countries"]:
                ad_map[ad_id]["countries"].append(country)

            # Always refresh time-sensitive fields from the latest scrape
            if ad.get("active") is not None:
                ad_map[ad_id]["active"] = ad["active"]
            if ad.get("started"):
                ad_map[ad_id]["started"] = ad["started"]
            if ad.get("text"):
                ad_map[ad_id]["text"] = ad["text"]

            # If body copy was missing/short, update it
            if len(ad.get("body", "")) > len(ad_map[ad_id].get("body", "")):
                ad_map[ad_id]["body"] = ad["body"]
        else:
            # New ad: tag with current country
            ad["countries"] = [country]
            ad_map[ad_id] = ad

    # Save ads.json
    with open(ads_file, "w", encoding="utf-8") as f:
        json.dump(list(ad_map.values()), f, indent=2, ensure_ascii=False)

    # snapshot_urls.txt (full refresh from map)
    with open(out_dir / "snapshot_urls.txt", "w", encoding="utf-8") as f:
        for ad_id, ad in ad_map.items():
            f.write(f"{ad_id} | {ad.get('snapshot_url','')}\n")

    # 3. Images — download only if missing
    downloaded = skipped = failed = 0
    img_count = 0
    for ad in new_ads:
        if img_count >= MAX_IMAGES: break
        ad_id = ad.get("id", f"card_{ad['index']:04d}")
        for j, img_url in enumerate(ad.get("images", [])):
            if img_count >= MAX_IMAGES: break
            suffix   = f"_{j+1}" if j > 0 else ""
            existing = list(img_dir.glob(f"{ad_id}{suffix}.*"))
            if existing:
                skipped += 1
                img_count += 1
                continue
            dest = img_dir / f"{ad_id}{suffix}{detect_ext(img_url)}"
            if download_image(img_url, dest):
                downloaded += 1
                img_count += 1
            else:
                failed += 1

    log(name, f"Images: {downloaded} new, {skipped} existing skipped. Total ads now in {brand['slug']}: {len(ad_map)}")


# ─── PER-BRAND ENTRY POINT ────────────────────────────────────────────────────

def run_brand(brand: dict):
    try:
        all_ads: dict[str, dict] = {}
        for country in COUNTRIES:
            for ad in scrape_brand(brand, country):
                all_ads.setdefault(str(ad["id"]), ad)
            save_brand(brand, list(all_ads.values()), country)
        log(brand["name"], "[OK] Complete")
    except Exception as exc:
        log(brand["name"], f"[FAIL] Error: {exc}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  Facebook Ads Library — Concurrent Scraper")
    print(f"  Brands  : {len(BRANDS)}")
    print(f"  Workers : {WORKERS}  (browsers in parallel)")
    print(f"  Max imgs: {MAX_IMAGES} per brand (ads.json is unlimited)")
    print(f"  Since   : {START_DATE}")
    print(f"  Countries: {', '.join(COUNTRIES)}  (active + historical)")
    print(f"{'='*60}\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(run_brand, b): b["name"] for b in BRANDS}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                log(name, f"Unhandled exception: {exc}")

    print("\n[DONE] All brands complete.")


if __name__ == "__main__":
    main()
