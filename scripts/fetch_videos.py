#!/usr/bin/env python3
"""
fetch_videos.py
Concurrent Facebook Ads Library video scraper — one browser per brand.
Uses Playwright network interception to capture video files as they stream
through the browser (bypasses CDN session-auth issues).

Saves to: campaigns/{slug}/videos/{ad_id}.mp4
                           video_ads.json
                           video_snapshot_urls.txt

Edit CONFIG and BRANDS, then run:
    python fetch_videos.py
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

# Force UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
# ─── PATHS & CONFIG ───────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
DATA_DIR      = BASE_DIR / "data"
CAMPAIGNS_DIR = BASE_DIR / "campaigns"

WORKERS      = 3                # Concurrent browsers
MAX_VIDEOS   = 300              # Max video files saved per brand
MAX_SCROLLS  = 80               # Safety cap on scroll iterations
SCROLL_PAUSE = 5.0              # Slightly longer pause
HEADLESS     = False
COUNTRIES    = ["US", "CA", "GB"]
START_DATE   = "2023-01-01"
MIN_DATE     = datetime(2023, 1, 1)
MIN_SIZE_KB  = 50               # Skip intercepted files smaller than this (likely thumbnails)
# ──────────────────────────────────────────────────────────────────────────────

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
    params = [
        ("active_status", "all"),
        ("ad_type",       "all"),
        ("country",       country),
        ("is_targeted_country", "false"),
        ("media_type",    "video"),           # <-- videos only
        ("start_date[min]", START_DATE),
        ("sort_data[direction]", "desc"),
        ("sort_data[mode]",      "total_impressions"),
    ]
    if brand.get("page_id"):
        params += [("search_type", "page"), ("view_all_page_id", brand["page_id"])]
    else:
        params += [("search_type", "keyword_unordered"), ("q", brand["name"])]

    qs = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params)
    return "https://www.facebook.com/ads/library/?" + qs


# ─── CARD METADATA EXTRACTION JS ──────────────────────────────────────────────

SCRAPE_JS = """
() => {
    const results = [];
    const idSpans = [...document.querySelectorAll('span, div')]
        .filter(el => el.childElementCount === 0
                   && el.textContent.trim().startsWith('Library ID:'));

    idSpans.forEach((span, idx) => {
        let card = span;
        for (let i = 0; i < 15; i++) {
            card = card.parentElement;
            if (!card) break;
            if (card.offsetHeight > 200) break;
        }
        if (!card) return;

        const ad = { index: idx + 1, id: '', body: '', snapshot_url: '', started: '', active: null };

        const m = span.textContent.match(/Library ID:\\s*(\\d+)/);
        ad.id = m ? m[1] : ('card_' + String(idx + 1).padStart(4, '0'));

        const bodyEls = card.querySelectorAll('[style*="white-space: pre-wrap"]');
        ad.body = [...bodyEls].map(el => el.innerText.trim()).filter(Boolean).join(' | ');
        ad.text = card.innerText.slice(0, 600).trim();

        // Grab video src URLs directly from <video> elements
        ad.video_srcs = [...card.querySelectorAll('video')]
            .flatMap(v => [
                v.src,
                ...[...v.querySelectorAll('source')].map(s => s.src)
            ])
            .filter(src => src && src.startsWith('http'));

        const links = [...card.querySelectorAll('a[href*="/ads/"]')];
        ad.snapshot_url = links.length ? links[0].href : '';

        // Active status — appears as a standalone line before Library ID
        const activeM = card.innerText.match(/\\n(Active|Inactive)\\n/);
        ad.active = activeM ? activeM[1] === 'Active' : null;

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


# ─── DOWNLOAD ONE VIDEO ───────────────────────────────────────────────────────

def download_video(url: str, dest: Path, name: str) -> bool:
    """Fetch a single video URL and write to dest. Returns True on success."""
    try:
        r = requests.get(
            url, timeout=60, stream=True,
            headers={"User-Agent": UA, "Referer": "https://www.facebook.com/"},
        )
        if r.status_code not in (200, 206):
            return False
        body = b"".join(r.iter_content(65536))
        if len(body) < MIN_SIZE_KB * 1024:
            return False
        dest.write_bytes(body)
        return True
    except Exception as exc:
        warnings.warn(f"[{name}] Download failed ({dest.name}): {exc}")
        return False


# ─── SCRAPE ONE BRAND ─────────────────────────────────────────────────────────

def scrape_brand(brand: dict, country: str) -> list[dict]:
    """Scrape card metadata only. Video download happens after index assignment."""
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

        log(name, f"Opening Ads Library ({country})...")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except PWTimeout:
            log(name, "Load timed out — continuing anyway")

        time.sleep(4)
        dismiss_popups(page)
        time.sleep(1)

        prev_count = 0
        stable     = 0

        for i in range(MAX_SCROLLS):
            page.evaluate(
                "var el = document.scrollingElement || document.body;"
                "if (el) window.scrollTo(0, el.scrollHeight)"
            )
            time.sleep(SCROLL_PAUSE)
            dismiss_popups(page)

            count = page.locator('div:has-text("Library ID:")').count()
            log(name, f"Scroll {i+1}: ~{count} cards ({country})")

            if count == prev_count:
                stable += 1
                if stable >= 3:
                    break
            else:
                stable = 0
            prev_count = count

        try:
            page.wait_for_load_state("networkidle", timeout=10_000)
        except Exception:
            pass
        time.sleep(1.5)

        current_url = page.url
        if "ads/library" not in current_url:
            log(name, "Redirected — navigating back")
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20_000)
                time.sleep(3)
                dismiss_popups(page)
                page.wait_for_load_state("networkidle", timeout=8_000)
            except Exception:
                pass

        raw: list = []
        for attempt in range(3):
            try:
                raw = page.evaluate(SCRAPE_JS) or []
                break
            except Exception as exc:
                log(name, f"JS attempt {attempt+1} failed: {exc}")
                time.sleep(3)

        browser.close()

    # Date filter
    out = []
    for ad in raw:
        dt = parse_started(ad)
        if dt and dt < MIN_DATE:
            continue
        out.append(ad)

    log(name, f"Collected {len(out)} video ad cards from {country}.")
    return out


# ─── SAVE METADATA ────────────────────────────────────────────────────────────

def save_brand(brand: dict, new_ads: list[dict], country: str):
    out_dir = CAMPAIGNS_DIR / brand["slug"]
    ads_file = out_dir / "video_ads.json"
    out_dir.mkdir(parents=True, exist_ok=True)

    existing_ads = []
    if ads_file.exists():
        try:
            with open(ads_file, "r", encoding="utf-8") as f:
                existing_ads = json.load(f)
        except Exception: pass

    ad_map = {str(ad.get("id")): ad for ad in existing_ads}
    
    # Calculate current max index to assign new ones
    max_idx = 0
    if existing_ads:
        max_idx = max([int(ad.get("index", 0)) for ad in existing_ads] or [0])

    for ad in new_ads:
        ad_id = str(ad.get("id"))
        if ad_id in ad_map:
            if "countries" not in ad_map[ad_id]:
                ad_map[ad_id]["countries"] = ["US"]
            if country not in ad_map[ad_id]["countries"]:
                ad_map[ad_id]["countries"].append(country)
            # Always refresh CDN URLs — they expire and are useless when stale
            if ad.get("video_srcs"):
                ad_map[ad_id]["video_srcs"] = ad["video_srcs"]
            if ad.get("active") is not None:
                ad_map[ad_id]["active"] = ad["active"]
            if ad.get("started"):
                ad_map[ad_id]["started"] = ad["started"]
            if ad.get("text"):
                ad_map[ad_id]["text"] = ad["text"]
        else:
            # New ad: assign next index and tag with current country
            max_idx += 1
            ad["index"] = max_idx
            ad["countries"] = [country]
            ad_map[ad_id] = ad

    with open(ads_file, "w", encoding="utf-8") as f:
        json.dump(list(ad_map.values()), f, indent=2, ensure_ascii=False)

    with open(out_dir / "video_snapshot_urls.txt", "w", encoding="utf-8") as f:
        for ad_id, ad in ad_map.items():
            f.write(f"{ad_id} | {ad.get('snapshot_url','')}\n")


# ─── PER-BRAND ENTRY POINT ────────────────────────────────────────────────────

def run_brand(brand: dict):
    name      = brand["name"]
    video_dir = CAMPAIGNS_DIR / brand["slug"] / "videos"
    video_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Pass 1: scrape metadata for all countries, merge into JSON
        for country in COUNTRIES:
            ads = scrape_brand(brand, country)
            save_brand(brand, ads, country)

        # Pass 2: download videos using the now-stable index from JSON
        # Each ad's video file is named video_{index:04d}.mp4 — one file per ad.
        ads_file = CAMPAIGNS_DIR / brand["slug"] / "video_ads.json"
        with open(ads_file, encoding="utf-8") as f:
            all_ads = json.load(f)

        downloaded = skipped = failed = 0
        for ad in all_ads:
            idx  = int(ad.get("index", 0))
            dest = video_dir / f"video_{idx:04d}.mp4"

            if dest.exists():
                skipped += 1
                continue

            srcs = ad.get("video_srcs") or []
            if not srcs:
                continue

            # Try each src URL in order until one succeeds
            for src_url in srcs:
                if download_video(src_url, dest, name):
                    log(name, f"  Downloaded video_{idx:04d}.mp4 (ad {ad['id']})")
                    downloaded += 1
                    break
            else:
                log(name, f"  [FAIL] All URLs failed for ad {ad['id']} (index {idx})")
                failed += 1

            if downloaded >= MAX_VIDEOS:
                log(name, f"Reached {MAX_VIDEOS} video cap — stopping.")
                break

        log(name, f"[OK] downloaded={downloaded} skipped={skipped} failed={failed}")
    except Exception as exc:
        log(name, f"[FAIL] Error: {exc}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*60}")
    print(f"  Facebook Ads Library — Video Scraper (Multi-Country)")
    print(f"  Brands    : {len(BRANDS)}")
    print(f"  Countries : {', '.join(COUNTRIES)}")
    print(f"  Workers   : {WORKERS}")
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
