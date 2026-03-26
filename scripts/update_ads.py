#!/usr/bin/env python3
"""
update_ads.py — Refresh time-sensitive fields for existing ads only.

Scrapes the Facebook Ads Library for each brand and updates any ad whose
Library ID already exists in the JSON.  It never adds new ads.

Fields refreshed per matched ad:
  active   — current active/inactive status
  text     — raw card text (contains date range for inactive ads)
  started  — "Started running on …" date (active ads)

Run:
    python scripts/update_ads.py                  # all brands, all countries
    python scripts/update_ads.py caraway hexclad  # specific brands only
"""

import json
import re
import sys
import time
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ── Force UTF-8 on Windows ────────────────────────────────────────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── Paths & config ────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent.parent
DATA_DIR      = BASE_DIR / "data"
CAMPAIGNS_DIR = BASE_DIR / "campaigns"

WORKERS      = 3
MAX_SCROLLS  = 80
SCROLL_PAUSE = 2.5
HEADLESS     = True
COUNTRIES    = ["US", "CA", "GB"]
START_DATE   = "2023-01-01"

BRANDS_FILE = DATA_DIR / "brands.json"
with open(BRANDS_FILE, encoding="utf-8") as _f:
    ALL_BRANDS = json.load(_f)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

_lock = threading.Lock()


def log(brand: str, msg: str):
    with _lock:
        print(f"[{brand:<20}] {msg}", flush=True)


# ── URL builder (same as fetch_ads.py) ───────────────────────────────────────

def build_url(brand: dict, country: str) -> str:
    params = [
        ("active_status",        "all"),
        ("ad_type",              "all"),
        ("country",              country),
        ("is_targeted_country",  "false"),
        ("media_type",           "all"),
        ("start_date[min]",      START_DATE),
        ("sort_data[direction]", "desc"),
        ("sort_data[mode]",      "total_impressions"),
    ]
    if brand.get("page_id"):
        params += [("search_type", "page"), ("view_all_page_id", brand["page_id"])]
    else:
        params += [("search_type", "keyword_unordered"), ("q", brand["name"])]

    qs = "&".join(f"{k}={quote(str(v), safe='')}" for k, v in params)
    return "https://www.facebook.com/ads/library/?" + qs


# ── Extraction JS (same scraper, identical to fetch_ads.py) ──────────────────

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

        const m = span.textContent.match(/Library ID:\\s*(\\d+)/);
        const id = m ? m[1] : null;
        if (!id) return;

        const activeM = card.innerText.match(/\\n(Active|Inactive)\\n/);
        const active  = activeM ? activeM[1] === 'Active' : null;

        const dateM   = card.innerText.match(/Started running on (.+)/);
        const started = dateM ? dateM[1].trim() : '';

        const text = card.innerText.slice(0, 600).trim();

        results.push({ id, active, started, text });
    });

    return results;
}
"""


# ── Popup dismissal ───────────────────────────────────────────────────────────

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


# ── Scrape one brand+country, return id→{active,started,text} ────────────────

def scrape_updates(brand: dict, country: str) -> dict[str, dict]:
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

        log(name, f"Scraping for updates ({country})…")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        except PWTimeout:
            log(name, "Load timed out — continuing anyway")

        time.sleep(4)
        dismiss_popups(page)

        prev_count = stable = 0
        for i in range(MAX_SCROLLS):
            page.evaluate(
                "var el = document.scrollingElement || document.body;"
                "if (el) window.scrollTo(0, el.scrollHeight)"
            )
            time.sleep(SCROLL_PAUSE)
            dismiss_popups(page)
            count = page.locator('div:has-text("Library ID:")').count()
            log(name, f"  Scroll {i+1}: ~{count} cards ({country})")
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
        time.sleep(3)

        raw = []
        for attempt in range(3):
            try:
                raw = page.evaluate(SCRAPE_JS) or []
                break
            except Exception as exc:
                log(name, f"  JS attempt {attempt+1} failed: {exc}")
                time.sleep(3)

        browser.close()

    return {r["id"]: r for r in raw if r.get("id")}


# ── Apply updates to one JSON file ────────────────────────────────────────────

def apply_updates(json_path: Path, scraped: dict[str, dict], brand_name: str) -> tuple[int, int]:
    """
    Merge scraped updates into json_path.
    Returns (matched, total) counts.
    """
    with open(json_path, encoding="utf-8") as f:
        ads = json.load(f)

    matched = 0
    for ad in ads:
        ad_id = str(ad.get("id", ""))
        fresh = scraped.get(ad_id)
        if not fresh:
            continue
        matched += 1

        if fresh.get("active") is not None:
            ad["active"] = fresh["active"]
        if fresh.get("started"):
            ad["started"] = fresh["started"]
        if fresh.get("text"):
            ad["text"] = fresh["text"]

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(ads, f, indent=2, ensure_ascii=False)

    return matched, len(ads)


# ── Per-brand orchestration ───────────────────────────────────────────────────

def update_brand(brand: dict):
    name = brand["name"]
    slug = brand["slug"]
    brand_dir = CAMPAIGNS_DIR / slug

    # Discover which JSON files exist for this brand
    json_files = [p for p in [
        brand_dir / "ads.json",
        brand_dir / "video_ads.json",
    ] if p.exists()]

    if not json_files:
        log(name, "No JSON files found — skipping")
        return

    # Collect all existing ad IDs across both files so we know when to stop
    existing_ids: set[str] = set()
    for jf in json_files:
        with open(jf, encoding="utf-8") as f:
            for ad in json.load(f):
                existing_ids.add(str(ad.get("id", "")))

    log(name, f"Loaded {len(existing_ids)} existing ad IDs across {len(json_files)} file(s)")

    # Scrape all countries and merge results
    all_scraped: dict[str, dict] = {}
    for country in COUNTRIES:
        try:
            updates = scrape_updates(brand, country)
            # Only keep ads that already exist in our JSON
            for ad_id, data in updates.items():
                if ad_id in existing_ids:
                    all_scraped[ad_id] = data
        except Exception as exc:
            log(name, f"Scrape error ({country}): {exc}")

    log(name, f"Scraped {len(all_scraped)} matching IDs from library")

    # Apply to each JSON file
    for jf in json_files:
        matched, total = apply_updates(jf, all_scraped, name)
        log(name, f"Updated {matched}/{total} ads in {jf.name}")

    log(name, "[OK] Complete")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Optional: filter to specific brand slugs passed as CLI args
    slugs = set(sys.argv[1:])
    brands = [b for b in ALL_BRANDS if not slugs or b["slug"] in slugs]

    if not brands:
        print(f"No matching brands found. Available: {[b['slug'] for b in ALL_BRANDS]}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Facebook Ads Library — Update Existing Ads")
    print(f"  Brands   : {len(brands)}")
    print(f"  Workers  : {WORKERS}")
    print(f"  Countries: {', '.join(COUNTRIES)}")
    print(f"  Mode     : update existing IDs only (no new ads added)")
    print(f"{'='*60}\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(update_brand, b): b["name"] for b in brands}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                fut.result()
            except Exception as exc:
                log(name, f"Unhandled error: {exc}")

    print("\n[DONE] All brands updated.")


if __name__ == "__main__":
    main()
