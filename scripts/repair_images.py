"""
repair_images.py — Audit and clean up image files for all brands.

Pass 1 — Orphan deletion
    Delete any image file whose base Ad ID has no entry in ads.json.
    e.g. 2099490750883571_2.jpg → ID 2099490750883571 not in JSON → delete.

Pass 2 — Missing image report
    List ads in ads.json that have no image file at all (nothing to delete,
    just informational so you know what to re-download).
"""

import json
import re
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR      = Path(__file__).resolve().parent.parent
CAMPAIGNS_DIR = BASE_DIR / "campaigns"

IMAGE_RE = re.compile(r"^(\d+)(?:_\d+)?\.(jpg|jpeg|png|webp)$", re.IGNORECASE)


def repair_brand(brand_dir: Path):
    ads_file  = brand_dir / "ads.json"
    image_dir = brand_dir / "images"

    if not ads_file.exists() or not image_dir.exists():
        return

    with open(ads_file, encoding="utf-8") as f:
        ads = json.load(f)

    json_ids: set[str] = {str(ad["id"]) for ad in ads}
    all_images = sorted(image_dir.glob("*"))
    image_files = [p for p in all_images if IMAGE_RE.match(p.name)]

    print(f"\n{'='*60}")
    print(f"Brand: {brand_dir.name}")
    print(f"  Ads in JSON     : {len(json_ids)}")
    print(f"  Image files     : {len(image_files)}")

    deleted = 0

    # ── Pass 1: delete orphaned image files ───────────────────────────────
    for img in image_files:
        m = IMAGE_RE.match(img.name)
        ad_id = m.group(1)
        if ad_id not in json_ids:
            print(f"  [ORPHAN] {img.name}  — ID {ad_id} not in ads.json, deleting")
            img.unlink()
            deleted += 1

    # ── Pass 2: report ads missing any image ──────────────────────────────
    # Build set of IDs that have at least one image on disk after cleanup
    covered_ids: set[str] = set()
    for img in image_dir.glob("*"):
        m = IMAGE_RE.match(img.name)
        if m:
            covered_ids.add(m.group(1))

    missing = [ad for ad in ads if str(ad["id"]) not in covered_ids]

    if missing:
        print(f"  [MISSING] {len(missing)} ads have no image on disk:")
        for ad in missing[:10]:
            print(f"    - {ad['id']}  ({(ad.get('body') or '')[:60]})")
        if len(missing) > 10:
            print(f"    … and {len(missing) - 10} more")
    else:
        print(f"  All ads have at least one image on disk.")

    total_remaining = len(list(image_dir.glob("*")))
    print(f"  ── deleted: {deleted}  missing: {len(missing)}  remaining: {total_remaining}")


if __name__ == "__main__":
    for brand_folder in sorted(CAMPAIGNS_DIR.iterdir()):
        if brand_folder.is_dir():
            repair_brand(brand_folder)
    print("\n✓ Done — image audit complete.")
