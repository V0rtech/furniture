"""
repair_videos.py — Rename hash-named video files and deduplicate by Ad ID.

Matching strategy for 12-char hex filenames (e.g. 21dc41945ac6.mp4):
  1. md5(ad_id)[:12]
  2. md5(video_url)[:12]           — full URL including query params
  3. md5(video_url_no_params)[:12] — URL stripped of query string
"""

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlparse, urlunparse

BASE_DIR = Path(__file__).resolve().parent.parent
CAMPAIGNS_DIR = BASE_DIR / "campaigns"

HEX12 = re.compile(r"^[0-9a-f]{12}$", re.IGNORECASE)


def md5_prefix(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()[:12]


def strip_query(url: str) -> str:
    p = urlparse(url)
    return urlunparse(p._replace(query="", fragment=""))


def build_hash_index(ads: list) -> dict[str, str]:
    """Return {12-char-hex -> ad_id} for all candidates derived from each ad."""
    index: dict[str, str] = {}

    def register(key: str, ad_id: str):
        if key in index and index[key] != ad_id:
            # Collision — mark ambiguous so we don't misidentify
            index[key] = None
        else:
            index[key] = ad_id

    for ad in ads:
        ad_id = str(ad["id"])
        register(md5_prefix(ad_id), ad_id)
        for url in ad.get("video_srcs", []):
            register(md5_prefix(url), ad_id)
            register(md5_prefix(strip_query(url)), ad_id)

    return index


def repair_brand(brand_dir: Path):
    ads_file = brand_dir / "video_ads.json"
    video_dir = brand_dir / "videos"
    if not ads_file.exists() or not video_dir.exists():
        return

    with open(ads_file, encoding="utf-8") as f:
        ads = json.load(f)

    # Ground-truth maps
    id_to_index: dict[str, int] = {str(ad["id"]): ad["index"] for ad in ads}
    hash_index = build_hash_index(ads)

    print(f"\n{'='*60}")
    print(f"Brand: {brand_dir.name}")
    print(f"  Ads in JSON  : {len(ads)}")
    print(f"  Videos on disk: {len(list(video_dir.glob('*.mp4')))}")

    renamed = deleted = skipped = 0

    # ── Pass 1: rename hash files ──────────────────────────────────────────
    for file in sorted(video_dir.glob("*.mp4")):
        stem = file.stem
        if not HEX12.match(stem):
            continue  # not a hash file

        ad_id = hash_index.get(stem.lower())

        if ad_id is None:
            print(f"  [SKIP]   {file.name}  — no match found (ambiguous or unknown hash)")
            skipped += 1
            continue

        idx = id_to_index.get(ad_id)
        if idx is None:
            print(f"  [SKIP]   {file.name}  — hash resolved to {ad_id} but that ID isn't in JSON")
            skipped += 1
            continue

        canonical = video_dir / f"video_{idx:04d}.mp4"

        if canonical.exists():
            print(f"  [DELETE] {file.name}  — duplicate of {canonical.name} (ad {ad_id})")
            file.unlink()
            deleted += 1
        else:
            print(f"  [RENAME] {file.name}  →  {canonical.name}  (ad {ad_id})")
            file.rename(canonical)
            renamed += 1

    # ── Pass 2: deduplicate video_XXXX.mp4 files ──────────────────────────
    canonical_by_id: dict[str, Path] = {}
    index_to_id: dict[int, str] = {ad["index"]: str(ad["id"]) for ad in ads}

    for file in sorted(video_dir.glob("video_*.mp4")):
        m = re.match(r"video_(\d+)\.mp4$", file.name)
        if not m:
            continue
        idx = int(m.group(1))
        ad_id = index_to_id.get(idx)
        if ad_id is None:
            continue  # handled in pass 3

        if ad_id in canonical_by_id:
            existing = canonical_by_id[ad_id]
            correct_idx = id_to_index[ad_id]
            loser = file if idx != correct_idx else existing
            winner = existing if idx != correct_idx else file
            print(f"  [DELETE] {loser.name}  — duplicate for ad {ad_id}, keeping {winner.name}")
            loser.unlink()
            deleted += 1
            canonical_by_id[ad_id] = winner
        else:
            canonical_by_id[ad_id] = file

    # ── Pass 3: delete orphan video_XXXX.mp4 files (no JSON entry) ────────
    valid_indices: set[int] = set(index_to_id.keys())
    orphaned = 0

    for file in sorted(video_dir.glob("video_*.mp4")):
        m = re.match(r"video_(\d+)\.mp4$", file.name)
        if not m:
            continue
        idx = int(m.group(1))
        if idx not in valid_indices:
            print(f"  [ORPHAN] {file.name}  — index {idx} has no JSON entry, deleting")
            file.unlink()
            orphaned += 1

    total_remaining = len(list(video_dir.glob("video_*.mp4")))
    print(f"  ── renamed: {renamed}  deleted: {deleted}  orphans removed: {orphaned}  skipped: {skipped}  remaining: {total_remaining}")


if __name__ == "__main__":
    for brand_folder in sorted(CAMPAIGNS_DIR.iterdir()):
        if brand_folder.is_dir():
            repair_brand(brand_folder)
    print("\n✓ Done — all brands repaired and deduplicated.")
