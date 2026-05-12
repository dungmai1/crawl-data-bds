"""
V-Nexus: 3-Source Merge Pipeline
Merge data từ nhatot + muaban → 1 dataset sạch nhất

Strategy:
  1. Load clean data từ cả 3 sources (đã qua unified_pipeline)
  2. Cross-reference: match cùng 1 BĐS trên nhiều platform
     Match criteria: same ward + price ±15% + area ±10%
  3. Merge: lấy field mạnh nhất từ mỗi source
     - nhatot: lat/lng, street, direction, legal_document
     - muaban: phone_full
     - merge: cross-reference for phone + GPS + legal enrichment
  4. Dedup: giữ record merged, loại bỏ duplicate
  5. Output: 1 JSON file — data sạch, đầy đủ nhất, sẵn sàng INSERT DB

Usage:
    python merge_pipeline.py
    python merge_pipeline.py --nhatot data/clean/nhatot.json --muaban data/clean/muaban.json
"""

import json
import os
import re
import sys
import logging
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from dataclasses import asdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("merge")


def normalize_ward_for_match(ward: str) -> str:
    """Normalize ward name for fuzzy matching."""
    if not ward: return ""
    w = ward.lower().strip()
    # Remove prefix
    for prefix in ["phường ", "xã ", "thị trấn "]:
        w = w.replace(prefix, "")
    # Remove diacritics for matching
    return w


def normalize_price_range(price: int, tolerance: float = 0.15):
    """Return (min, max) range for price matching."""
    if not price or price <= 0:
        return (0, 0)
    return (int(price * (1 - tolerance)), int(price * (1 + tolerance)))


def find_matches(source_a: list, source_b: list, label_a: str, label_b: str) -> list:
    """Find matching listings between two sources.
    Returns list of (item_a, item_b, match_score) tuples."""

    # Build index on source_b by ward
    ward_index = defaultdict(list)
    for item in source_b:
        ward_key = normalize_ward_for_match(item.get("ward", ""))
        province = item.get("province", "")
        if ward_key and province:
            key = f"{province}|{ward_key}"
            ward_index[key].append(item)

    matches = []

    for item_a in source_a:
        ward_key_a = normalize_ward_for_match(item_a.get("ward", ""))
        province_a = item_a.get("province", "")
        if not ward_key_a or not province_a:
            continue

        key_a = f"{province_a}|{ward_key_a}"
        candidates = ward_index.get(key_a, [])

        price_a = item_a.get("price", 0) or 0
        area_a = item_a.get("area", 0) or 0

        best_match = None
        best_score = 0

        for item_b in candidates:
            score = 0
            price_b = item_b.get("price", 0) or 0
            area_b = item_b.get("area", 0) or 0

            # Price match (±15%)
            if price_a > 0 and price_b > 0:
                price_diff = abs(price_a - price_b) / max(price_a, price_b)
                if price_diff <= 0.15:
                    score += 30 * (1 - price_diff / 0.15)  # 0-30 points
                else:
                    continue  # Skip if price too different

            # Area match (±10%)
            if area_a > 0 and area_b > 0:
                area_diff = abs(area_a - area_b) / max(area_a, area_b)
                if area_diff <= 0.10:
                    score += 25 * (1 - area_diff / 0.10)  # 0-25 points
                else:
                    continue

            # Phone match (6 digits prefix)
            phone_a = (item_a.get("phone_full") or "")[:7].replace(" ", "")
            phone_b = (item_b.get("phone_full") or "")[:7].replace(" ", "")
            if phone_a and phone_b and phone_a == phone_b:
                score += 30  # Strong signal

            # Property type match
            if item_a.get("property_type") and item_a["property_type"] == item_b.get("property_type"):
                score += 10

            # Transaction type match
            if item_a.get("transaction_type") == item_b.get("transaction_type"):
                score += 5

            if score > best_score and score >= 40:
                best_score = score
                best_match = item_b

        if best_match:
            matches.append((item_a, best_match, best_score))

    log.info(f"  {label_a} x {label_b}: {len(matches)} matches found")
    return matches


def merge_two_items(primary: dict, secondary: dict) -> dict:
    """Merge two items, taking the best field from each.
    Primary = more trusted source, secondary fills gaps."""

    merged = dict(primary)  # Start with primary

    # Fill missing fields from secondary
    fill_fields = [
        "phone_full", "lat", "lng", "street",
        "direction", "legal_document", "bedrooms", "bathrooms",
        "floors", "description", "contact_name",
    ]

    for field in fill_fields:
        if not merged.get(field) and secondary.get(field):
            merged[field] = secondary[field]

    # Prefer more images
    if len(secondary.get("images", [])) > len(merged.get("images", [])):
        merged["images"] = secondary["images"]
        merged["image_count"] = secondary.get("image_count", len(secondary["images"]))

    # Track merge source
    merged["_merged_from"] = secondary.get("source", "unknown")
    merged["_merged_source_id"] = secondary.get("source_id", "")

    return merged


def run_merge(nhatot_file: str, muaban_file: str):
    """Run the full merge pipeline."""

    log.info(f"{'='*60}")
    log.info(f"  V-NEXUS MERGE PIPELINE")
    log.info(f"{'='*60}")

    # Load data
    sources = {}

    log.info("\nLoading sources...")
    with open(nhatot_file, encoding="utf-8") as f:
        sources["nhatot"] = json.load(f)["listings"]
    log.info(f"  nhatot: {len(sources['nhatot'])} listings")

    with open(muaban_file, encoding="utf-8") as f:
        sources["muaban"] = json.load(f)["listings"]
    log.info(f"  muaban: {len(sources['muaban'])} listings")

    # === STEP 1: Cross-reference nhatot ↔ muaban ===
    log.info("\n--- STEP 1: Cross-reference nhatot ↔ muaban ---")

    nt_mb_matches = find_matches(sources["nhatot"], sources["muaban"], "nhatot", "muaban")

    matched_nt_ids = set()
    matched_mb_ids = set()
    merged_items = []

    for nt_item, mb_item, score in nt_mb_matches:
        merged = merge_two_items(nt_item, mb_item)
        merged["_match_score"] = score
        merged_items.append(merged)
        matched_nt_ids.add(nt_item.get("source_id"))
        matched_mb_ids.add(mb_item.get("source_id"))

    log.info(f"  Merged (nhatot+muaban): {len(merged_items)} records")
    phones_gained = sum(1 for m in merged_items if m.get("phone_full"))
    log.info(f"  Phones gained from muaban: {phones_gained}")

    # === STEP 2: Add unmatched items ===
    log.info("\n--- STEP 2: Add unmatched items ---")

    unmatched_nt = [i for i in sources["nhatot"] if i.get("source_id") not in matched_nt_ids]
    log.info(f"  Unmatched nhatot: {len(unmatched_nt)}")

    unmatched_mb = [i for i in sources["muaban"] if i.get("source_id") not in matched_mb_ids]
    log.info(f"  Unmatched muaban: {len(unmatched_mb)}")

    all_items = merged_items + unmatched_nt + unmatched_mb
    log.info(f"  Total before dedup: {len(all_items)}")

    # === STEP 3: Final stats ===
    total = len(all_items)
    has_phone = sum(1 for i in all_items if i.get("phone_full"))
    has_gps = sum(1 for i in all_items if i.get("lat") and i.get("lng"))
    has_street = sum(1 for i in all_items if i.get("street"))
    has_legal = sum(1 for i in all_items if i.get("legal_document"))
    has_direction = sum(1 for i in all_items if i.get("direction"))
    merged_count = sum(1 for i in all_items if i.get("_merged_from"))

    # Source distribution
    from collections import Counter
    source_dist = Counter(i.get("source") for i in all_items)

    log.info(f"\n{'='*60}")
    log.info(f"  FINAL MERGED DATASET")
    log.info(f"{'='*60}")
    log.info(f"  Total:          {total}")
    log.info(f"  Merged records: {merged_count} (same BĐS on 2+ platforms)")
    log.info(f"")
    log.info(f"  FIELD COVERAGE:")
    log.info(f"    Phone (full):   {has_phone:>6} ({has_phone/total*100:>5.1f}%)")
    log.info(f"    GPS (lat/lng):  {has_gps:>6} ({has_gps/total*100:>5.1f}%)")
    log.info(f"    Street:         {has_street:>6} ({has_street/total*100:>5.1f}%)")
    log.info(f"    Legal doc:      {has_legal:>6} ({has_legal/total*100:>5.1f}%)")
    log.info(f"    Direction:      {has_direction:>6} ({has_direction/total*100:>5.1f}%)")
    log.info(f"")
    log.info(f"  BY SOURCE:")
    for src, count in source_dist.most_common():
        print(f"    {src}: {count}")

    # Save
    import sys as _sys
    _cfg = str(Path(__file__).resolve().parent.parent)
    if _cfg not in _sys.path: _sys.path.insert(0, _cfg)
    from config import final_path
    out = final_path("merged")

    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "total": total,
            "merged_cross_platform": merged_count,
            "phone_full": has_phone,
            "gps_coverage": has_gps,
            "sources": dict(source_dist),
            "processed_at": datetime.now().isoformat(),
            "listings": all_items,
        }, f, ensure_ascii=False, indent=2)

    log.info(f"\n  SAVED: {out}")
    return all_items


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--nhatot", required=True)
    parser.add_argument("--muaban", required=True)
    args = parser.parse_args()
    run_merge(args.nhatot, args.muaban)
