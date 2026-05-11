"""
V-Nexus: Master Scraper Runner
1 lệnh: cào nhatot + muaban → lọc → merge → 1 file data chuẩn

Usage:
    python run.py                          # Full cycle: cào cả 2 + lọc + merge
    python run.py --nhatot-only            # Chỉ cào nhatot
    python run.py --muaban-only            # Chỉ cào muaban
    python run.py --loop --interval 60     # Chạy liên tục mỗi 60 phút

Output:
    data/final/{YYYY-MM-DD}/{HHMMSS}_merged.json
"""

import asyncio
import json
import os
import sys
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

_base = Path(__file__).resolve().parent
sys.path.insert(0, str(_base))
sys.path.insert(0, str(_base / "scrapers"))
sys.path.insert(0, str(_base / "pipeline"))

from config import clean_path, final_path, find_latest_raw, find_latest_final, reset_session
from unified_pipeline import process_batch, AddressMapper
from merge_pipeline import run_merge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("runner")


async def scrape_nhatot(
    count: int = 200,
    tabs: int = 5,
    batch_size: int = 50,
    region: int = 13000,
) -> str:
    """Run nhatot scraper. Returns output file path."""
    from nhatot_fast_scraper import run_cycle
    result = await run_cycle(
        max_listings=count,
        num_tabs=tabs,
        batch_size=batch_size,
        region=region,
    )
    return result.get("file") if isinstance(result, dict) else None


async def scrape_muaban(per_city: int = 500, category: str = "all") -> str:
    """Run muaban scraper. Returns output file path."""
    from muaban_scraper import run_cycle as muaban_cycle
    result = await muaban_cycle(per_city=per_city, category=category)
    return result.get("file") if isinstance(result, dict) else None


def run_pipeline_for_source(source: str, input_file: str, mapper: AddressMapper) -> str:
    """Run unified pipeline on raw data. Returns clean file path."""
    with open(input_file, encoding="utf-8") as f:
        data = json.load(f)

    # nhatot_fast_scraper already outputs clean DTOs in "listings"
    # muaban_scraper outputs raw items in "items"
    if source == "nhatot":
        items = data.get("listings", data.get("ads", []))
        # Check if already processed (has 'source' field = DTO)
        if items and isinstance(items[0], dict) and items[0].get("source") == "nhatot":
            log.info(f"  [{source}] Already clean DTO ({len(items)} listings), skipping pipeline")
            out = clean_path(source)
            with open(out, "w", encoding="utf-8") as f:
                json.dump({"source": source, "total": len(items), "listings": items}, f, ensure_ascii=False, indent=2)
            return out
        items = data.get("ads", items)
    else:
        items = data.get("items", [])

    log.info(f"  [{source}] Processing {len(items)} raw items through pipeline...")
    results = process_batch(items, source, mapper)

    from dataclasses import asdict
    out = clean_path(source)
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "source": source,
            "total": len(results),
            "listings": [asdict(r) for r in results],
        }, f, ensure_ascii=False, indent=2)

    log.info(f"  [{source}] Clean: {len(results)} listings → {out}")
    return out


async def full_cycle(
    nhatot_only: bool = False,
    muaban_only: bool = False,
    nhatot_count: int = 200,
    nhatot_tabs: int = 5,
    nhatot_batch_size: int = 50,
    muaban_per_city: int = 500,
    muaban_category: str = "all",
):
    """Full cycle: scrape → pipeline → merge → final output."""
    reset_session()
    start = time.time()

    log.info(f"\n{'#'*60}")
    log.info(f"  V-NEXUS FULL CYCLE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'#'*60}")

    for d in ["data/raw/nhatot", "data/raw/muaban", "data/clean", "data/final"]:
        os.makedirs(d, exist_ok=True)

    # === STEP 1: SCRAPE ===
    log.info(f"\n{'='*50}")
    log.info(f"  STEP 1: SCRAPE")
    log.info(f"{'='*50}")

    nhatot_raw = None
    muaban_raw = None

    if not muaban_only:
        log.info("\n  [nhatot] Starting...")
        try:
            nhatot_raw = await scrape_nhatot(
                count=nhatot_count,
                tabs=nhatot_tabs,
                batch_size=nhatot_batch_size,
            )
            log.info(f"  [nhatot] Done → {nhatot_raw}")
        except Exception as e:
            log.error(f"  [nhatot] Failed: {e}")

    if not nhatot_only:
        log.info(f"\n  [muaban] Starting (category={muaban_category})...")
        try:
            muaban_raw = await scrape_muaban(per_city=muaban_per_city, category=muaban_category)
            log.info(f"  [muaban] Done → {muaban_raw}")
        except Exception as e:
            log.error(f"  [muaban] Failed: {e}")

    # Fallback to latest raw if a scrape failed
    if not nhatot_raw and not muaban_only:
        nhatot_raw = find_latest_raw("nhatot")
    if not muaban_raw and not nhatot_only:
        muaban_raw = find_latest_raw("muaban")
    if not nhatot_raw and not muaban_raw:
        log.error("No data files found. Run scrape first.")
        return None

    # === STEP 2: PIPELINE (normalize + classify) ===
    log.info(f"\n{'='*50}")
    log.info(f"  STEP 2: PIPELINE (normalize + classify)")
    log.info(f"{'='*50}")

    ref_dir = str(_base / "pipeline" / "reference")
    try:
        mapper = AddressMapper(ref_dir)
    except Exception as e:
        log.warning(f"AddressMapper failed: {e}")
        mapper = None

    nhatot_clean = None
    muaban_clean = None
    if nhatot_raw:
        nhatot_clean = run_pipeline_for_source("nhatot", nhatot_raw, mapper)
    if muaban_raw:
        muaban_clean = run_pipeline_for_source("muaban", muaban_raw, mapper)

    # === STEP 3: MERGE → 1 FINAL FILE ===
    log.info(f"\n{'='*50}")
    log.info(f"  STEP 3: MERGE → FINAL OUTPUT")
    log.info(f"{'='*50}")

    if nhatot_clean and muaban_clean:
        run_merge(nhatot_clean, muaban_clean)
    elif nhatot_clean or muaban_clean:
        src_clean = nhatot_clean or muaban_clean
        src_name = "nhatot" if nhatot_clean else "muaban"
        fp = final_path()
        with open(src_clean, encoding="utf-8") as f:
            data = json.load(f)
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"  Single source ({src_name}) → {fp}")
    else:
        log.error("No clean data to merge")
        return None

    # === SUMMARY ===
    elapsed = int(time.time() - start)
    final_file = find_latest_final()

    if final_file:
        with open(final_file, encoding="utf-8") as f:
            final_data = json.load(f)
        total = final_data.get("total", len(final_data.get("listings", [])))
        phones = final_data.get("phone_full", final_data.get("full_phone", 0))
        quality = final_data.get("avg_quality", 0)

        log.info(f"\n{'#'*60}")
        log.info(f"  CYCLE COMPLETE in {elapsed}s")
        log.info(f"  Total listings: {total}")
        log.info(f"  Full phones:    {phones}")
        log.info(f"  Avg quality:    {quality}")
        log.info(f"  Output:         {final_file}")
        log.info(f"{'#'*60}")

    return final_file


async def loop(interval_min: int = 60, **kwargs):
    """Run continuously."""
    cycle = 0
    while True:
        cycle += 1
        log.info(f"\n{'*'*60}")
        log.info(f"  LOOP CYCLE #{cycle}")
        log.info(f"{'*'*60}")
        try:
            await full_cycle(**kwargs)
        except Exception as e:
            log.error(f"Cycle failed: {e}")
        log.info(f"Next cycle in {interval_min} min...")
        await asyncio.sleep(interval_min * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="V-Nexus Master Scraper Runner")
    parser.add_argument("--nhatot-only", action="store_true", help="Only scrape nhatot")
    parser.add_argument("--muaban-only", action="store_true", help="Only scrape muaban")
    parser.add_argument("--nhatot-count", type=int, default=500, help="Nhatot listings count")
    parser.add_argument("--nhatot-tabs", type=int, default=5, help="Nhatot browser tabs")
    parser.add_argument("--nhatot-batch-size", type=int, default=50, help="Nhatot phone reveal batch size")
    parser.add_argument("--muaban-per-city", type=int, default=500, help="Muaban listings per city")
    parser.add_argument("--muaban-category", default="all", help="Muaban subcategory (e.g. dat-tho-cu). Default: all")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval (minutes)")
    args = parser.parse_args()

    if args.loop:
        asyncio.run(loop(
            interval_min=args.interval,
            nhatot_only=args.nhatot_only,
            muaban_only=args.muaban_only,
            nhatot_count=args.nhatot_count,
            nhatot_tabs=args.nhatot_tabs,
            nhatot_batch_size=args.nhatot_batch_size,
            muaban_per_city=args.muaban_per_city,
            muaban_category=args.muaban_category,
        ))
    else:
        asyncio.run(full_cycle(
            nhatot_only=args.nhatot_only,
            muaban_only=args.muaban_only,
            nhatot_count=args.nhatot_count,
            nhatot_tabs=args.nhatot_tabs,
            nhatot_batch_size=args.nhatot_batch_size,
            muaban_per_city=args.muaban_per_city,
            muaban_category=args.muaban_category,
        ))
