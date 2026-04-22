"""
V-Nexus: Master Scraper Runner
1 lệnh duy nhất: cào 3 sources → lọc → merge → 1 file data chuẩn

Usage:
    python run.py                          # Full cycle: cào + lọc + merge
    python run.py --skip-scrape            # Chỉ lọc + merge (dùng data cào sẵn)
    python run.py --nhatot-only            # Chỉ cào nhatot
    python run.py --muaban-only            # Chỉ cào muaban
    python run.py --loop --interval 60     # Chạy liên tục mỗi 60 phút

Output:
    data/final/vnexus_YYYYMMDD_HHMMSS.json  ← 1 file duy nhất, data sạch nhất
"""

import asyncio
import json
import os
import sys
import time
import logging
import argparse
import glob
from datetime import datetime
from pathlib import Path

# Add paths
_base = Path(__file__).resolve().parent
sys.path.insert(0, str(_base))
sys.path.insert(0, str(_base / "scrapers"))
sys.path.insert(0, str(_base / "pipeline"))

from config import raw_path, clean_path, final_path, find_latest_raw, find_latest_clean, find_latest_final, reset_session
from unified_pipeline import process_batch, AddressMapper
from merge_pipeline import run_merge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("runner")


async def scrape_nhatot(count: int = 500, tabs: int = 5, region: int = 13000) -> str:
    """Run nhatot scraper. Returns output file path."""
    from nhatot_fast_scraper import run_cycle
    result = await run_cycle(max_listings=count, num_tabs=tabs, region=region)
    return result.get("file") if isinstance(result, dict) else None


async def scrape_muaban(per_city: int = 500) -> str:
    """Run muaban scraper. Returns output file path."""
    from muaban_scraper import run_cycle as muaban_cycle
    result = await muaban_cycle(per_city=per_city)
    return result.get("file") if isinstance(result, dict) else None




def run_pipeline_for_source(source: str, input_file: str, mapper: AddressMapper) -> str:
    """Run unified pipeline on raw data. Returns clean file path."""
    with open(input_file) as f:
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

    out = clean_path(source)

    from dataclasses import asdict
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "source": source,
            "total": len(results),
            "listings": [asdict(r) for r in results],
        }, f, ensure_ascii=False, indent=2)

    log.info(f"  [{source}] Clean: {len(results)} listings → {out}")
    return out


async def full_cycle(
    skip_scrape: bool = False,
    nhatot_only: bool = False,
    muaban_only: bool = False,
    nhatot_count: int = 500,
    nhatot_tabs: int = 5,
    muaban_per_city: int = 500,
):
    """Full cycle: scrape → pipeline → merge → final output."""
    reset_session()  # New timestamp for this run
    start = time.time()

    log.info(f"\n{'#'*60}")
    log.info(f"  V-NEXUS FULL CYCLE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'#'*60}")

    # Ensure data dirs
    for d in ["data/raw/nhatot", "data/raw/muaban", "data/clean", "data/final"]:
        os.makedirs(d, exist_ok=True)

    # === STEP 1: SCRAPE ===
    nhatot_raw = None
    muaban_raw = None

    if not skip_scrape:
        log.info(f"\n{'='*50}")
        log.info(f"  STEP 1: SCRAPE")
        log.info(f"{'='*50}")

        if not muaban_only:
            log.info("\n  [nhatot] Starting...")
            try:
                nhatot_raw = await scrape_nhatot(count=nhatot_count, tabs=nhatot_tabs)
                log.info(f"  [nhatot] Done → {nhatot_raw}")
            except Exception as e:
                log.error(f"  [nhatot] Failed: {e}")

        if not nhatot_only:
            log.info("\n  [muaban] Starting...")
            try:
                muaban_raw = await scrape_muaban(per_city=muaban_per_city)
                log.info(f"  [muaban] Done → {muaban_raw}")
            except Exception as e:
                log.error(f"  [muaban] Failed: {e}")

    else:
        log.info("\n  Skipping scrape (--skip-scrape)")

    # Find latest raw files if not just scraped
    if not nhatot_raw:
        nhatot_raw = find_latest_raw("nhatot")
    if not muaban_raw:
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
        final = run_merge(nhatot_clean, muaban_clean)
    elif nhatot_clean:
        fp = final_path()
        with open(nhatot_clean) as f:
            data = json.load(f)
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"  Single source (nhatot) → {fp}")
        final = data.get("listings", [])
    elif muaban_clean:
        fp = final_path()
        with open(muaban_clean) as f:
            data = json.load(f)
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"  Single source (muaban) → {fp}")
        final = data.get("listings", [])
    else:
        log.error("No clean data to merge")
        return None

    # === SUMMARY ===
    elapsed = int(time.time() - start)
    final_file = find_latest_final()

    if final_file:
        with open(final_file) as f:
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
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping, use existing data")
    parser.add_argument("--nhatot-only", action="store_true", help="Only scrape nhatot")
    parser.add_argument("--muaban-only", action="store_true", help="Only scrape muaban")
    parser.add_argument("--nhatot-count", type=int, default=500, help="Nhatot listings count")
    parser.add_argument("--nhatot-tabs", type=int, default=5, help="Nhatot browser tabs")
    parser.add_argument("--muaban-per-city", type=int, default=500, help="Muaban listings per city")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval (minutes)")
    args = parser.parse_args()

    if args.loop:
        asyncio.run(loop(
            interval_min=args.interval,
            nhatot_count=args.nhatot_count,
            nhatot_tabs=args.nhatot_tabs,
            muaban_per_city=args.muaban_per_city,
        ))
    else:
        asyncio.run(full_cycle(
            skip_scrape=args.skip_scrape,
            nhatot_only=args.nhatot_only,
            muaban_only=args.muaban_only,
            nhatot_count=args.nhatot_count,
            nhatot_tabs=args.nhatot_tabs,
            muaban_per_city=args.muaban_per_city,
        ))
