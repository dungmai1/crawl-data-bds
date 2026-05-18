"""
V-Nexus: Master Scraper Runner
1 lệnh: cào nhatot + muaban → lọc (normalize/classify) → ghi file final riêng từng nguồn
→ upload ảnh lên Cloudflare R2 (thay URL gốc bằng URL R2 trong từng file final)

Usage:
    python run.py                          # Full cycle: cào cả 2 + lọc + upload ảnh
    python run.py --nhatot-only            # Chỉ cào nhatot
    python run.py --muaban-only            # Chỉ cào muaban
    python run.py --skip-upload            # Bỏ qua bước upload ảnh lên R2
    python run.py --loop --interval 60     # Chạy liên tục mỗi 60 phút

Output (per source, no merge):
    data/final/{source}/{YYYY-MM-DD}/{HHMMSS}.json
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

from config import final_path, find_latest_raw, reset_session
from unified_pipeline import process_batch, AddressMapper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger("runner")


async def scrape_nhatot(
    count: int = 500,
    tabs: int = 5,
    batch_size: int = 50,
    region: int = 13000,
    transaction: str = "both",
) -> str:
    """Run nhatot scraper. Returns output file path."""
    from nhatot_fast_scraper import run_cycle
    result = await run_cycle(
        max_listings=count,
        num_tabs=tabs,
        batch_size=batch_size,
        region=region,
        transaction=transaction,
    )
    return result.get("file") if isinstance(result, dict) else None


async def scrape_muaban(per_city: int = 200, category: str = "all") -> str:
    """Run muaban scraper. Returns output file path."""
    from muaban_scraper import run_cycle as muaban_cycle
    result = await muaban_cycle(per_city=per_city, category=category)
    return result.get("file") if isinstance(result, dict) else None


def _write_final(out: str, source: str, listings: list) -> None:
    """Write per-source final JSON with summary stats."""
    total = len(listings)
    phone_full = sum(1 for x in listings if x.get("phone_full"))
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "source": source,
            "total": total,
            "phone_full": phone_full,
            "processed_at": datetime.now().isoformat(),
            "listings": listings,
        }, f, ensure_ascii=False, indent=2)


def run_pipeline_for_source(source: str, input_file: str, mapper: AddressMapper) -> str:
    """Run unified pipeline on raw data. Returns final file path (data/final/{source}/...)."""
    with open(input_file, encoding="utf-8") as f:
        data = json.load(f)

    # nhatot_fast_scraper already outputs clean DTOs in "listings"
    # muaban_scraper outputs raw items in "items"
    if source == "nhatot":
        items = data.get("listings", data.get("ads", []))
        # Check if already processed (has 'source' field = DTO)
        if items and isinstance(items[0], dict) and items[0].get("source") == "nhatot":
            log.info(f"  [{source}] Already clean DTO ({len(items)} listings), skipping pipeline")
            out = final_path(source)
            _write_final(out, source, items)
            log.info(f"  [{source}] Final: {len(items)} listings → {out}")
            return out
        items = data.get("ads", items)
    else:
        items = data.get("items", [])

    log.info(f"  [{source}] Processing {len(items)} raw items through pipeline...")
    results = process_batch(items, source, mapper)

    from dataclasses import asdict
    out = final_path(source)
    _write_final(out, source, [asdict(r) for r in results])
    log.info(f"  [{source}] Final: {len(results)} listings → {out}")
    return out


async def full_cycle(
    nhatot_only: bool = False,
    muaban_only: bool = False,
    nhatot_count: int = 500,
    nhatot_tabs: int = 5,
    nhatot_batch_size: int = 50,
    nhatot_transaction: str = "both",
    muaban_per_city: int = 200,
    muaban_category: str = "all",
    skip_upload: bool = False,
):
    """Full cycle: scrape → pipeline → per-source final output → R2 image upload."""
    reset_session()
    start = time.time()

    log.info(f"\n{'#'*60}")
    log.info(f"  V-NEXUS FULL CYCLE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'#'*60}")

    for d in ["data/raw/nhatot", "data/raw/muaban", "data/final/nhatot", "data/final/muaban"]:
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
                transaction=nhatot_transaction,
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

    # === STEP 2: PIPELINE (normalize + classify) → per-source final ===
    log.info(f"\n{'='*50}")
    log.info(f"  STEP 2: PIPELINE (normalize + classify)")
    log.info(f"{'='*50}")

    ref_dir = str(_base / "pipeline" / "reference")
    try:
        mapper = AddressMapper(ref_dir)
    except Exception as e:
        log.warning(f"AddressMapper failed: {e}")
        mapper = None

    final_files = []
    if nhatot_raw:
        final_files.append(("nhatot", run_pipeline_for_source("nhatot", nhatot_raw, mapper)))
    if muaban_raw:
        final_files.append(("muaban", run_pipeline_for_source("muaban", muaban_raw, mapper)))

    if not final_files:
        log.error("No clean data produced")
        return None

    # === STEP 3: UPLOAD IMAGES → CLOUDFLARE R2 ===
    if not skip_upload:
        log.info(f"\n{'='*50}")
        log.info(f"  STEP 3: UPLOAD IMAGES → CLOUDFLARE R2")
        log.info(f"{'='*50}")
        try:
            from image_uploader import upload_all_in_final_file
            for src, fp in final_files:
                stats = await upload_all_in_final_file(fp)
                log.info(
                    f"  [{src}] images: {stats['uploaded']} uploaded, "
                    f"{stats['skipped']} skipped (already in R2), "
                    f"{stats['failed']} failed / {stats['total_images']} total"
                )
        except Exception as e:
            log.error(f"  Image upload skipped: {e} — final files retain original URLs")

    # === SUMMARY ===
    elapsed = int(time.time() - start)
    log.info(f"\n{'#'*60}")
    log.info(f"  CYCLE COMPLETE in {elapsed}s")
    for src, fp in final_files:
        with open(fp, encoding="utf-8") as f:
            d = json.load(f)
        log.info(
            f"  [{src}] total={d.get('total', 0)}  "
            f"phone_full={d.get('phone_full', 0)}"
        )
        log.info(f"         → {fp}")
    log.info(f"{'#'*60}")

    return [fp for _, fp in final_files]


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
    parser.add_argument(
        "--nhatot-transaction",
        choices=["ban", "cho-thue", "both"],
        default="both",
        help="Nhatot transaction type: ban | cho-thue | both (default: both)",
    )
    parser.add_argument("--muaban-per-city", type=int, default=200, help="Muaban listings per city")
    parser.add_argument("--muaban-category", default="all", help="Muaban subcategory (e.g. dat-tho-cu). Default: all")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval (minutes)")
    parser.add_argument("--skip-upload", action="store_true", help="Skip uploading images to Cloudflare R2")
    args = parser.parse_args()

    if args.loop:
        asyncio.run(loop(
            interval_min=args.interval,
            nhatot_only=args.nhatot_only,
            muaban_only=args.muaban_only,
            nhatot_count=args.nhatot_count,
            nhatot_tabs=args.nhatot_tabs,
            nhatot_batch_size=args.nhatot_batch_size,
            nhatot_transaction=args.nhatot_transaction,
            muaban_per_city=args.muaban_per_city,
            muaban_category=args.muaban_category,
            skip_upload=args.skip_upload,
        ))
    else:
        asyncio.run(full_cycle(
            nhatot_only=args.nhatot_only,
            muaban_only=args.muaban_only,
            nhatot_count=args.nhatot_count,
            nhatot_tabs=args.nhatot_tabs,
            nhatot_batch_size=args.nhatot_batch_size,
            nhatot_transaction=args.nhatot_transaction,
            muaban_per_city=args.muaban_per_city,
            muaban_category=args.muaban_category,
            skip_upload=args.skip_upload,
        ))
