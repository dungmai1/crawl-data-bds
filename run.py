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
    muaban_category: str = "all",
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
            log.info(f"\n  [muaban] Starting (category={muaban_category})...")
            try:
                muaban_raw = await scrape_muaban(per_city=muaban_per_city, category=muaban_category)
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
        with open(nhatot_clean, encoding="utf-8") as f:
            data = json.load(f)
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"  Single source (nhatot) → {fp}")
        final = data.get("listings", [])
    elif muaban_clean:
        fp = final_path()
        with open(muaban_clean, encoding="utf-8") as f:
            data = json.load(f)
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        log.info(f"  Single source (muaban) → {fp}")
        final = data.get("listings", [])
    else:
        log.error("No clean data to merge")
        return None

    # === STEP 4: UPLOAD IMAGES → CLOUDFLARE R2 ===
    log.info(f"\n{'='*50}")
    log.info(f"  STEP 4: UPLOAD IMAGES → CLOUDFLARE R2")
    log.info(f"{'='*50}")

    final_file_for_upload = find_latest_final()
    if final_file_for_upload:
        try:
            from image_uploader import upload_all_in_final_file
            stats = await upload_all_in_final_file(final_file_for_upload)
            log.info(
                f"  Images: {stats['uploaded']} uploaded, "
                f"{stats['skipped']} skipped (already in R2), "
                f"{stats['failed']} failed "
                f"(total {stats['total_images']})"
            )
        except Exception as e:
            log.error(f"  Image upload failed: {e} — final file retains original URLs")

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


async def accumulate_phones_nhatot(
    target: int,
    per_cycle: int = 500,
    tabs: int = 5,
    region: int = 13000,
    max_cycles: int = 20,
) -> str:
    """Loop scrape cycles cho nhatot đến khi phone_cache có >= `target` phones.

    Mỗi cycle dùng `offset_shift` tăng dần để lấy ads mới, phone_cache tránh reveal lại.
    Trả về path của file final (chứa đúng `target` listings có phone).
    """
    from nhatot_fast_scraper import load_phone_cache, run_cycle as nhatot_run_cycle

    log.info(f"\n{'#'*60}")
    log.info(f"  ACCUMULATE PHONES — target={target}, per_cycle={per_cycle}")
    log.info(f"{'#'*60}")

    reset_session()
    offset_shift = 0
    cycle = 0

    while cycle < max_cycles:
        cache = load_phone_cache()
        phones_count = sum(1 for v in cache.values() if v)
        log.info(f"\n[Accumulator] Cycle {cycle} done → phones in cache: {phones_count}/{target}")

        if phones_count >= target:
            log.info(f"  ✓ TARGET REACHED")
            break

        cycle += 1
        log.info(f"\n[Accumulator] Starting cycle #{cycle} (offset_shift={offset_shift})")

        try:
            await nhatot_run_cycle(
                max_listings=per_cycle,
                num_tabs=tabs,
                region=region,
                offset_shift=offset_shift,
            )
        except Exception as e:
            log.error(f"  Cycle #{cycle} error: {e}")

        # Shift offset cho cycle kế: mỗi cycle fetch ~per_cycle/4 ads mỗi category
        # offsets đã dùng [0, 50, ..., per_cat-50] với per_cat = max(per_cycle//4, 50)
        per_cat = max(per_cycle // 4, 50)
        offset_shift += per_cat

    # Build final file từ phone_cache + raw files tích luỹ
    # upload_r2=False vì đang ở async context — upload sẽ gọi await thủ công dưới đây
    final_file = build_phones_final(target, upload_r2=False)

    # STEP 4: Upload images → R2 (async version trong accumulator)
    log.info(f"\n{'='*50}")
    log.info(f"  STEP 4: UPLOAD IMAGES → CLOUDFLARE R2")
    log.info(f"{'='*50}")
    if final_file:
        try:
            from image_uploader import upload_all_in_final_file
            stats = await upload_all_in_final_file(final_file)
            log.info(
                f"  Images: {stats['uploaded']} uploaded, "
                f"{stats['skipped']} skipped (already in R2), "
                f"{stats['failed']} failed "
                f"(total {stats['total_images']})"
            )
        except Exception as e:
            log.error(f"  Image upload failed: {e} — final file retains original URLs")

    return final_file


async def _fetch_and_process_ads_by_ids(list_ids: list, phone_map: dict) -> list:
    """Fetch ads by list_id từ nhatot API → process qua pipeline → return list dict DTOs."""
    import httpx
    from dataclasses import asdict

    sys.path.insert(0, str(_base / "scrapers"))
    sys.path.insert(0, str(_base / "pipeline"))
    from nhatot_fast_scraper import HEADERS
    from unified_pipeline import process_batch, AddressMapper, calc_quality_score

    ads = []
    sem = asyncio.Semaphore(10)

    async with httpx.AsyncClient(headers=HEADERS, timeout=15) as client:
        async def fetch(lid):
            async with sem:
                try:
                    resp = await client.get(
                        f"https://gateway.chotot.com/v1/public/ad-listing/{lid}"
                    )
                    body = resp.json()
                    ad = body.get("ad", {})
                    if not ad:
                        return None
                    # Thêm detail fields như enrich_with_detail
                    ad["_detail"] = {
                        "body": ad.get("body", ""),
                        "street_name": ad.get("street_name", ""),
                        "ward_name": ad.get("ward_name", ""),
                        "property_legal_document": ad.get("property_legal_document"),
                    }
                    # Inject phone từ cache
                    ad["_phone_full"] = phone_map.get(str(lid))
                    return ad
                except Exception as e:
                    log.warning(f"  Fetch {lid} failed: {str(e)[:60]}")
                    return None

        results = await asyncio.gather(*[fetch(lid) for lid in list_ids])

    ads = [r for r in results if r]
    log.info(f"  API re-fetch: {len(ads)}/{len(list_ids)} ads")

    # Process qua pipeline
    ref_dir = str(_base / "pipeline" / "reference")
    try:
        mapper = AddressMapper(ref_dir)
    except Exception as e:
        log.warning(f"  AddressMapper load failed: {e}")
        mapper = None

    dtos = process_batch(ads, "nhatot", mapper)

    # Đảm bảo phone_full có mặt trên DTO
    for dto in dtos:
        if not dto.phone_full:
            dto.phone_full = phone_map.get(str(dto.source_id))
            if dto.phone_full:
                dto.quality_score = calc_quality_score(dto)

    return [asdict(d) for d in dtos]


def build_phones_final(target: int, rebuild_from_cache: bool = True, upload_r2: bool = True) -> str:
    """Build final file chứa listings có phone.

    rebuild_from_cache=True: dùng phone_cache làm nguồn chính, re-fetch ad data qua API
                             cho mọi list_id có phone (chính xác hơn, không mất listing).
    rebuild_from_cache=False: chỉ đọc từ raw files đã lưu (có thể thiếu nếu Ctrl+C).
    """
    import glob as _glob
    from dataclasses import asdict

    by_id = {}

    # Nguồn 1: Raw files đã lưu (nhanh)
    raw_files = sorted(_glob.glob("data/raw/nhatot/**/*_raw.json", recursive=True))
    log.info(f"\n[Finalize] Scanning {len(raw_files)} raw files for phones...")
    for rf in raw_files:
        try:
            with open(rf, encoding="utf-8") as f:
                data = json.load(f)
            for lst in data.get("listings", []):
                sid = lst.get("source_id")
                if not sid:
                    continue
                if lst.get("phone_full"):
                    by_id[sid] = lst
        except Exception as e:
            log.warning(f"  Skip {rf}: {e}")
    log.info(f"[Finalize] From raw files: {len(by_id)} listings")

    # Nguồn 2: Phone cache — re-fetch ads chưa có trong raw files
    if rebuild_from_cache:
        sys.path.insert(0, str(_base / "scrapers"))
        from nhatot_fast_scraper import load_phone_cache
        cache = load_phone_cache()
        cache_phones = {str(k): v for k, v in cache.items() if v and str(k).isdigit()}
        missing_ids = [int(k) for k in cache_phones.keys() if k not in by_id]

        if missing_ids:
            log.info(f"[Finalize] {len(missing_ids)} list_id trong cache chưa có trong raw → re-fetching qua API...")
            fetched = asyncio.run(_fetch_and_process_ads_by_ids(missing_ids, cache_phones))
            for dto_dict in fetched:
                sid = dto_dict.get("source_id")
                if sid and dto_dict.get("phone_full"):
                    by_id[sid] = dto_dict
            log.info(f"[Finalize] Re-fetched: {len(fetched)} listings hợp lệ")

    with_phone = list(by_id.values())
    log.info(f"[Finalize] Unique listings với phone: {len(with_phone)}")

    if len(with_phone) < target:
        log.warning(f"  ⚠ Chỉ có {len(with_phone)} phones — chưa đủ {target}. Output cả {len(with_phone)}.")
    else:
        # Ưu tiên quality cao, sau đó mới → cũ
        with_phone.sort(key=lambda x: (x.get("quality_score", 0), x.get("processed_at", "")), reverse=True)
        with_phone = with_phone[:target]

    # Ghi thành final file
    final_file = final_path()
    phones = sum(1 for l in with_phone if l.get("phone_full"))
    avg_q = sum(l.get("quality_score", 0) for l in with_phone) / max(len(with_phone), 1)
    output = {
        "source": "nhatot",
        "total": len(with_phone),
        "full_phone": phones,
        "avg_quality": round(avg_q, 1),
        "target": target,
        "listings": with_phone,
    }
    with open(final_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"\n{'#'*60}")
    log.info(f"  FINAL FILE BUILT")
    log.info(f"  Listings với phone: {len(with_phone)}/{target}")
    log.info(f"  Output: {final_file}")
    log.info(f"{'#'*60}")

    # STEP 4: Upload images → Cloudflare R2
    if upload_r2:
        log.info(f"\n{'='*50}")
        log.info(f"  STEP 4: UPLOAD IMAGES → CLOUDFLARE R2")
        log.info(f"{'='*50}")
        try:
            sys.path.insert(0, str(_base / "pipeline"))
            from image_uploader import upload_all_in_final_file
            stats = asyncio.run(upload_all_in_final_file(final_file))
            log.info(
                f"  Images: {stats['uploaded']} uploaded, "
                f"{stats['skipped']} skipped (already in R2), "
                f"{stats['failed']} failed "
                f"(total {stats['total_images']})"
            )
        except Exception as e:
            log.error(f"  Image upload failed: {e} — final file retains original URLs")

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
    parser.add_argument("--muaban-category", default="all", help="Muaban subcategory (e.g. dat-tho-cu). Default: all")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60, help="Loop interval (minutes)")
    parser.add_argument("--target-phones", type=int, default=0,
                        help="Accumulator: loop nhatot cycles đến khi đủ N listings CÓ phone. Override các flag khác.")
    parser.add_argument("--per-cycle", type=int, default=500,
                        help="Số listings mỗi cycle trong accumulator mode (default 500)")
    parser.add_argument("--max-cycles", type=int, default=20,
                        help="Giới hạn số cycle trong accumulator (default 20)")
    args = parser.parse_args()

    if args.target_phones > 0:
        asyncio.run(accumulate_phones_nhatot(
            target=args.target_phones,
            per_cycle=args.per_cycle,
            tabs=args.nhatot_tabs,
            max_cycles=args.max_cycles,
        ))
    elif args.loop:
        asyncio.run(loop(
            interval_min=args.interval,
            nhatot_count=args.nhatot_count,
            nhatot_tabs=args.nhatot_tabs,
            muaban_per_city=args.muaban_per_city,
            muaban_category=args.muaban_category,
        ))
    else:
        asyncio.run(full_cycle(
            skip_scrape=args.skip_scrape,
            nhatot_only=args.nhatot_only,
            muaban_only=args.muaban_only,
            nhatot_count=args.nhatot_count,
            nhatot_tabs=args.nhatot_tabs,
            muaban_per_city=args.muaban_per_city,
            muaban_category=args.muaban_category,
        ))
