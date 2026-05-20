"""
Scheduler jobs — orchestrator chạy 1 cycle cho 1 source:
    crawl mới → normalize (DTO) → map sang row data_sources →
    upsert (ON CONFLICT) → classify poster_type (batch).

Mỗi source có 1 hàm async độc lập. Lỗi trong 1 source KHÔNG affect source kia
(scheduler/main.py catch tại lớp ngoài).

Chỉ crawl **tin mới nhất** mỗi cycle — giảm tải đối tác và tránh tự DDOS.
Tham số quota:
    - nhatot: `max_listings` (chia đều ban/cho-thue khi transaction=both)
    - muaban: `per_city` (sweep qua list cities mặc định)

Cấu hình qua env:
    SCRAPE_NHATOT_LIMIT (default 50)
    SCRAPE_NHATOT_TABS  (default 3)
    SCRAPE_NHATOT_TRANSACTION (ban|cho-thue|both, default both)
    SCRAPE_MUABAN_PER_CITY (default 30)
    SCRAPE_MUABAN_CATEGORY (default all)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

_base = Path(__file__).resolve().parent.parent
if str(_base) not in sys.path:
    sys.path.insert(0, str(_base))
if str(_base / "scrapers") not in sys.path:
    sys.path.insert(0, str(_base / "scrapers"))
if str(_base / "pipeline") not in sys.path:
    sys.path.insert(0, str(_base / "pipeline"))

from classification.poster import classify_batch_in_db
from db.repository import fetch_rows_for_classification, upsert_data_sources
from db.session import session_scope
from pipeline.normalize_to_db import dto_to_row

log = logging.getLogger("scheduler.jobs")


# ============================================================
# Config — env-driven, không hardcode
# ============================================================

def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _nhatot_limit() -> int:
    return _env_int("SCRAPE_NHATOT_LIMIT", 50)


def _nhatot_tabs() -> int:
    return _env_int("SCRAPE_NHATOT_TABS", 3)


def _nhatot_transaction() -> str:
    v = (os.getenv("SCRAPE_NHATOT_TRANSACTION") or "both").strip().lower()
    return v if v in {"ban", "cho-thue", "both"} else "both"


def _muaban_per_city() -> int:
    return _env_int("SCRAPE_MUABAN_PER_CITY", 30)


def _muaban_category() -> str:
    return (os.getenv("SCRAPE_MUABAN_CATEGORY") or "all").strip().lower()


# ============================================================
# Helpers — pair adapted DTOs back to their raw payloads
# ============================================================

def _build_raw_map(raw_items: list[dict], source: str) -> dict[str, dict]:
    """Map source_id (string) → raw dict, dùng để cấp raw_payload cho từng DTO khi map sang row."""
    by_id: dict[str, dict] = {}
    for r in raw_items:
        if source == "nhatot":
            sid = r.get("ad_id") or r.get("list_id")
        else:
            sid = r.get("id") or r.get("source_id")
        if sid is not None:
            by_id[str(sid)] = r
    return by_id


def _process_dtos_to_db(
    source: str,
    dtos: list[Any],
    raw_items: list[dict],
) -> dict[str, Any]:
    """Common path: normalize → upsert → classify cho cả 2 source.

    Trả dict thống kê {total, inserted, updated, moi_gioi, chu_nha}.
    """
    if not dtos:
        return {"total": 0, "inserted": 0, "updated": 0, "moi_gioi": 0, "chu_nha": 0}

    raw_map = _build_raw_map(raw_items, source)
    rows: list[dict] = []
    keys: list[tuple[str, str]] = []
    for dto in dtos:
        d = asdict(dto) if is_dataclass(dto) else dto
        sid = str(d.get("source_id")) if d.get("source_id") is not None else None
        raw = raw_map.get(sid, {}) if sid else {}
        row = dto_to_row(dto, raw)
        if row:
            rows.append(row)
            keys.append((row["source"], row["source_id"]))

    with session_scope() as session:
        upsert_stats = upsert_data_sources(session, rows)
        # Đọc lại các row vừa upsert (đã có id + phone_full sau merge) để classify.
        orm_rows = fetch_rows_for_classification(session, keys)
        classify_stats = classify_batch_in_db(session, orm_rows)

    return {
        "total": len(rows),
        "inserted": upsert_stats["inserted"],
        "updated": upsert_stats["updated"],
        "moi_gioi": classify_stats["moi_gioi"],
        "chu_nha": classify_stats["chu_nha"],
    }


# ============================================================
# Source jobs — crawl + DB pipeline (async)
# ============================================================

async def run_nhatot_job() -> dict[str, Any]:
    """Crawl Nhà Tốt → upsert → classify. Bao mọi error trong source này."""
    start = time.time()
    log.info("[nhatot] cycle start limit=%d tabs=%d tx=%s",
             _nhatot_limit(), _nhatot_tabs(), _nhatot_transaction())

    try:
        # Import lazy — tránh load Playwright khi chỉ chạy mỗi muaban.
        from nhatot_fast_scraper import run_cycle as nhatot_cycle

        result = await nhatot_cycle(
            max_listings=_nhatot_limit(),
            num_tabs=_nhatot_tabs(),
            batch_size=20,
            transaction=_nhatot_transaction(),
        )
    except Exception as e:
        log.exception("[nhatot] crawl FAILED: %s", e)
        return {"source": "nhatot", "ok": False, "error": str(e)}

    # `run_cycle` ghi file JSON + trả dict {file, total, ...}; cần re-load để
    # lấy listings DTO + raw ads. Để giảm IO, lấy DTO trực tiếp từ file final.
    file_path = result.get("file") if isinstance(result, dict) else None
    listings: list[dict] = []
    if file_path and Path(file_path).exists():
        import json
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        listings = data.get("listings", [])

    if not listings:
        log.warning("[nhatot] no listings produced")
        return {"source": "nhatot", "ok": True, "stats": {"total": 0}}

    # DTOs nhatot không kèm raw — pipeline đã gộp tất cả thông tin vào DTO,
    # raw_data lưu chính DTO (chấp nhận, vì DTO chứa đủ ngữ cảnh).
    try:
        stats = _process_dtos_to_db("nhatot", listings, listings)
    except Exception as e:
        log.exception("[nhatot] DB pipeline FAILED: %s", e)
        return {"source": "nhatot", "ok": False, "error": f"db: {e}"}

    elapsed = int(time.time() - start)
    log.info(
        "[nhatot] DONE elapsed=%ds total=%d (insert=%d update=%d) "
        "moi_gioi=%d chu_nha=%d",
        elapsed, stats["total"], stats["inserted"], stats["updated"],
        stats["moi_gioi"], stats["chu_nha"],
    )
    return {"source": "nhatot", "ok": True, "stats": stats, "elapsed_sec": elapsed}


async def run_muaban_job() -> dict[str, Any]:
    """Crawl Mua Bán → upsert → classify. Bao mọi error trong source này."""
    start = time.time()
    log.info("[muaban] cycle start per_city=%d category=%s",
             _muaban_per_city(), _muaban_category())

    try:
        from muaban_scraper import run_cycle as muaban_cycle

        result = await muaban_cycle(
            per_city=_muaban_per_city(),
            category=_muaban_category(),
        )
    except Exception as e:
        log.exception("[muaban] crawl FAILED: %s", e)
        return {"source": "muaban", "ok": False, "error": str(e)}

    file_path = result.get("file") if isinstance(result, dict) else None
    items: list[dict] = []
    if file_path and Path(file_path).exists():
        import json
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
        items = data.get("items", [])

    if not items:
        log.warning("[muaban] no items produced")
        return {"source": "muaban", "ok": True, "stats": {"total": 0}}

    # muaban scraper trả raw items → cần qua unified_pipeline.process_batch
    # để có DTO chuẩn (đã chạy address mapping + classify cũ, nhưng OK — ta sẽ
    # reclassify với rule-based mới).
    try:
        from unified_pipeline import AddressMapper, process_batch

        ref_dir = str(_base / "pipeline" / "reference")
        try:
            mapper = AddressMapper(ref_dir)
        except Exception as e:
            log.warning("[muaban] AddressMapper unavailable: %s", e)
            mapper = None

        # use_db_phone_history=False: classification của process_batch sẽ bị
        # classify_batch_in_db ghi đè, nên bỏ query phone-history DB thừa ở đây
        # (chỉ giữ address-mapping). Tiết kiệm 1 round-trip DB mỗi cycle.
        dtos = process_batch(items, "muaban", mapper, use_db_phone_history=False)
        stats = _process_dtos_to_db("muaban", dtos, items)
    except Exception as e:
        log.exception("[muaban] DB pipeline FAILED: %s", e)
        return {"source": "muaban", "ok": False, "error": f"db: {e}"}

    elapsed = int(time.time() - start)
    log.info(
        "[muaban] DONE elapsed=%ds total=%d (insert=%d update=%d) "
        "moi_gioi=%d chu_nha=%d",
        elapsed, stats["total"], stats["inserted"], stats["updated"],
        stats["moi_gioi"], stats["chu_nha"],
    )
    return {"source": "muaban", "ok": True, "stats": stats, "elapsed_sec": elapsed}


async def run_all_sources_once() -> list[dict[str, Any]]:
    """1 cycle: chạy 2 source SONG SONG để lỗi nguồn này không block nguồn kia.

    `asyncio.gather(return_exceptions=True)` đảm bảo coroutine fail → trả về
    Exception thay vì raise; ta convert thành dict {ok: False}.
    """
    results = await asyncio.gather(
        run_nhatot_job(),
        run_muaban_job(),
        return_exceptions=True,
    )
    out: list[dict[str, Any]] = []
    for r in results:
        if isinstance(r, Exception):
            log.exception("source job raised: %s", r)
            out.append({"ok": False, "error": str(r)})
        else:
            out.append(r)
    return out


def run_all_sources_once_sync() -> list[dict[str, Any]]:
    """APScheduler dùng được job sync — wrap async vào event loop riêng."""
    return asyncio.run(run_all_sources_once())
