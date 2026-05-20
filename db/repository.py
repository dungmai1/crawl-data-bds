"""
Upsert repository cho bảng `data_sources`.

Dùng `INSERT ... ON CONFLICT (source, source_id) DO UPDATE` (Postgres) để bảo đảm
idempotent: chạy lại cùng input → không tạo duplicate, chỉ refresh các field
được phép update.

Quy tắc update:
  - Luôn refresh: source_url, title, description, raw_data, property_type,
    transaction_type, price, area, province, ward, contact_name, posted_at,
    scraped_at, last_seen_at, updated_at.
  - phone_full BẢO TOÀN: nếu DB đã có giá trị → không ghi đè bằng NULL/masked,
    chỉ cập nhật khi row mới mang phone đầy đủ. (Tránh trường hợp lần crawl
    sau không reveal được phone → mất phone đã có.)
  - phone_masked: refresh nếu vẫn chưa có phone_full; nếu phone_full đã có thì
    clear masked về NULL (không cần giữ song song).
  - poster_type / poster_confidence_score / poster_classification_reason: KHÔNG
    upsert ở đây — classification chạy bước sau (xem `classification.poster`).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from .models import DataSource

log = logging.getLogger("db.repository")

# Field-list được phép set khi INSERT (mọi cột có data).
_INSERT_FIELDS = (
    "source",
    "source_id",
    "source_url",
    "title",
    "description",
    "raw_data",
    "property_type",
    "transaction_type",
    "price",
    "area",
    "province",
    "ward",
    "phone_masked",
    "phone_full",
    "contact_name",
    "posted_at",
    "scraped_at",
    "last_seen_at",
)


def upsert_data_sources(session: Session, rows: Iterable[dict]) -> dict[str, int]:
    """Bulk upsert vào `data_sources`. Trả counters {inserted, updated, skipped}.

    Vì Postgres không phân biệt được INSERT vs UPDATE trong cùng 1 statement
    không có RETURNING xmax, ta đếm "affected" thay vì phân biệt chính xác —
    để biết chính xác phải pre-fetch (source, source_id), đắt khi batch lớn.
    Ở đây dùng pre-fetch (1 query SELECT) vì batch thường < 1000 rows.
    """
    rows = [r for r in rows if r]
    if not rows:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    # Tách rows có identity duy nhất — phòng trường hợp 1 batch chứa 2 lần cùng source_id
    seen: dict[tuple[str, str], dict] = {}
    skipped = 0
    for r in rows:
        key = (r.get("source"), r.get("source_id"))
        if not (key[0] and key[1]):
            skipped += 1
            continue
        # Tin sau ghi đè tin trước (caller chịu trách nhiệm thứ tự).
        seen[key] = r
    if not seen:
        return {"inserted": 0, "updated": 0, "skipped": skipped}

    unique_keys = list(seen.keys())

    # Pre-fetch existing keys (chỉ để đếm insert vs update). Dùng tuple IN nên
    # khớp đúng từng cặp (source, source_id), không cartesian.
    existing_q = select(DataSource.source, DataSource.source_id).where(
        tuple_(DataSource.source, DataSource.source_id).in_(unique_keys)
    )
    existing: set[tuple[str, str]] = {
        (src, sid) for src, sid in session.execute(existing_q).all()
    }

    # Bóc rows + đảm bảo updated_at
    now = datetime.now(tz=timezone.utc)
    insert_payload: list[dict] = []
    for key, row in seen.items():
        payload = {k: row.get(k) for k in _INSERT_FIELDS}
        # last_seen_at default = scraped_at nếu thiếu
        if not payload.get("last_seen_at"):
            payload["last_seen_at"] = payload.get("scraped_at") or now
        insert_payload.append(payload)

    inserted = sum(1 for k in seen if k not in existing)
    updated = len(seen) - inserted

    stmt = insert(DataSource).values(insert_payload)

    # ON CONFLICT DO UPDATE — bảo toàn phone_full nếu DB đã có.
    excluded = stmt.excluded
    update_cols = {
        "source_url": excluded.source_url,
        "title": excluded.title,
        "description": excluded.description,
        "raw_data": excluded.raw_data,
        "property_type": excluded.property_type,
        "transaction_type": excluded.transaction_type,
        "price": excluded.price,
        "area": excluded.area,
        "province": excluded.province,
        "ward": excluded.ward,
        "contact_name": excluded.contact_name,
        "posted_at": excluded.posted_at,
        "scraped_at": excluded.scraped_at,
        "last_seen_at": excluded.last_seen_at,
        "updated_at": func.now(),
        # phone_full: COALESCE(DB.cũ, mới) — chỉ điền nếu DB đang trống.
        "phone_full": func.coalesce(DataSource.phone_full, excluded.phone_full),
        # phone_masked: nếu sau update phone_full vẫn null → giữ masked mới,
        # ngược lại clear về null (phone_full ưu tiên).
        "phone_masked": func.case(
            (
                func.coalesce(DataSource.phone_full, excluded.phone_full).isnot(None),
                None,
            ),
            else_=func.coalesce(excluded.phone_masked, DataSource.phone_masked),
        ),
    }
    stmt = stmt.on_conflict_do_update(
        constraint="uq_data_sources_source_id",
        set_=update_cols,
    )
    session.execute(stmt)

    log.info(
        "upsert data_sources: %d rows (insert=%d update=%d skip=%d)",
        len(insert_payload),
        inserted,
        updated,
        skipped,
    )
    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def fetch_rows_for_classification(
    session: Session,
    source_ids: Iterable[tuple[str, str]],
) -> list[DataSource]:
    """Đọc các row vừa upsert để classify. Trả list ORM object (dùng để update tiếp)."""
    keys = list(source_ids)
    if not keys:
        return []
    q = select(DataSource).where(
        tuple_(DataSource.source, DataSource.source_id).in_(keys)
    )
    return list(session.execute(q).scalars().all())
