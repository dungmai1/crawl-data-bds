"""
Normalize layer thứ 2: PropertyDTO + raw → row khớp schema bảng `data_sources`.

`unified_pipeline.py` chuẩn hóa raw → PropertyDTO (31 fields, format JSON).
File này chuyển PropertyDTO sang dict đúng các cột data_sources:
- Bỏ các field không có trong schema (price_display, lat, lng, bedrooms, floors…).
- Tách `phone_masked` vs `phone_full` cho nhatot:
    + Nếu raw phone vẫn còn dạng "0328***868" hoặc *** → chỉ điền phone_masked.
    + Nếu reveal được phone đầy đủ (đủ 10–12 digit) → điền phone_full.
- Parse `posted_at` thành `datetime` aware.
- Đảm bảo `source ∈ {'nhatot','muaban'}` và `transaction_type ∈ {'ban','cho-thue'}`.
- `raw_data` lưu nguyên payload gốc (dùng để debug / tái phân loại).

Hàm chính: `dto_to_row(dto, raw_payload)` — không upsert, chỉ trả dict.
"""

from __future__ import annotations

import logging
import re
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, Optional

try:  # package import (pipeline.normalize_to_db)
    from .phone_history import normalize_phone
except ImportError:  # khi pipeline/ nằm trực tiếp trên sys.path
    from phone_history import normalize_phone

log = logging.getLogger("pipeline.normalize_to_db")

_ALLOWED_SOURCES = {"nhatot", "muaban"}
_ALLOWED_TX = {"ban", "cho-thue"}

# Pattern "0328***868", "+84-***-xxx" → phone đang bị che, KHÔNG ghi phone_full.
_MASKED_RE = re.compile(r"[\*xX\.]{2,}")


def _looks_masked(value: Any) -> bool:
    """True khi chuỗi chứa dấu *, x liên tiếp (>=2) — tức là phone bị che."""
    if not value:
        return False
    return bool(_MASKED_RE.search(str(value)))


def split_phone(raw_phone: Any) -> tuple[Optional[str], Optional[str]]:
    """Trả (phone_masked, phone_full). Đúng 1 trong 2 sẽ None tùy raw_phone.

    Quy ước (theo yêu cầu):
      - Phone còn dấu *** → chỉ điền phone_masked giữ nguyên chuỗi gốc (đã trim).
      - Phone đầy đủ → chỉ điền phone_full (đã chuẩn hóa digits).
      - Trống → cả 2 None.
    """
    if not raw_phone:
        return None, None
    s = str(raw_phone).strip()
    if not s:
        return None, None
    if _looks_masked(s):
        # cắt về 20 ký tự cho khớp VARCHAR(20)
        return s[:20], None
    full = normalize_phone(s)
    if full:
        return None, full
    # Có gì đó nhưng không phải digits hợp lệ — vẫn lưu nguyên dạng masked để debug.
    return s[:20], None


def _to_datetime(value: Any) -> Optional[datetime]:
    """Parse ISO string / epoch ms / datetime → datetime aware UTC. Junk → None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, (int, float)):
        # ms epoch (nhatot list_time) — heuristic > 10^12 → ms, ngược lại s
        ts = float(value) / 1000.0 if value > 1e12 else float(value)
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    s = str(value).strip()
    if not s:
        return None
    # Strip Z suffix, fromisoformat 3.11+ chấp nhận TZ offset chuẩn
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass
    # Format muaban: "2026-05-19 14:32:00"
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _truncate(value: Any, n: int) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s[:n] if s else None


def _normalize_transaction(tx: Any) -> str:
    s = (str(tx).strip().lower() if tx else "ban")
    return s if s in _ALLOWED_TX else "ban"


def _normalize_source(src: Any) -> Optional[str]:
    s = (str(src).strip().lower() if src else "")
    return s if s in _ALLOWED_SOURCES else None


def _raw_phone_from_payload(raw: dict, source: str) -> Any:
    """Lấy phone string THÔ từ raw payload — có thể là masked hoặc full."""
    if source == "nhatot":
        # Pipeline đã chèn phone_full vào _phone_full khi reveal thành công.
        # phone gốc của nhatot list API thường KHÔNG có sẵn — vẫn check cẩn thận.
        return raw.get("_phone_full") or raw.get("phone_masked") or raw.get("phone")
    if source == "muaban":
        # muaban listing API trả "phone" (đầy đủ) nếu có; detail page có thể bổ sung.
        return raw.get("phone") or raw.get("phone_full")
    return None


def dto_to_row(dto: Any, raw_payload: Optional[dict] = None) -> Optional[dict[str, Any]]:
    """Chuyển 1 PropertyDTO (hoặc dict tương đương) + raw payload → row data_sources.

    Returns:
        dict cột → giá trị, sẵn sàng upsert. None nếu thiếu identity tối thiểu
        (source / source_id) — caller skip listing đó.
    """
    # Chấp nhận cả dataclass PropertyDTO lẫn dict đã asdict().
    if is_dataclass(dto):
        d = asdict(dto)
    elif isinstance(dto, dict):
        d = dto
    else:
        log.warning("dto_to_row: unsupported dto type %s", type(dto).__name__)
        return None

    source = _normalize_source(d.get("source"))
    source_id = _truncate(d.get("source_id"), 100)
    if not (source and source_id):
        log.debug("Skip listing thiếu source/source_id: %r", d.get("source"))
        return None

    raw_payload = raw_payload if isinstance(raw_payload, dict) else {}

    # Phone — ưu tiên phone_full đã sạch của DTO; nếu DTO trống mà raw còn masked → masked.
    dto_phone_full = normalize_phone(d.get("phone_full"))
    raw_phone = _raw_phone_from_payload(raw_payload, source)
    raw_masked, raw_full = split_phone(raw_phone)

    phone_full = dto_phone_full or raw_full
    phone_masked = raw_masked if not phone_full else None

    # Area: PropertyDTO dùng float; DB NUMERIC(10,2) — round 2 decimals.
    area = d.get("area")
    try:
        area = round(float(area), 2) if area is not None else None
    except (TypeError, ValueError):
        area = None

    price = d.get("price")
    try:
        price = int(price) if price is not None else None
        if price is not None and price < 0:
            price = None
    except (TypeError, ValueError):
        price = None

    posted_at = _to_datetime(d.get("posted_at"))
    scraped_at = _to_datetime(d.get("scraped_at")) or datetime.now(tz=timezone.utc)

    return {
        "source": source,
        "source_id": source_id,
        "source_url": _truncate(d.get("source_url"), 2000),
        "title": _truncate(d.get("title"), 500),
        "description": d.get("description"),
        "raw_data": raw_payload or None,
        "property_type": _truncate(d.get("property_type"), 50),
        "transaction_type": _normalize_transaction(d.get("transaction_type")),
        "price": price,
        "area": area,
        "province": _truncate(d.get("province"), 100),
        "ward": _truncate(d.get("ward"), 100),
        "phone_masked": phone_masked,
        "phone_full": phone_full,
        "contact_name": _truncate(d.get("contact_name"), 255),
        "posted_at": posted_at,
        "scraped_at": scraped_at,
        "last_seen_at": scraped_at,
    }


def normalize_batch(
    dtos: list[Any],
    raw_items: Optional[list[dict]] = None,
) -> list[dict]:
    """Map song song dto[i] với raw_items[i] (nếu cung cấp) → list rows valid."""
    raw_items = raw_items or [{}] * len(dtos)
    if len(raw_items) != len(dtos):
        log.warning(
            "normalize_batch: len mismatch dtos=%d raw=%d — pad/truncate",
            len(dtos),
            len(raw_items),
        )
        # pad raw bằng dict rỗng
        if len(raw_items) < len(dtos):
            raw_items = list(raw_items) + [{}] * (len(dtos) - len(raw_items))
        else:
            raw_items = raw_items[: len(dtos)]
    rows: list[dict] = []
    for dto, raw in zip(dtos, raw_items):
        row = dto_to_row(dto, raw)
        if row:
            rows.append(row)
    return rows
