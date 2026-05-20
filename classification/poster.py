"""
Phân loại người đăng tin (poster_type) — rule-based scoring.

Yêu cầu nghiệp vụ:
  - Chỉ có 2 nhãn hợp lệ: `moi_gioi` | `chu_nha`. KHÔNG dùng `khong_xac_dinh`.
  - Score >= 60 → `moi_gioi`. Score < 60 → `chu_nha`.
  - `poster_confidence_score` (SMALLINT 0–100) phản ánh độ tin cậy về nhãn đã chọn.
  - `poster_classification_reason` TEXT giải thích các signal đã fire.

Signals (cộng/trừ điểm):
  + Phone history: số tin cùng phone_full trong DB CÙNG SOURCE.
        >= 5 listings → +45 (strong)
        >= 2 listings → +20 (weak)
        == 0 listing  → -10 (likely owner)
  + contact_name chứa keyword môi giới (công ty/BĐS/land/realty/sale/chuyên/môi giới): +25
  + title/description chứa keyword môi giới (chuyên / nhận ký gửi / hoa hồng /
        hỗ trợ vay / bđs / land / realty / tư vấn / pháp lý / hotline /
        quỹ căn / giỏ hàng): +20
  - title/description chứa "chính chủ" / "không trung gian" / "miễn trung gian": -20

Lưu ý đặc biệt:
  - Nếu chỉ có `phone_masked` (chưa reveal phone_full) → KHÔNG dùng phone làm
    bằng chứng mạnh. Trong trường hợp này score phone = 0 (không cộng cũng
    không trừ). Khi không có signal mạnh khác → fallback `chu_nha` với
    confidence thấp (30).

Hàm chính:
  - `classify_row(row, phone_count)` — trả tuple (poster_type, score, reason).
  - `classify_batch_in_db(session, source_ids)` — preload phone history, classify
    tất cả các row trong batch, UPDATE trực tiếp DB.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterable, Optional

from sqlalchemy import case, func, select, tuple_, update
from sqlalchemy.orm import Session

from db.models import DataSource

log = logging.getLogger("classification.poster")

# ============================================================
# Keywords
# ============================================================

# Tên công ty / chuyên viên môi giới
NAME_BROKER_KEYWORDS = [
    "công ty", "cty", "cty.",
    "bđs", "bds", "bất động sản",
    "land", "realty", "real estate",
    "địa ốc",
    "chuyên viên", "chuyên",
    "môi giới", "broker",
    "sale", "sales",
    "tư vấn",
]

# Trong title / description
TEXT_BROKER_KEYWORDS = [
    "chuyên",
    "nhận ký gửi", "ký gửi",
    "hoa hồng",
    "hỗ trợ vay", "ho tro vay",
    "bđs", "bds", "bất động sản",
    "land", "realty",
    "tư vấn",
    "pháp lý",
    "hotline",
    "quỹ căn", "quy can",
    "giỏ hàng", "gio hang",
    "đầu tư sinh lời",
    "cam kết lợi nhuận",
]

# Counter-signal — chủ nhà rõ ràng
OWNER_KEYWORDS = [
    "chính chủ", "chinh chu",
    "không trung gian", "khong trung gian",
    "miễn trung gian", "mien trung gian",
    "không qua môi giới",
]

# ============================================================
# Scoring thresholds (score range 0–100)
# ============================================================

THRESHOLD_BROKER = 60          # >= 60 → moi_gioi

PHONE_STRONG_COUNT = 5         # >= 5 listings cùng phone → strong broker
PHONE_WEAK_COUNT = 2           # >= 2 listings → weak hint
PHONE_NO_HISTORY = 0           # == 0 → likely owner

SCORE_PHONE_STRONG = 45
SCORE_PHONE_WEAK = 20
SCORE_PHONE_NO_HISTORY = -10
SCORE_NAME_KW = 25
SCORE_TEXT_KW = 20
SCORE_OWNER_KW = -20

# Confidence khi không có evidence (chỉ phone_masked / null) → fallback chu_nha
LOW_CONFIDENCE_FALLBACK = 30


@dataclass
class ClassifyResult:
    poster_type: str                # 'moi_gioi' | 'chu_nha'
    confidence_score: int           # 0..100
    reason: str                     # multi-line text
    raw_score: int                  # debug
    signals: list[str]              # list of signal codes for analytics


# ============================================================
# Helpers
# ============================================================

_NORM_KEEP = re.compile(r"[^a-z0-9\sàáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]")


def _norm(s: Optional[str]) -> str:
    if not s:
        return ""
    return _NORM_KEEP.sub(" ", s.lower())


def _has_any(text: str, keywords: list[str]) -> list[str]:
    """Trả danh sách keyword match (deduped)."""
    hits = []
    for kw in keywords:
        if kw in text and kw not in hits:
            hits.append(kw)
    return hits


def _row_get(row, field: str):
    """Get attribute từ ORM row hoặc dict — dùng được cả 2."""
    if hasattr(row, field):
        return getattr(row, field)
    if isinstance(row, dict):
        return row.get(field)
    return None


# ============================================================
# Core classifier
# ============================================================

def classify_row(
    row,
    phone_count: int = 0,
) -> ClassifyResult:
    """Phân loại 1 row.

    Args:
        row: DataSource ORM hoặc dict tương đương; cần các field: phone_full,
            phone_masked, contact_name, title, description.
        phone_count: số listings cùng phone_full trong DB cùng source
            (đã loại chính row hiện tại). Caller phụ trách query.
    """
    score = 0
    signals: list[str] = []
    reason_parts: list[str] = []

    phone_full = _row_get(row, "phone_full")
    phone_masked = _row_get(row, "phone_masked")

    # ---- S1: phone history ----
    if phone_full:
        if phone_count >= PHONE_STRONG_COUNT:
            score += SCORE_PHONE_STRONG
            signals.append("PHONE_STRONG")
            reason_parts.append(
                f"phone {phone_full} đã xuất hiện {phone_count} listings cùng source "
                f"(>={PHONE_STRONG_COUNT}) → +{SCORE_PHONE_STRONG}"
            )
        elif phone_count >= PHONE_WEAK_COUNT:
            score += SCORE_PHONE_WEAK
            signals.append("PHONE_WEAK")
            reason_parts.append(
                f"phone {phone_full} xuất hiện {phone_count} listings cùng source "
                f"(>={PHONE_WEAK_COUNT}) → +{SCORE_PHONE_WEAK}"
            )
        elif phone_count == PHONE_NO_HISTORY:
            score += SCORE_PHONE_NO_HISTORY
            signals.append("PHONE_NO_HISTORY")
            reason_parts.append(
                f"phone {phone_full} chưa từng xuất hiện cùng source → "
                f"{SCORE_PHONE_NO_HISTORY}"
            )
    elif phone_masked:
        signals.append("PHONE_MASKED_ONLY")
        reason_parts.append(
            "Chỉ có phone_masked, chưa reveal được phone đầy đủ → "
            "không dùng phone làm bằng chứng mạnh"
        )

    # ---- S2: contact_name keyword ----
    contact_name = _norm(_row_get(row, "contact_name"))
    if contact_name:
        name_hits = _has_any(contact_name, NAME_BROKER_KEYWORDS)
        if name_hits:
            score += SCORE_NAME_KW
            signals.append("NAME_KW")
            reason_parts.append(
                f"contact_name chứa keyword môi giới {name_hits[:3]} → +{SCORE_NAME_KW}"
            )

    # ---- S3: title + description keyword ----
    text = _norm(_row_get(row, "title")) + " " + _norm(_row_get(row, "description"))
    text_hits = _has_any(text, TEXT_BROKER_KEYWORDS)
    if text_hits:
        score += SCORE_TEXT_KW
        signals.append("TEXT_KW")
        reason_parts.append(
            f"title/description chứa keyword môi giới {text_hits[:3]} → +{SCORE_TEXT_KW}"
        )

    # ---- O1: owner keyword counter-signal ----
    owner_hits = _has_any(text, OWNER_KEYWORDS)
    if owner_hits:
        score += SCORE_OWNER_KW
        signals.append("OWNER_KW")
        reason_parts.append(
            f"title/description chứa keyword chủ nhà {owner_hits[:3]} → "
            f"{SCORE_OWNER_KW}"
        )

    # ---- Final ----
    clamped = max(0, min(100, score))

    if score >= THRESHOLD_BROKER:
        poster_type = "moi_gioi"
        confidence = clamped
    else:
        poster_type = "chu_nha"
        # Trường hợp đặc biệt: chỉ có phone_masked, không có signal khác →
        # fallback chu_nha với confidence thấp 30.
        if (
            not phone_full
            and phone_masked
            and not any(s in signals for s in ("NAME_KW", "TEXT_KW", "OWNER_KW"))
        ):
            confidence = LOW_CONFIDENCE_FALLBACK
            reason_parts.append(
                f"Fallback chu_nha với confidence {LOW_CONFIDENCE_FALLBACK} — "
                "thiếu evidence vì phone bị che"
            )
        else:
            # Càng nhiều signal owner → confidence càng cao về nhãn chu_nha.
            # Mapping đơn giản: 100 - score (clamped).
            confidence = max(40, 100 - clamped) if signals else 50

    if not reason_parts:
        reason_parts.append("Không có signal nào fire — mặc định chu_nha")

    reason = " | ".join(reason_parts)

    return ClassifyResult(
        poster_type=poster_type,
        confidence_score=int(confidence),
        reason=reason,
        raw_score=score,
        signals=signals,
    )


# ============================================================
# Batch classify trực tiếp trong DB
# ============================================================

def _preload_phone_counts(
    session: Session,
    rows: Iterable[DataSource],
) -> dict[tuple[str, str], int]:
    """Đếm số tin trong DB cho mỗi (source, phone_full) có trong rows.

    KHÔNG trừ đi row hiện tại — caller phải `-1` khi chính row đó nằm trong
    cùng source/phone (vì row đó cũng đã tồn tại trong DB sau khi upsert).
    """
    pairs: set[tuple[str, str]] = set()
    for r in rows:
        if r.phone_full:
            pairs.add((r.source, r.phone_full))
    if not pairs:
        return {}

    q = (
        select(DataSource.source, DataSource.phone_full, func.count("*"))
        .where(tuple_(DataSource.source, DataSource.phone_full).in_(list(pairs)))
        .group_by(DataSource.source, DataSource.phone_full)
    )
    counts: dict[tuple[str, str], int] = {}
    for src, ph, cnt in session.execute(q).all():
        counts[(src, ph)] = int(cnt)
    return counts


def classify_batch_in_db(
    session: Session,
    rows: list[DataSource],
) -> dict[str, int]:
    """Classify list rows + UPDATE columns poster_type/confidence/reason trực tiếp.

    Trả counters {moi_gioi, chu_nha, total}.
    """
    if not rows:
        return {"moi_gioi": 0, "chu_nha": 0, "total": 0}

    counts_map = _preload_phone_counts(session, rows)
    stats = {"moi_gioi": 0, "chu_nha": 0, "total": 0}

    for r in rows:
        key = (r.source, r.phone_full) if r.phone_full else None
        total_for_phone = counts_map.get(key, 0) if key else 0
        # row hiện tại cũng đã có trong DB → trừ 1 cho chính nó.
        others = max(0, total_for_phone - 1)

        result = classify_row(r, phone_count=others)

        r.poster_type = result.poster_type
        r.poster_confidence_score = result.confidence_score
        r.poster_classification_reason = result.reason

        stats[result.poster_type] += 1
        stats["total"] += 1

    session.flush()
    log.info(
        "classify batch: total=%d moi_gioi=%d chu_nha=%d",
        stats["total"],
        stats["moi_gioi"],
        stats["chu_nha"],
    )
    return stats
