"""
V-Nexus phone-history lookup — per-source phone count từ DB `data_sources`.

Mục đích: trước khi phân loại 1 tin mới là MÔI GIỚI hay CHỦ NHÀ, đếm số tin cũ
trong DB có cùng (source, phone_full). Logic phân loại phía consumer dùng count
này như 1 signal (S1) trong scoring.

Quy ước:
  - Lookup CHỈ trong cùng source — KHÔNG gộp giữa nhatot và muaban
    (số phone xuất hiện nhiều ở nhatot không suy ra môi giới ở muaban, và ngược lại).
  - Source values match DB constraint: 'nhatot' | 'muaban'
    (mặc dù user labels là 'nha-tot' / 'mua-ban-nha', DB và code dùng key gốc).

Graceful failure: nếu DB unreachable / env vars thiếu / table chưa tồn tại →
trả `{}` (counts = 0 cho mọi cặp) + log WARNING. Caller vẫn classify được
bằng in-cycle counter mà không crash.

Env vars: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD (xem .env.example).
"""

from __future__ import annotations

import logging
import os
from typing import Iterable, Optional

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("phone_history")


def _connect():
    """Lấy connection tới shared Postgres; trả None nếu thiếu config/DB down.

    Ưu tiên mượn connection từ engine pool của `db.session` (tránh mở connection
    mới mỗi lần gọi); nếu không khả dụng thì fallback psycopg2 trực tiếp. Trả
    None khi thiếu env hoặc DB down — caller vẫn classify được bằng in-cycle.
    """
    host = os.getenv("DB_HOST", "").strip()
    name = os.getenv("DB_NAME", "").strip()
    user = os.getenv("DB_USER", "").strip()
    if not (host and name and user):
        log.warning("DB env vars thiếu (DB_HOST/DB_NAME/DB_USER) — bỏ qua DB phone-history")
        return None

    # Dùng chung engine pool nếu khả dụng (1 nguồn connection cho cả project).
    try:
        from db.session import get_engine

        return get_engine().raw_connection()
    except Exception as e:
        log.debug(f"pool connection không khả dụng ({e}) — thử psycopg2 trực tiếp")

    try:
        import psycopg2
    except ImportError:
        log.warning("psycopg2 không khả dụng — bỏ qua DB phone-history")
        return None
    try:
        port = os.getenv("DB_PORT", "5432").strip()
        return psycopg2.connect(
            host=host,
            port=int(port) if port else 5432,
            dbname=name,
            user=user,
            password=os.getenv("DB_PASSWORD", "").strip(),
            connect_timeout=5,
        )
    except Exception as e:
        log.warning(f"Không kết nối được DB cho phone-history ({e}) — fallback in-cycle only")
        return None


def get_phone_history_counts(
    pairs: Iterable[tuple[str, str]],
    conn=None,
    exclude_source_ids: Optional[dict[str, set[str]]] = None,
) -> dict[tuple[str, str], int]:
    """Đếm số tin cũ trong DB cho từng cặp (source, phone_full) — CÙNG SOURCE only.

    Tham số:
        pairs: iterable các tuple (source, phone). Source phải khớp DB
            constraint ('nhatot' | 'muaban'); phone là string đã normalize
            (digits, không space/dot/dash).
        conn: psycopg2 connection sẵn có. None → mở mới và đóng sau khi xong.
        exclude_source_ids: optional {source: {source_id, ...}} để loại trừ
            chính các tin đang classify khỏi count (tránh tự đếm bản thân khi
            tin đã được insert từ trước đó cùng cycle).

    Trả về:
        dict[(source, phone_full), count]. Cặp không match trong DB → không có
        trong dict (consumer treat như 0). DB unreachable → trả `{}` toàn bộ.

    Query: GROUP BY (source, phone_full) trên data_sources, filter cặp đầu vào
    bằng VALUES list (bulk; tránh N queries lẻ).
    """
    pairs = [(s, p) for s, p in pairs if s and p]
    if not pairs:
        return {}

    owned_conn = False
    if conn is None:
        conn = _connect()
        owned_conn = True
    if conn is None:
        return {}

    # Dedupe — cùng cặp lặp lại không cần query thêm
    unique_pairs = list({(s, p) for s, p in pairs})

    result: dict[tuple[str, str], int] = {}
    try:
        with conn.cursor() as cur:
            # VALUES list cho IN-clause; psycopg2 mogrify để escape an toàn
            values_sql = ",".join(
                cur.mogrify("(%s,%s)", (s, p)).decode("utf-8") for s, p in unique_pairs
            )
            exclude_clause = ""
            exclude_params: list = []
            if exclude_source_ids:
                # Loại các source_id đang xử lý ra khỏi count để không tự đếm bản thân
                conds = []
                for src, ids in exclude_source_ids.items():
                    if not ids:
                        continue
                    placeholders = ",".join(["%s"] * len(ids))
                    conds.append(f"NOT (source = %s AND source_id IN ({placeholders}))")
                    exclude_params.append(src)
                    exclude_params.extend(ids)
                if conds:
                    exclude_clause = " AND " + " AND ".join(conds)

            sql = (
                "SELECT source, phone_full, COUNT(*) AS cnt "
                "FROM data_sources "
                f"WHERE phone_full IS NOT NULL AND (source, phone_full) IN ({values_sql})"
                f"{exclude_clause} "
                "GROUP BY source, phone_full"
            )
            cur.execute(sql, exclude_params)
            for source, phone, cnt in cur.fetchall():
                result[(source, phone)] = int(cnt)
        log.info(
            f"DB phone-history: queried {len(unique_pairs)} cặp, "
            f"{len(result)} có lịch sử (>=1 tin cũ)"
        )
    except Exception as e:
        log.warning(f"DB query phone-history failed ({e}) — fallback in-cycle only")
        return {}
    finally:
        if owned_conn:
            try:
                conn.close()
            except Exception:
                pass

    return result


def normalize_phone(value) -> Optional[str]:
    """Chuẩn hóa phone về digits-only string (10–12 ký tự). Junk → None.

    Dùng chung giữa scraper output (đôi khi có space/dot/dash) và DB key
    để bảo đảm key của count map khớp đúng.
    """
    if not value:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not (10 <= len(digits) <= 12):
        return None
    # +84xxxxxxxxx (12 digits) → 0xxxxxxxxx (10 digits) để khớp data_sources convention
    if len(digits) == 11 and digits.startswith("84"):
        digits = "0" + digits[2:]
    return digits
