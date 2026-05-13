"""Generate batch INSERT SQL from a scraper final JSON into 5 tables:
users, phone_registry, properties, listings, legal_documents.

Idempotent: deterministic UUIDs (uuid5) + ON CONFLICT DO NOTHING.
"""
import json
import uuid
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "data" / "final" / "nhatot" / "2026-05-13" / "100341.json"
DST = ROOT / "data" / "final" / "nhatot" / "2026-05-13" / "100341_seed.sql"

# Deterministic UUID namespaces — fixed so re-runs produce identical IDs.
NS_USER = uuid.UUID("11111111-1111-1111-1111-111111111111")
NS_PHONE_REG = uuid.UUID("22222222-2222-2222-2222-222222222222")
NS_PROPERTY = uuid.UUID("33333333-3333-3333-3333-333333333333")
NS_LISTING = uuid.UUID("44444444-4444-4444-4444-444444444444")
NS_LEGAL = uuid.UUID("55555555-5555-5555-5555-555555555555")

# Bcrypt for the literal password "ChangeMe@123" (10 rounds).
PASSWORD_HASH = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

AVATAR_BG = "FFECEB"
AVATAR_FG = "74150F"

BATCH_SIZE = 100

# Scraper/pipeline emits a few typo'd property_type codes. Map them to the
# canonical code in property_types.code before lookup, so the FK resolves.
PROPERTY_TYPE_FIXUP = {
    "penhouse": "penthouse",
}


def normalize_phone(raw: str) -> str:
    return "".join(c for c in str(raw or "") if c.isdigit())


def fix_property_type(code):
    return PROPERTY_TYPE_FIXUP.get(code, code)


def gen_avatar_url(full_name: str) -> str:
    name = (full_name or "Người dùng").strip()
    return (
        f"https://ui-avatars.com/api/?name={quote(name)}"
        f"&background={AVATAR_BG}&color={AVATAR_FG}&size=256&bold=true&format=png"
    )


def s(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def n(v):
    return "NULL" if v is None else str(v)


def ts(v):
    return "NULL" if v is None else f"TIMESTAMPTZ '{v}'"


def arr_text(items):
    if not items:
        return "NULL"
    return "ARRAY[" + ",".join(s(x) for x in items) + "]::TEXT[]"


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def emit_batched(out, header_sql, rows, on_conflict_sql=";"):
    for batch in chunked(rows, BATCH_SIZE):
        out.append(header_sql)
        out.append(",\n".join(batch))
        out.append(on_conflict_sql)
        out.append("")


def main():
    data = json.loads(SRC.read_text(encoding="utf-8"))
    listings_in = data["listings"]
    valid = [l for l in listings_in if normalize_phone(l.get("phone_full"))]

    users_by_phone = {}
    for l in valid:
        phone = normalize_phone(l["phone_full"])
        if phone in users_by_phone:
            continue
        users_by_phone[phone] = {
            "id": str(uuid.uuid5(NS_USER, phone)),
            "phone": phone,
            "full_name": (l.get("contact_name") or "Người dùng").strip()[:255],
            "province": l.get("province"),
            "ward": l.get("ward"),
        }

    out = []
    out.append(f"-- Auto-generated from {SRC.name}")
    out.append(
        f"-- Source: {data.get('source')} | Listings input: {len(listings_in)} | "
        f"Users: {len(users_by_phone)} | Listings out: {len(valid)}"
    )
    out.append("")
    out.append("BEGIN;")
    out.append("")

    out.append("-- ============ USERS ============")
    user_rows = []
    for u in users_by_phone.values():
        avatar = gen_avatar_url(u["full_name"])
        user_rows.append(
            "  ("
            f"{s(u['id'])}, {s(u['phone'])}, NULL, {s(PASSWORD_HASH)}, NULL, "
            f"{s(u['full_name'])}, {s(avatar)}, {s(u['province'])}, {s(u['ward'])}, "
            "NULL, NULL, 'ACTIVE'::user_status, NULL, 'LOCAL', false"
            ")"
        )
    emit_batched(
        out,
        "INSERT INTO users (id, phone, email, password, bio, full_name, avatar_url, "
        "province, ward, refresh_token, refresh_token_expires_at, status, last_login_at, "
        "auth_provider, email_verified) VALUES",
        user_rows,
        "ON CONFLICT (phone) DO NOTHING;",
    )

    out.append("-- ============ PHONE_REGISTRY ============")
    pr_rows = []
    for u in users_by_phone.values():
        pr_id = str(uuid.uuid5(NS_PHONE_REG, u["phone"]))
        pr_rows.append(
            f"  ({s(pr_id)}, {s(u['phone'])}, "
            f"(SELECT id FROM users WHERE phone = {s(u['phone'])}), false, NULL)"
        )
    emit_batched(
        out,
        "INSERT INTO phone_registry (id, phone, user_id, is_blocked, block_reason) VALUES",
        pr_rows,
        "ON CONFLICT (phone) DO NOTHING;",
    )

    out.append("-- ============ PROPERTIES ============")
    prop_rows = []
    for l in valid:
        phone = normalize_phone(l["phone_full"])
        sid = l["source_id"]
        prop_id = str(uuid.uuid5(NS_PROPERTY, sid))
        ptype = fix_property_type(l.get("property_type"))
        prop_rows.append(
            f"  ({s(prop_id)}, "
            f"(SELECT id FROM users WHERE phone = {s(phone)}), "
            f"(SELECT id FROM property_types WHERE code = {s(ptype)}), "
            "NULL, NULL, "
            f"{s(l.get('street'))}, "
            f"{s(l.get('ward'))}, {s(l.get('province'))}, "
            f"{n(l.get('lng'))}, {n(l.get('lat'))}, "
            f"{s(l.get('full_address'))}, NULL, "
            f"{n(l.get('area'))}, {n(l.get('width'))}, {n(l.get('length'))}, "
            "NULL, NULL, "
            f"{s(l.get('direction'))}, "
            f"{n(l.get('bedrooms'))}, {n(l.get('bathrooms'))}, "
            "NULL, NULL, NULL, NULL, NULL, "
            f"{n(l.get('floors'))}, "
            "NULL, NULL, "
            "'ACTIVE')"
        )
    emit_batched(
        out,
        "INSERT INTO properties (id, created_by_user_id, property_type_id, "
        "house_number, alley_path, street_name, ward, province, longitude, latitude, "
        "address, normalized_address, area_m2, width, length, alley_width, road_width, "
        "house_direction, bedrooms, bathrooms, balcony_direction, interior, floor_number, "
        "tower_block, unit_number, floors, distance_to_main_road, height, status) VALUES",
        prop_rows,
        "ON CONFLICT (id) DO NOTHING;",
    )

    out.append("-- ============ LISTINGS ============")
    list_rows = []
    for l in valid:
        phone = normalize_phone(l["phone_full"])
        sid = l["source_id"]
        prop_id = str(uuid.uuid5(NS_PROPERTY, sid))
        list_id = str(uuid.uuid5(NS_LISTING, sid))
        list_rows.append(
            f"  ({s(list_id)}, "
            f"(SELECT id FROM users WHERE phone = {s(phone)}), "
            f"{s(l.get('title'))}, "
            f"{s(l.get('description'))}, "
            f"{n(l.get('price'))}, "
            "'ACTIVE'::listing_status, "
            "'BAN', "
            f"{arr_text(l.get('images') or [])}, "
            f"{ts(l.get('posted_at'))}, "
            f"{s(prop_id)})"
        )
    emit_batched(
        out,
        "INSERT INTO listings (id, user_id, title, description, price, status, "
        "listing_types, image_urls, published_at, property_id) VALUES",
        list_rows,
        "ON CONFLICT (id) DO NOTHING;",
    )

    out.append("-- ============ LEGAL_DOCUMENTS ============")
    legal_rows = []
    for l in valid:
        legal = l.get("legal_document")
        if not legal:
            continue
        sid = l["source_id"]
        prop_id = str(uuid.uuid5(NS_PROPERTY, sid))
        legal_id = str(uuid.uuid5(NS_LEGAL, sid))
        doc_type = str(legal)[:50]
        legal_rows.append(f"  ({s(legal_id)}, {s(doc_type)}, {s(prop_id)})")
    emit_batched(
        out,
        "INSERT INTO legal_documents (id, document_type, property_id) VALUES",
        legal_rows,
        "ON CONFLICT (id) DO NOTHING;",
    )

    out.append("COMMIT;")

    DST.parent.mkdir(parents=True, exist_ok=True)
    DST.write_text("\n".join(out), encoding="utf-8")
    print(f"Wrote {DST}")
    print(
        f"Users: {len(users_by_phone)} | Properties: {len(valid)} | "
        f"Listings: {len(valid)} | Legal: {len(legal_rows)}"
    )


if __name__ == "__main__":
    main()
