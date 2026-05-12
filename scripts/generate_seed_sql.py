"""Generate INSERT SQL for users + listings from a merged scraper JSON file."""
import json
import sys
from pathlib import Path
from urllib.parse import quote

SRC = Path(__file__).resolve().parent.parent / "data" / "final" / "2026-04-20" / "141425_merged.json"
DST = Path(__file__).resolve().parent.parent / "data" / "final" / "2026-04-20" / "141425_seed.sql"

# Bcrypt placeholder for the literal string "ChangeMe@123" (10 rounds).
PASSWORD_HASH = "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"

# Avatar: dùng ui-avatars.com — sinh avatar từ chữ cái đầu của tên với 1 màu nền cố định.
# Màu brand V-Nexus: nền hồng nhạt (#FFECEB), chữ đỏ đậm (#74150F).
AVATAR_BG = "FFECEB"
AVATAR_FG = "74150F"


def gen_avatar_url(full_name: str) -> str:
    """Sinh URL avatar từ tên — sử dụng ui-avatars.com với màu nền cố định."""
    name = (full_name or "Người dùng").strip()
    # ui-avatars tự động lấy initials từ tên (tối đa 2 chữ cái)
    return (
        f"https://ui-avatars.com/api/"
        f"?name={quote(name)}"
        f"&background={AVATAR_BG}"
        f"&color={AVATAR_FG}"
        f"&size=256"
        f"&bold=true"
        f"&format=png"
    )

# Map property_type slug + transaction_type → property_types.slug (already same), but
# the listings table itself only stores listing_type SALE/RENT. property_type goes into attributes JSONB.

# Mirror of property_types.attributes_schema.fields, keyed by (slug, listing_type).
# Single source of truth for which keys are allowed in listings.attributes JSONB.
PROPERTY_TYPE_FIELDS = {
    ("can-ho-chung-cu"): ["bedrooms", "bathrooms", "house_direction", "balcony_direction", "interior", "floor_number", "tower_block", "unit_number"],
    ("chung-cu-mini-can-ho-dich-vu"): ["bedrooms", "bathrooms", "house_direction", "balcony_direction", "interior", "floor_number"],
    ("nha-o"): ["bedrooms", "bathrooms", "floors", "frontage", "access_road", "house_direction", "balcony_direction", "interior"],
    ("nha-biet-thu-doc-lap"): ["bedrooms", "bathrooms", "floors", "frontage", "access_road", "house_direction", "balcony_direction", "interior"],
    ("nha-biet-thu-lien-ke"): ["bedrooms", "bathrooms", "floors", "frontage", "access_road", "house_direction", "balcony_direction", "interior"],
    ("shophouse"): ["bedrooms", "bathrooms", "floors", "frontage", "access_road", "house_direction", "balcony_direction", "interior"],
    ("penhouse"): ["bedrooms", "bathrooms", "floors", "frontage", "access_road", "house_direction", "balcony_direction", "interior"],
    ("dat-tho-cu"): ["access_road", "house_direction", "frontage"],
    ("dat-nen-du-an"): ["access_road", "house_direction", "frontage"],
    ("dat-nong-nghiep"): ["access_road", "house_direction", "frontage"],
    ("trang-trai-khu-nghi-duong"): ["bedrooms", "bathrooms", "floors", "frontage", "access_road", "house_direction", "balcony_direction", "interior"],
    ("kho-nha-xuong"): ["frontage", "height", "bathrooms", "house_direction", "access_road"],
    ("loai-bds-khac"): [],
}


def build_attributes(listing, slug, listing_type_lower):
    """Build attributes JSONB containing ONLY keys defined in property_types.attributes_schema.

    PROPERTY_TYPE_FIELDS keys = slug (string). Allowed field names: English snake_case.
    """
    allowed = PROPERTY_TYPE_FIELDS.get(slug, [])
    if not allowed:
        return {}

    # Source-of-truth values mapped to English schema field names.
    source_map = {
        "bedrooms": listing.get("bedrooms"),
        "bathrooms": listing.get("bathrooms"),
        "floors": listing.get("floors"),             # number of stories (houses)
        "floor_number": listing.get("floors"),       # which floor (apartments)
        "house_direction": listing.get("direction"),
        "balcony_direction": None,
        "interior": None,
        "frontage": None,
        "access_road": None,
        "height": None,
        "tower_block": None,
        "unit_number": None,
    }
    return {k: source_map.get(k) for k in allowed}


def sql_str(v):
    if v is None:
        return "NULL"
    s = str(v).replace("'", "''")
    return f"'{s}'"


def sql_num(v):
    return "NULL" if v is None else str(v)


def sql_ts(v):
    if v is None:
        return "NULL"
    return f"TIMESTAMPTZ '{v}'"


def sql_text_array(items):
    if not items:
        return "NULL"
    parts = []
    for x in items:
        s = str(x).replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'"{s}"')
    inner = ",".join(parts)
    inner_escaped = inner.replace("'", "''")
    return f"ARRAY[{','.join(sql_str(x) for x in items)}]::TEXT[]"


def sql_jsonb(d):
    if d is None:
        return "NULL"
    s = json.dumps(d, ensure_ascii=False).replace("'", "''")
    return f"'{s}'::JSONB"


def role_for(poster_type):
    if poster_type == "moi_gioi":
        return "ARRAY['ROLE_BROKER']::TEXT[]"
    return "ARRAY['ROLE_USER']::TEXT[]"


def main():
    with SRC.open(encoding="utf-8") as f:
        data = json.load(f)

    listings = data["listings"]

    # Dedupe users by phone — take first contact_name / location encountered.
    users_by_phone = {}
    for x in listings:
        phone = x.get("phone_full")
        if not phone or phone in users_by_phone:
            continue
        users_by_phone[phone] = {
            "phone": phone,
            "full_name": x.get("contact_name") or "Người dùng",
            "province": x.get("province"),
            "ward": x.get("ward"),
            "poster_type": x.get("poster_type"),
        }

    out = []
    out.append("-- Auto-generated from 141425_merged.json")
    out.append(f"-- Users: {len(users_by_phone)} | Listings: {len(listings)}")
    out.append("")
    out.append("BEGIN;")
    out.append("")
    out.append("-- =========================== USERS ===========================")
    out.append(
        "INSERT INTO users (phone, email, password, bio, full_name, avatar_url, "
        "province, ward, refresh_token, refresh_token_expires_at, roles, status, last_login_at) VALUES"
    )
    rows = []
    for u in users_by_phone.values():
        avatar = gen_avatar_url(u['full_name'])
        rows.append(
            "  ("
            f"{sql_str(u['phone'])}, "
            "NULL, "
            f"{sql_str(PASSWORD_HASH)}, "
            "NULL, "
            f"{sql_str(u['full_name'])}, "
            f"{sql_str(avatar)}, "
            f"{sql_str(u['province'])}, "
            f"{sql_str(u['ward'])}, "
            "NULL, NULL, "
            f"{role_for(u['poster_type'])}, "
            "'ACTIVE', "
            "NULL"
            ")"
        )
    out.append(",\n".join(rows) + "\nON CONFLICT (phone) DO NOTHING;")
    out.append("")
    out.append("-- =========================== LISTINGS ===========================")

    for x in listings:
        phone = x.get("phone_full")
        if not phone:
            continue

        listing_type_lower = "sale" if x.get("transaction_type") == "ban" else "rent"
        # listings.listing_types VARCHAR — dùng enum-style uppercase (SALE / RENT)
        listing_types = "SALE" if listing_type_lower == "sale" else "RENT"
        slug = x.get("property_type")
        address = x.get("full_address") or x.get("street")

        # attributes JSONB: shaped strictly by property_types.attributes_schema.fields
        attrs = build_attributes(x, slug, listing_type_lower)

        out.append(
            "INSERT INTO listings ("
            "user_id, title, description, price, province, ward, address, area_m2, "
            "legal_document, status, trust_score, listing_types, property_types, "
            "image_urls, attributes, published_at, created_at, updated_at"
            ") VALUES ("
            f"(SELECT id FROM users WHERE phone = {sql_str(phone)}), "
            f"{sql_str(x.get('title'))}, "
            f"{sql_str(x.get('description'))}, "
            f"{sql_num(x.get('price'))}, "
            f"{sql_str(x.get('province'))}, "
            f"{sql_str(x.get('ward'))}, "
            f"{sql_str(address)}, "
            f"{sql_num(x.get('area'))}, "
            f"{sql_str(x.get('legal_document'))}, "
            "'ACTIVE', "
            "NULL, "  # trust_score — scraper no longer computes this score
            f"{sql_str(listing_types)}, "
            f"{sql_str(slug)}, "
            f"{sql_text_array(x.get('images') or [])}, "
            f"{sql_jsonb(attrs)}, "
            f"{sql_ts(x.get('posted_at'))}, "
            f"{sql_ts(x.get('scraped_at'))}, "
            f"{sql_ts(x.get('scraped_at'))}"
            ");"
        )

    out.append("")
    out.append("COMMIT;")

    DST.write_text("\n".join(out), encoding="utf-8")
    print(f"Wrote {DST}")
    print(f"Users: {len(users_by_phone)} | Listings: {sum(1 for x in listings if x.get('phone_full'))}")


if __name__ == "__main__":
    main()
