"""
V-Nexus Unified Data Pipeline
Chuẩn hóa data từ TẤT CẢ sources (nhatot, muaban, facebook) vào 1 DTO duy nhất.

Pipeline:
  Raw JSON → Source Adapter → Unified DTO → Address Mapping (34 tỉnh) →
  Price Validation → Property Classification → Broker Detection →
  Clean JSON / DB Insert

Output DTO fields: 31 fields chuẩn, format thống nhất
"""

import json
import re
import os
import logging
import unicodedata
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, List
from pathlib import Path

log = logging.getLogger("pipeline")

# ============================================================
# UNIFIED DTO — 31 fields chuẩn cho mọi source
# ============================================================

@dataclass
class PropertyDTO:
    # Identity
    source: str                          # nhatot | muaban
    source_id: str                       # ID gốc từ source
    source_url: Optional[str] = None     # URL bài đăng gốc

    # Content
    title: str = ""                      # Cleaned title
    description: Optional[str] = None    # Max 500 chars

    # Classification — slug khớp property_types.sql (13 loại hình):
    # can-ho-chung-cu | chung-cu-mini-can-ho-dich-vu | nha-o |
    # nha-biet-thu-doc-lap | nha-biet-thu-lien-ke | shophouse | penhouse |
    # dat-tho-cu | dat-nen-du-an | dat-nong-nghiep |
    # trang-trai-khu-nghi-duong | kho-nha-xuong | loai-bds-khac
    property_type: Optional[str] = None
    transaction_type: str = "ban"        # ban | cho-thue

    # Price
    price: Optional[int] = None          # VND
    price_display: Optional[str] = None  # "9,5 tỷ"
    price_per_m2: Optional[int] = None   # VND/m2
    price_unit: str = "VND"

    # Area
    area: Optional[float] = None         # m2
    width: Optional[float] = None        # Chiều ngang / mặt tiền (m)
    length: Optional[float] = None       # Chiều dài (m)

    # Address (NEW 34-province system)
    province: Optional[str] = None       # Tỉnh/TP mới (post 01/07/2025)
    ward: Optional[str] = None           # Phường/Xã mới
    street: Optional[str] = None         # Đường
    full_address: Optional[str] = None   # Ghép: đường, phường, tỉnh (KHÔNG có quận)

    # Address legacy (giữ quận cũ để reference, không hiển thị)
    district_legacy: Optional[str] = None

    # Geo
    lat: Optional[float] = None
    lng: Optional[float] = None

    # Details
    bedrooms: Optional[int] = None
    bathrooms: Optional[int] = None
    floors: Optional[int] = None
    direction: Optional[str] = None      # "Đông", "Tây", "Nam", "Bắc", "Đông Nam"... (see DIRECTION_MAP)
    legal_document: Optional[str] = None # "Sổ hồng", "Hợp đồng mua bán", "Giấy phép xây dựng", "Giấy tờ hợp lệ", "Khác" (see LEGAL_MAP)

    # Contact
    phone_full: Optional[str] = None     # Full 10 digits
    contact_name: Optional[str] = None

    # Media
    images: List[str] = field(default_factory=list)
    image_count: int = 0

    # Poster classification
    poster_type: Optional[str] = None    # moi_gioi | chu_nha | khong_xac_dinh

    # Metadata
    posted_at: Optional[str] = None      # ISO 8601
    scraped_at: str = ""                 # ISO 8601


# ============================================================
# ADDRESS MAPPING (34 provinces, post 01/07/2025)
# ============================================================

class AddressMapper:
    def __init__(self, ref_dir: str = "scraper/pipeline/reference"):
        self.ref_dir = Path(ref_dir)
        self._load_mappings()

    def _load_mappings(self):
        # Province mapping: old name -> new name
        with open(self.ref_dir / "v1_provinces.json", encoding="utf-8") as f:
            self.v1 = json.load(f)
        with open(self.ref_dir / "v2_provinces.json", encoding="utf-8") as f:
            self.v2 = json.load(f)

        # Build ward lookup: (v1_province, v1_district, v1_ward) -> new info
        v2_ward_by_code = {}
        for p in self.v2:
            for w in p.get("wards", []):
                v2_ward_by_code[w["code"]] = {"name": w["name"], "province": p["name"]}

        self.ward_map = {}  # (old_prov, old_dist, old_ward) -> {new_ward, new_province}
        for p in self.v1:
            for d in p.get("districts", []):
                for w in d.get("wards", []):
                    if w["code"] in v2_ward_by_code:
                        new = v2_ward_by_code[w["code"]]
                        self.ward_map[(p["name"], d["name"], w["name"])] = new

        # Province name aliases
        self.prov_aliases = {
            "Tp Hồ Chí Minh": "Thành phố Hồ Chí Minh",
            "TP Hồ Chí Minh": "Thành phố Hồ Chí Minh",
            "TP.HCM": "Thành phố Hồ Chí Minh",
            "TPHCM": "Thành phố Hồ Chí Minh",
            "Hà Nội": "Thành phố Hà Nội",
            "Đà Nẵng": "Thành phố Đà Nẵng",
            "Huế": "Thành phố Huế",
            "Hải Phòng": "Thành phố Hải Phòng",
            "Cần Thơ": "Thành phố Cần Thơ",
        }
        # Add v2 provinces as identity mappings
        for p in self.v2:
            self.prov_aliases[p["name"]] = p["name"]
        # Province mapping (old merged -> new)
        pm_path = self.ref_dir / "province_mapping.json"
        if pm_path.exists():
            with open(pm_path, encoding="utf-8") as f:
                for code, m in json.load(f).items():
                    self.prov_aliases[m["old_name"]] = m["new_name"]

        log.info(f"AddressMapper loaded: {len(self.ward_map)} ward mappings, {len(self.prov_aliases)} province aliases")

    def normalize(self, old_province: str, old_district: str, old_ward: str, street: str = None):
        """Map old address → new 2-tier system (province + ward, no district)."""
        # Normalize province
        v1_prov = self.prov_aliases.get(old_province, old_province)
        new_province = v1_prov

        # Normalize ward using full key
        key = (v1_prov, old_district, old_ward)
        ward_info = self.ward_map.get(key)

        if ward_info:
            new_ward = ward_info["name"]
            new_province = ward_info["province"]
            ward_changed = old_ward != new_ward
        else:
            new_ward = old_ward
            ward_changed = False

        province_changed = old_province and new_province != old_province

        # Build full address: NO DISTRICT
        parts = [p for p in [street, new_ward, new_province] if p]

        return {
            "province": new_province,
            "ward": new_ward,
            "street": street,
            "full_address": ", ".join(parts),
            "district_legacy": old_district if old_district else None,
            "province_old": old_province if province_changed else None,
            "ward_old": old_ward if ward_changed else None,
        }


# ============================================================
# SOURCE ADAPTERS — Raw JSON → PropertyDTO
# ============================================================

# --- Mapping tables ---
# property_type slugs đồng bộ với property_types.sql (13 loại hình, cho cả sale + rent)
# Verified từ gateway.chotot.com/v1/public/ad-listing (category + sub-field).

NHATOT_CAT_MAP = {
    1010: "can-ho-chung-cu",   # fallback nếu thiếu apartment_type
    1020: "nha-o",             # fallback nếu thiếu house_type
    1030: "loai-bds-khac",     # Văn phòng / mặt bằng kinh doanh
    1040: "loai-bds-khac",     # Đất (fallback nếu thiếu land_type)
    1050: "loai-bds-khac",     # Phòng trọ (backend chưa có slug riêng)
}

NHATOT_APARTMENT_MAP = {       # cat=1010, field: apartment_type
    1: "can-ho-chung-cu",                 # Chung cư thường
    2: "chung-cu-mini-can-ho-dich-vu",    # Studio / mini / officetel
    3: "can-ho-chung-cu",                 # Duplex → gộp
    4: "penhouse",                        # Penthouse (chú ý slug backend đánh vần thiếu 't')
    5: "can-ho-chung-cu",                 # Tập thể, cư xá → gộp
}

NHATOT_HOUSE_MAP = {           # cat=1020, field: house_type
    1: "nha-o",                # Nhà mặt tiền
    2: "nha-o",                # Nhà hẻm
    3: "nha-biet-thu-doc-lap", # Biệt thự (override 'liền kề' qua title)
    4: "nha-o",                # Nhà phố
}

NHATOT_COMMERCIAL_MAP = {      # cat=1030, field: commercial_type
    1: "shophouse",            # Shophouse
    2: "kho-nha-xuong",        # Kho xưởng
    3: "loai-bds-khac",        # Văn phòng (backend chưa có slug riêng)
    4: "loai-bds-khac",        # Mặt bằng kinh doanh / kiot
}

NHATOT_LAND_MAP = {            # cat=1040, field: land_type
    1: "dat-tho-cu",           # Đất thổ cư
    2: "dat-nen-du-an",        # Đất nền dự án
    3: "loai-bds-khac",        # Đất thương mại / kho bãi
    4: "dat-nong-nghiep",      # Đất nông nghiệp
}

# Override theo title — ưu tiên cao hơn category mapping
FARMSTAY_RE = re.compile(r'\b(farmstay|homestay|nghỉ dưỡng|resort|trang trại)\b', re.IGNORECASE)
LIEN_KE_RE  = re.compile(r'\b(liền kề|liên kế|liền\s+k[ềê])\b', re.IGNORECASE)

# direction / legal_document are stored as Vietnamese display strings (a canonical
# casing/spelling), not slugs — same vocabulary across nhatot + muaban.
DIRECTION_MAP = {1: "Đông", 2: "Tây", 3: "Nam", 4: "Bắc", 5: "Đông Bắc", 6: "Đông Nam", 7: "Tây Bắc", 8: "Tây Nam"}
# 1-5 from chotot's property_legal_document codes; 6 = "Giấy tờ hợp lệ" (nhatot code unverified —
# muaban exposes this value as free text and it's the canonical form there).
LEGAL_MAP = {1: "Sổ hồng", 2: "Hợp đồng mua bán", 3: "Giấy phép xây dựng", 4: "Đang chờ sổ", 5: "Khác", 6: "Giấy tờ hợp lệ"}

# muaban detail pages expose direction / legal as free Vietnamese text — normalize the
# casing/spelling to the canonical form. Keys are accent-stripped lowercase (see _vn_norm).
MUABAN_DIRECTION_MAP = {
    "dong": "Đông", "tay": "Tây", "nam": "Nam", "bac": "Bắc",
    "dong bac": "Đông Bắc", "dong nam": "Đông Nam",
    "tay bac": "Tây Bắc", "tay nam": "Tây Nam",
}
MUABAN_LEGAL_MAP = {
    "so hong": "Sổ hồng", "so do": "Sổ hồng",          # sổ đỏ gộp vào sổ hồng
    "so hong rieng": "Sổ hồng", "so do rieng": "Sổ hồng",
    "hop dong": "Hợp đồng mua bán", "hop dong mua ban": "Hợp đồng mua bán",
    "giay phep xay dung": "Giấy phép xây dựng",
    "dang cho so": "Đang chờ sổ", "cho so": "Đang chờ sổ",
    "giay to hop le": "Giấy tờ hợp lệ",
}

def _vn_norm(text: str) -> str:
    """Lowercase, strip, drop Vietnamese accents — for keying the maps above."""
    if not text:
        return ""
    t = unicodedata.normalize("NFD", text.strip().lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")  # drop combining marks
    return t.replace("đ", "d")


def _clean_dimension(value) -> Optional[float]:
    """nhatot exposes user-entered `width`/`length` (m) — coerce to float, drop junk
    (<=0, or absurdly large values from people typing area into the wrong field)."""
    if value is None:
        return None
    try:
        f = round(float(value), 2)
    except (TypeError, ValueError):
        return None
    return f if 0 < f <= 1000 else None


def _parse_int_field(value) -> Optional[int]:
    """Coerce a possibly-stringy field (e.g. floors="2", "34/35") to int; junk → None."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    m = re.search(r"\d+", str(value))
    return int(m.group()) if m else None


def _normalize_vn_direction(text: Optional[str]) -> Optional[str]:
    """muaban direction text → canonical Vietnamese label ("Đông Nam"); unknown → None."""
    return MUABAN_DIRECTION_MAP.get(_vn_norm(text)) if text else None


def _normalize_muaban_legal(text: Optional[str]) -> Optional[str]:
    """Known forms → canonical Vietnamese label (matching LEGAL_MAP values); anything
    else passes through verbatim (stripped) so values like "Vi bằng" aren't lost to "Khác"."""
    if not text:
        return None
    t = _vn_norm(text)
    if t in MUABAN_LEGAL_MAP:
        return MUABAN_LEGAL_MAP[t]
    if "so hong" in t or "so do" in t or "shr" in t:
        return "Sổ hồng"
    if "hop dong" in t:
        return "Hợp đồng mua bán"
    if "giay phep" in t:
        return "Giấy phép xây dựng"
    if "cho so" in t:
        return "Đang chờ sổ"
    if "giay to hop le" in t:
        return "Giấy tờ hợp lệ"
    return text.strip()

MUABAN_CAT_MAP = {
    "Nhà hẻm ngõ": "nha-o",
    "Nhà mặt tiền": "nha-o",
    "Đất thổ cư": "dat-tho-cu",
    "Đất nông nghiệp": "dat-nong-nghiep",
    "Đất dự án": "dat-nen-du-an",
    "Đất trống": "dat-tho-cu",
    "Chung cư": "can-ho-chung-cu",
    "Penthouse": "penhouse",
    "Officetel": "chung-cu-mini-can-ho-dich-vu",
    "Căn hộ dịch vụ, mini": "chung-cu-mini-can-ho-dich-vu",
    "Tập thể, cư xá": "can-ho-chung-cu",
    "Biệt thự": "nha-biet-thu-doc-lap",
    "Cửa hàng, shophouse": "shophouse",
    "Shophouse": "shophouse",
    "Nhà trọ, phòng trọ": "loai-bds-khac",
    "Mặt bằng kinh doanh": "loai-bds-khac",
    "Văn phòng": "loai-bds-khac",
    "Nhà xưởng, nhà kho": "kho-nha-xuong",
}

MUABAN_CITY_MAP = {
    30: "Tp Hồ Chí Minh", 24: "Hà Nội", 15: "Đà Nẵng",
    28: "Bình Dương", 42: "Đồng Nai", 37: "Cần Thơ",
    39: "Khánh Hòa", 29: "Bà Rịa - Vũng Tàu",
}


def clean_title(t: str) -> str:
    if not t: return ""
    t = re.sub(r'[^\w\sÀ-ỹ,.()/\-!?:;\'\"₫đĐ]', '', t, flags=re.UNICODE)
    t = re.sub(r'\s+', ' ', t).strip()
    if t == t.upper() and len(t) > 15:
        t = t.title()
    return t


def extract_area_from_attrs(attrs: list) -> Optional[float]:
    """Extract area from muaban attributes list."""
    for attr in attrs:
        val = attr.get("value", "")
        m = re.match(r'([\d.,]+)\s*m[²2]', val, re.IGNORECASE)
        if m:
            num_str = m.group(1)
            # Handle "1.723.5" → "1723.5", "120,5" → "120.5"
            parts = num_str.split(".")
            if len(parts) > 2:  # "1.723.5" → thousand separator
                num_str = "".join(parts[:-1]) + "." + parts[-1]
            num_str = num_str.replace(",", ".")
            try:
                return float(num_str)
            except ValueError:
                continue
    return None


def extract_rooms_from_attrs(attrs: list, keyword: str) -> Optional[int]:
    """Extract bedrooms/bathrooms from muaban attributes."""
    for attr in attrs:
        val = attr.get("value", "")
        m = re.match(r'(\d+)\s*' + keyword, val, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None


def parse_muaban_location(location_str: str, locations_json: str = None):
    """Parse muaban location into district + ward."""
    district = None
    ward = None
    if location_str:
        parts = [p.strip() for p in location_str.split(",")]
        if len(parts) >= 2:
            ward = parts[0]
            district = parts[1]
        elif len(parts) == 1:
            district = parts[0]
    return ward, district


def classify_nhatot(raw: dict) -> str:
    """Map (category, sub_type) của nhatot → slug thuộc property_types.sql.

    Ưu tiên: title override (farmstay/resort) → category + sub-field →
    fallback NHATOT_CAT_MAP → 'loai-bds-khac'.
    """
    title = raw.get("subject") or ""
    if FARMSTAY_RE.search(title):
        return "trang-trai-khu-nghi-duong"

    cat = raw.get("category", 0)
    if cat == 1010:
        return NHATOT_APARTMENT_MAP.get(raw.get("apartment_type"), "can-ho-chung-cu")
    if cat == 1020:
        ht = raw.get("house_type")
        if ht == 3 and LIEN_KE_RE.search(title):
            return "nha-biet-thu-lien-ke"
        return NHATOT_HOUSE_MAP.get(ht, "nha-o")
    if cat == 1030:
        return NHATOT_COMMERCIAL_MAP.get(raw.get("commercial_type"), "loai-bds-khac")
    if cat == 1040:
        return NHATOT_LAND_MAP.get(raw.get("land_type"), "loai-bds-khac")
    return NHATOT_CAT_MAP.get(cat, "loai-bds-khac")


def adapt_nhatot(raw: dict) -> PropertyDTO:
    """nhatot raw → PropertyDTO"""
    prop_type = classify_nhatot(raw)

    # Transaction type
    tx = "cho-thue" if raw.get("type") in ("k", "u") else "ban"

    # Posted timestamp
    lt = raw.get("list_time")
    posted = datetime.fromtimestamp(lt / 1000).isoformat() if lt else None

    # Images
    imgs = [i for i in raw.get("images", []) if isinstance(i, str) and i.startswith("http")]

    # Phone: full phone injected by scraper via _phone_full (Playwright reveal)
    phone_full = raw.get("_phone_full")
    if phone_full:
        phone_full = re.sub(r'[\s.\-]', '', phone_full)

    # Build source URL from list_id — prefix theo loại giao dịch để khớp section thật
    list_id = raw.get("list_id", "")
    region_slug = raw.get("region_name", "").lower().replace(" ", "-")
    section = "cho-thue" if tx == "cho-thue" else "mua-ban"
    source_url = f"https://www.nhatot.com/{section}-{region_slug}/{list_id}.htm" if list_id else None

    return PropertyDTO(
        source="nhatot",
        source_id=str(raw.get("ad_id", raw.get("list_id", ""))),
        source_url=source_url,
        title=clean_title(raw.get("subject", "")),
        description=(raw.get("body") or "")[:500],
        property_type=prop_type,
        transaction_type=tx,
        price=raw.get("price"),
        price_display=raw.get("price_string", ""),
        price_per_m2=round(raw["price"] / raw["size"]) if raw.get("price") and raw.get("size") and raw["size"] > 0 else None,
        area=raw.get("size"),
        width=_clean_dimension(raw.get("width")),
        length=_clean_dimension(raw.get("length")),
        province=raw.get("region_name", ""),
        ward=raw.get("ward_name", ""),
        street=raw.get("street_name", ""),
        district_legacy=raw.get("area_name", ""),
        lat=raw.get("latitude"),
        lng=raw.get("longitude"),
        bedrooms=raw.get("rooms"),
        bathrooms=raw.get("toilets"),
        floors=raw.get("floors"),
        direction=DIRECTION_MAP.get(raw.get("direction")),
        legal_document=LEGAL_MAP.get(raw.get("property_legal_document")),
        phone_full=phone_full,
        contact_name=(raw.get("account_name") or "").strip(),
        images=imgs,
        image_count=raw.get("number_of_images", 0),
        posted_at=posted,
        scraped_at=datetime.now().isoformat(),
    )


def classify_muaban(raw: dict) -> str:
    """Map muaban category_name → slug thuộc property_types.sql.

    Override title farmstay/resort; override biệt thự liền kề nếu title khớp.
    """
    title = raw.get("title") or ""
    if FARMSTAY_RE.search(title):
        return "trang-trai-khu-nghi-duong"

    cat_name = raw.get("category_name", "")
    slug = MUABAN_CAT_MAP.get(cat_name, "loai-bds-khac")

    if slug == "nha-biet-thu-doc-lap" and LIEN_KE_RE.search(title):
        return "nha-biet-thu-lien-ke"
    return slug


def adapt_muaban(raw: dict) -> PropertyDTO:
    """muaban raw → PropertyDTO"""
    cat_name = raw.get("category_name", "")
    prop_type = classify_muaban(raw)

    # Transaction: detect from category_name or price pattern
    tx = "cho-thue" if any(kw in cat_name.lower() for kw in ["thuê", "trọ"]) or (raw.get("price", 0) < 100_000_000 and raw.get("price", 0) > 0) else "ban"

    # Area from attributes
    attrs = raw.get("attributes", [])
    area = extract_area_from_attrs(attrs)
    bedrooms = extract_rooms_from_attrs(attrs, "PN")
    bathrooms = extract_rooms_from_attrs(attrs, "WC")

    # Location
    ward_raw, district_raw = parse_muaban_location(raw.get("location", ""))

    # Province from city_id
    city_id = raw.get("city_id", 0)
    province_raw = MUABAN_CITY_MAP.get(city_id, "")

    # Price per m2
    price = raw.get("price")
    ppm2 = round(price / area) if price and area and area > 0 else None

    # Images: prefer the enriched full-res gallery (`images`, added by the
    # scraper detail pass); `covers` from the listing API holds only 1 thumbnail.
    imgs = raw.get("images") or raw.get("covers") or []
    if isinstance(imgs, list):
        imgs = [i for i in imgs if isinstance(i, str) and i.startswith("http")]
    else:
        imgs = []

    # Phone — listing API gives `phone`; detail pass may add `phone_full` as fallback
    phone_full = raw.get("phone") or raw.get("phone_full")
    if phone_full:
        phone_full = re.sub(r'[\s.\-]', '', phone_full)

    # Enriched detail-pass fields (added by muaban_scraper.enrich_detail_data).
    # Absent on un-enriched items → stay None.
    street = (raw.get("street") or "").strip() or None
    contact_name = (raw.get("contact_name") or "").strip() or None

    return PropertyDTO(
        source="muaban",
        source_id=str(raw.get("id", "")),
        source_url="https://muaban.net" + raw.get("url", ""),
        title=clean_title(raw.get("title", "")),
        description=(raw.get("summary") or "")[:500],
        property_type=prop_type,
        transaction_type=tx,
        price=price,
        price_display=raw.get("price_display", ""),
        price_per_m2=ppm2,
        area=area,
        province=province_raw,
        ward=ward_raw,
        street=street,
        district_legacy=district_raw,
        # muaban exposes no coordinates anywhere → leave lat/lng NULL
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        floors=_parse_int_field(raw.get("floors")),
        direction=_normalize_vn_direction(raw.get("direction")),
        legal_document=_normalize_muaban_legal(raw.get("legal_document")),
        phone_full=phone_full,
        contact_name=contact_name,
        images=imgs,
        image_count=raw.get("image_count") or len(imgs) or raw.get("total_images", 0),
        posted_at=raw.get("publish_at"),
        scraped_at=datetime.now().isoformat(),
    )


# ============================================================
# VALIDATION
# ============================================================

def validate_price(dto: PropertyDTO):
    """Filter out obviously bad price data."""
    if dto.price and dto.price > 500_000_000_000:
        dto.price = None  # > 500 ty = likely error
    if dto.price and dto.price < 0:
        dto.price = None


def classify_poster(dto: PropertyDTO, raw: dict):
    """Detect broker vs owner."""
    # nhatot: API trả `company_ad` cho tin do công ty/môi giới đăng.
    # Có trường này (truthy) ⇒ môi giới; không có ⇒ cá nhân.
    if dto.source == "nhatot":
        dto.poster_type = "moi_gioi" if raw.get("company_ad") else "chu_nha"
        return

    # muaban (và các source khác): heuristic scoring — giữ logic hiện tại
    score = 0

    sold = raw.get("sold_ads", 0)
    company = raw.get("company_ad", False) or raw.get("is_company", False)

    if sold >= 50: score += 5
    elif sold >= 20: score += 3
    elif sold >= 5: score += 2
    elif sold == 0: score -= 1

    if company: score += 2
    else: score -= 2

    # Name keywords
    name = (dto.contact_name or "").lower()
    if any(kw in name for kw in ["bds", "bất động sản", "land", "nhà đất", "chuyên", "địa ốc"]):
        score += 3

    # Description keywords
    desc = (dto.description or "").lower()
    if any(kw in desc for kw in ["hoa hồng", "hotline", "chuyên bán", "cam kết"]):
        score += 2
    if any(kw in desc for kw in ["chính chủ", "bán gấp", "không trung gian"]):
        score -= 2

    if score >= 4: dto.poster_type = "moi_gioi"
    elif score <= -2: dto.poster_type = "chu_nha"
    else: dto.poster_type = "khong_xac_dinh"


# ============================================================
# FULL PIPELINE
# ============================================================

def process_batch(raw_items: list, source: str, address_mapper: AddressMapper = None) -> list:
    """Process a batch of raw items into clean PropertyDTOs."""
    results = []

    for raw in raw_items:
        # Step 1: Adapt to DTO
        if source == "nhatot":
            dto = adapt_nhatot(raw)
        elif source == "muaban":
            dto = adapt_muaban(raw)
        else:
            continue

        # Step 2: Map address to new 34-province system
        if address_mapper:
            addr = address_mapper.normalize(
                old_province=dto.province or "",
                old_district=dto.district_legacy or "",
                old_ward=dto.ward or "",
                street=dto.street,
            )
            dto.province = addr["province"]
            dto.ward = addr["ward"]
            dto.street = addr["street"]
            dto.full_address = addr["full_address"]
            dto.district_legacy = addr["district_legacy"]

        # Step 3: Validate price
        validate_price(dto)

        # Step 4: Classify poster
        classify_poster(dto, raw)

        results.append(dto)

    return results


def run_pipeline(source: str, input_file: str, output_file: str = None):
    """Run full pipeline on a JSON file."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    log.info(f"Loading {input_file}...")
    with open(input_file, encoding="utf-8") as f:
        data = json.load(f)
    items = data.get("items", data.get("ads", []))
    log.info(f"  Raw items: {len(items)}")

    log.info("Loading address mapper...")
    mapper = AddressMapper()

    log.info("Processing...")
    results = process_batch(items, source, mapper)

    # Stats
    total = len(results)
    has_phone = sum(1 for r in results if r.phone_full)

    log.info(f"\n{'='*60}")
    log.info(f"  PIPELINE RESULTS — {source}")
    log.info(f"{'='*60}")
    log.info(f"  Total:        {total}")
    log.info(f"  Full phone:   {has_phone} ({has_phone/max(total,1)*100:.0f}%)")

    # Save
    if not output_file:
        import sys as _sys
        _cfg = str(Path(__file__).resolve().parent.parent)
        if _cfg not in _sys.path: _sys.path.insert(0, _cfg)
        from config import final_path
        output_file = final_path(source)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({
            "source": source,
            "total": total,
            "full_phone": has_phone,
            "processed_at": datetime.now().isoformat(),
            "listings": [asdict(r) for r in results],
        }, f, ensure_ascii=False, indent=2)

    log.info(f"  Saved: {output_file}")
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=["nhatot", "muaban"])
    parser.add_argument("--input", required=True, help="Raw JSON file")
    parser.add_argument("--output", default=None, help="Output JSON file")
    args = parser.parse_args()

    run_pipeline(args.source, args.input, args.output)
