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
from collections import Counter, defaultdict
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, List
from pathlib import Path

log = logging.getLogger("pipeline")

# Phone-history lookup — optional, falls back gracefully if DB not configured
try:
    from phone_history import get_phone_history_counts, normalize_phone as _normalize_phone_db
except ImportError:  # phone_history.py sits in same package; supports both run modes
    try:
        from .phone_history import get_phone_history_counts, normalize_phone as _normalize_phone_db  # type: ignore
    except Exception:
        get_phone_history_counts = None  # type: ignore
        _normalize_phone_db = None  # type: ignore

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

    # Poster classification — các field dưới CHỈ phục vụ JSON output (luồng run.py).
    # Luồng DB (scheduler/) KHÔNG đọc chúng: cột data_sources.poster_type được
    # classify riêng ở classification.poster sau khi upsert.
    poster_type: Optional[str] = None    # moi_gioi | chu_nha

    # Poster classification (new — BROKER/OWNER + confidence + reasons)
    owner_type: Optional[str] = None     # BROKER | OWNER
    confidence_score: float = 0.0        # ∈ [0,1] về nhãn đã chọn
    reason: List[str] = field(default_factory=list)  # giải thích Vietnamese

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


# ============================================================
# CLASSIFIER — weighted scoring, BROKER/OWNER + confidence + reasons
# ============================================================

# Phone-history thresholds (CÙNG SOURCE — không gộp cross-source)
PHONE_THRESHOLD = 3        # > 3 lần cùng source → strong broker signal
PHONE_WEAK_THRESHOLD = 1   # > 1 → weak hint

# Signal weights
W_PHONE_STRONG = 0.50
W_PHONE_WEAK = 0.15
W_COMPANY_FLAG = 0.45      # nhatot.company_ad / muaban.is_company
W_NAME_KW = 0.30
W_DESC_BROKER_KW = 0.20
W_SOLD_HIGH = 0.30         # muaban sold_ads >= 20
W_SOLD_MID = 0.15          # muaban sold_ads >= 5
W_OWNER_KW = -0.25         # "chính chủ", "không trung gian" trong title/desc
W_OWNER_NO_HISTORY = -0.15 # phone chưa từng xuất hiện cùng source

BROKER_THRESHOLD = 0.40

_NAME_BROKER_KW = ["bds", "bất động sản", "land", "nhà đất", "chuyên", "địa ốc",
                   "môi giới", "broker", "sale", "sales", "công ty"]
_DESC_BROKER_KW = ["hoa hồng", "hotline", "chuyên bán", "cam kết", "nhận ký gửi",
                   "hỗ trợ vay", "tư vấn miễn phí", "quỹ căn", "giỏ hàng"]
_OWNER_KW = ["chính chủ", "không trung gian", "không qua môi giới"]


def classify_poster(
    dto: PropertyDTO,
    raw: dict,
    db_phone_history: Optional[dict] = None,
    in_cycle_phone_count: int = 0,
):
    """Phân loại tin đăng là môi giới hay chủ nhà — weighted scoring.

    Set 4 field trên DTO:
      - owner_type:       "BROKER" | "OWNER"
      - confidence_score: ∈ [0,1] về nhãn đã chọn
      - reason:           list giải thích Vietnamese
      - poster_type:      "moi_gioi" | "chu_nha" — derived từ owner_type.
                          Chỉ dùng cho JSON output; luồng DB classify lại riêng
                          ở classification.poster (không đọc field này).

    Tham số:
      db_phone_history: optional dict {(source, phone): count} — preloaded
        từ `phone_history.get_phone_history_counts`. None = bỏ qua DB signal.
      in_cycle_phone_count: số tin KHÁC trong run hiện tại cùng (source, phone).
        Caller có trách nhiệm tính (build Counter trong process_batch).

    Lưu ý: việc check phone history CHỈ trong cùng source (`dto.source`) —
    không gộp giữa nhatot và muaban (theo yêu cầu phân loại).
    """
    score = 0.0
    reasons: list[str] = []

    # ===== S1: Phone history cùng source (DB + in-cycle) =====
    phone = (_normalize_phone_db(dto.phone_full) if _normalize_phone_db else dto.phone_full)
    if phone:
        db_count = 0
        if db_phone_history is not None:
            db_count = db_phone_history.get((dto.source, phone), 0)
        total = max(0, in_cycle_phone_count) + db_count

        if total > PHONE_THRESHOLD:
            score += W_PHONE_STRONG
            reasons.append(
                f"S1.phone đăng {total} tin trong source `{dto.source}` "
                f"(DB={db_count}, in-cycle khác={in_cycle_phone_count}) — > {PHONE_THRESHOLD}"
            )
        elif total > PHONE_WEAK_THRESHOLD:
            score += W_PHONE_WEAK
            reasons.append(
                f"S1w.phone xuất hiện {total} lần cùng source `{dto.source}` "
                f"(DB={db_count}, in-cycle khác={in_cycle_phone_count})"
            )
        elif total == 0:
            score += W_OWNER_NO_HISTORY
            reasons.append(
                f"O1.phone chưa có lịch sử trong source `{dto.source}` → thiên hướng chủ nhà"
            )

    # ===== S3: Explicit API broker flag =====
    is_company = bool(raw.get("company_ad") or raw.get("is_company"))
    if is_company:
        score += W_COMPANY_FLAG
        reasons.append("S3.cờ broker từ API (company_ad / is_company)")

    # ===== S2 (muaban-specific): sold_ads tier =====
    sold = raw.get("sold_ads", 0) or 0
    if sold >= 20:
        score += W_SOLD_HIGH
        reasons.append(f"S2.sold_ads={sold} (>=20)")
    elif sold >= 5:
        score += W_SOLD_MID
        reasons.append(f"S2w.sold_ads={sold} (>=5)")

    # ===== T1: contact_name keyword =====
    name = (dto.contact_name or "").lower()
    name_hits = [kw for kw in _NAME_BROKER_KW if kw in name]
    if name_hits:
        score += W_NAME_KW
        reasons.append(f"T1.contact_name chứa keyword môi giới: {name_hits[:3]}")

    # ===== T3: description/title broker keywords =====
    desc = (dto.description or "").lower()
    title_lower = (dto.title or "").lower()
    desc_hits = [kw for kw in _DESC_BROKER_KW if kw in desc or kw in title_lower]
    if desc_hits:
        score += W_DESC_BROKER_KW
        reasons.append(f"T3.description/title chứa keyword môi giới: {desc_hits[:3]}")

    # Owner counter-signal: "chính chủ" trong title/desc
    owner_hits = [kw for kw in _OWNER_KW if kw in desc or kw in title_lower]
    if owner_hits:
        score += W_OWNER_KW
        reasons.append(f"O2.title/description chứa keyword chủ nhà: {owner_hits[:3]}")

    # ===== FINAL CLASSIFICATION =====
    if score >= BROKER_THRESHOLD:
        owner_type = "BROKER"
        confidence = min(1.0, score)
        poster_legacy = "moi_gioi"
    else:
        owner_type = "OWNER"
        confidence = max(0.0, min(1.0, 1.0 - max(0.0, score) / BROKER_THRESHOLD * 0.5))
        poster_legacy = "chu_nha"

    if not reasons:
        reasons.append("Không có signal môi giới — phân loại mặc định chủ nhà")

    dto.owner_type = owner_type
    dto.confidence_score = round(confidence, 3)
    dto.reason = reasons
    dto.poster_type = poster_legacy


# ============================================================
# FULL PIPELINE
# ============================================================

def process_batch(
    raw_items: list,
    source: str,
    address_mapper: AddressMapper = None,
    use_db_phone_history: bool = True,
) -> list:
    """Process a batch of raw items into clean PropertyDTOs.

    Bước phân loại broker/owner dùng:
      1. In-cycle phone counter — đếm trong batch hiện tại, cùng source only.
      2. DB phone history (optional) — query `data_sources.phone_full` cùng source,
         loại trừ chính source_id đang xử lý để không tự đếm.
      3. Các signal khác: company_ad, sold_ads, contact_name/desc keywords.

    DB unreachable → bỏ qua signal lịch sử, vẫn classify bằng các signal khác.
    """
    # Pre-adapt tất cả để có (phone_full, source_id) trước khi fetch DB
    adapted: list[tuple[PropertyDTO, dict]] = []
    for raw in raw_items:
        if source == "nhatot":
            dto = adapt_nhatot(raw)
        elif source == "muaban":
            dto = adapt_muaban(raw)
        else:
            continue
        adapted.append((dto, raw))

    # Build in-cycle phone counter (cùng source duy nhất trong process_batch call này)
    phone_counter: Counter = Counter()
    for dto, _ in adapted:
        ph = _normalize_phone_db(dto.phone_full) if _normalize_phone_db else dto.phone_full
        if ph:
            phone_counter[ph] += 1

    # Fetch DB history — bulk, 1 query cho toàn batch
    db_phone_history: Optional[dict] = None
    if use_db_phone_history and get_phone_history_counts is not None:
        exclude_ids: set[str] = {str(dto.source_id) for dto, _ in adapted if dto.source_id}
        try:
            db_phone_history = get_phone_history_counts(
                [(source, ph) for ph in phone_counter],
                exclude_source_ids={source: exclude_ids} if exclude_ids else None,
            )
        except Exception as e:
            log.warning(f"DB phone-history fetch failed: {e} — fallback in-cycle only")
            db_phone_history = None

    results: list[PropertyDTO] = []
    for dto, raw in adapted:
        # Address normalize
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

        validate_price(dto)

        # In-cycle count cho riêng listing này = total cùng phone trừ chính nó
        ph = _normalize_phone_db(dto.phone_full) if _normalize_phone_db else dto.phone_full
        in_cycle_others = max(0, phone_counter.get(ph, 0) - 1) if ph else 0

        classify_poster(
            dto,
            raw,
            db_phone_history=db_phone_history,
            in_cycle_phone_count=in_cycle_others,
        )
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
