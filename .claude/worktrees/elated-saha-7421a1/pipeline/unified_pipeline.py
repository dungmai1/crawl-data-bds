"""
V-Nexus Unified Data Pipeline
Chuẩn hóa data từ TẤT CẢ sources (nhatot, muaban, facebook) vào 1 DTO duy nhất.

Pipeline:
  Raw JSON → Source Adapter → Unified DTO → Address Mapping (34 tỉnh) →
  Price Validation → Property Classification → Broker Detection →
  Quality Score → Clean JSON / DB Insert

Output DTO fields: 35 fields chuẩn, format thống nhất
"""

import json
import re
import os
import logging
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional, List
from pathlib import Path

log = logging.getLogger("pipeline")

# ============================================================
# UNIFIED DTO — 35 fields chuẩn cho mọi source
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

    # Classification
    property_type: Optional[str] = None  # nha-hem, nha-mat-tien, chung-cu, dat-nen, biet-thu, phong-tro, van-phong, khac
    transaction_type: str = "ban"        # ban | cho-thue | sang-nhuong

    # Price
    price: Optional[int] = None          # VND
    price_display: Optional[str] = None  # "9,5 tỷ"
    price_per_m2: Optional[int] = None   # VND/m2
    price_unit: str = "VND"

    # Area
    area: Optional[float] = None         # m2

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
    direction: Optional[str] = None      # dong, tay, nam, bac, dong-nam...
    legal_document: Optional[str] = None # so-hong, hop-dong, giay-phep-xd, khac

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
    quality_score: int = 0               # 0-100


# ============================================================
# ADDRESS MAPPING (34 provinces, post 01/07/2025)
# ============================================================

class AddressMapper:
    def __init__(self, ref_dir: str = "scraper/pipeline/reference"):
        self.ref_dir = Path(ref_dir)
        self._load_mappings()

    def _load_mappings(self):
        # Province mapping: old name -> new name
        with open(self.ref_dir / "v1_provinces.json") as f:
            self.v1 = json.load(f)
        with open(self.ref_dir / "v2_provinces.json") as f:
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
            with open(pm_path) as f:
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
NHATOT_CAT_MAP = {1010: "chung-cu", 1020: "nha-o", 1030: "dat", 1040: "dat-nen", 1050: "van-phong", 1060: "phong-tro"}
NHATOT_HOUSE_MAP = {1: "nha-mat-tien", 2: "nha-hem", 3: "biet-thu", 4: "nha-pho"}
DIRECTION_MAP = {1: "dong", 2: "tay", 3: "nam", 4: "bac", 5: "dong-bac", 6: "dong-nam", 7: "tay-bac", 8: "tay-nam"}
LEGAL_MAP = {1: "so-hong", 2: "hop-dong", 3: "giay-phep-xd", 4: "dang-cho-so", 5: "khac"}

MUABAN_CAT_MAP = {
    "Nhà hẻm ngõ": "nha-hem", "Nhà mặt tiền": "nha-mat-tien",
    "Đất thổ cư": "dat-nen", "Đất nông nghiệp": "dat-nen", "Đất dự án": "dat-nen", "Đất trống": "dat-nen",
    "Chung cư": "chung-cu", "Penthouse": "chung-cu", "Officetel": "chung-cu",
    "Căn hộ dịch vụ, mini": "chung-cu", "Tập thể, cư xá": "chung-cu",
    "Biệt thự": "biet-thu", "Cửa hàng, shophouse": "nha-mat-tien", "Shophouse": "nha-mat-tien",
    "Nhà trọ, phòng trọ": "phong-tro",
    "Mặt bằng kinh doanh": "van-phong", "Văn phòng": "van-phong",
    "Nhà xưởng, nhà kho": "van-phong",
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


def adapt_nhatot(raw: dict) -> PropertyDTO:
    """nhatot raw → PropertyDTO"""
    cat = raw.get("category", 0)
    ht = raw.get("house_type")
    prop_type = NHATOT_HOUSE_MAP.get(ht, NHATOT_CAT_MAP.get(cat, "khac")) if cat == 1020 and ht else NHATOT_CAT_MAP.get(cat, "khac")

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

    # Build source URL from list_id
    list_id = raw.get("list_id", "")
    region_slug = raw.get("region_name", "").lower().replace(" ", "-")
    source_url = f"https://www.nhatot.com/mua-ban-{region_slug}/{list_id}.htm" if list_id else None

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


def adapt_muaban(raw: dict) -> PropertyDTO:
    """muaban raw → PropertyDTO"""
    cat_name = raw.get("category_name", "")
    prop_type = MUABAN_CAT_MAP.get(cat_name, "khac")

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

    # Images
    imgs = raw.get("covers", [])
    if isinstance(imgs, list):
        imgs = [i for i in imgs if isinstance(i, str) and i.startswith("http")]

    # Phone
    phone_full = raw.get("phone")
    phone_display = raw.get("phone_display")
    # Normalize phone
    if phone_full:
        phone_full = re.sub(r'[\s.\-]', '', phone_full)

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
        district_legacy=district_raw,
        lat=None,  # muaban doesn't provide lat/lng in listing API
        lng=None,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        direction=None,  # muaban doesn't provide
        legal_document=None,  # muaban doesn't provide
        phone_full=phone_full,
        contact_name=None,
        images=imgs,
        image_count=raw.get("total_images", 0),
        posted_at=raw.get("publish_at"),
        scraped_at=datetime.now().isoformat(),
    )


# ============================================================
# VALIDATION + QUALITY SCORE
# ============================================================

def validate_price(dto: PropertyDTO):
    """Filter out obviously bad price data."""
    if dto.price and dto.price > 500_000_000_000:
        dto.price = None  # > 500 ty = likely error
    if dto.price and dto.price < 0:
        dto.price = None


def classify_poster(dto: PropertyDTO, raw: dict):
    """Detect broker vs owner."""
    score = 0

    # nhatot-specific signals
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


def calc_quality_score(dto: PropertyDTO) -> int:
    """Calculate 0-100 quality score."""
    s = 0
    checks = [
        (dto.title, 10), (dto.price, 15), (dto.area, 15),
        (dto.province, 10), (dto.ward, 5), (dto.street, 5),
        (dto.lat and dto.lng, 8),
        (dto.images and len(dto.images) > 0, 5),
        (dto.description and len(dto.description) > 50, 5),
        (dto.bedrooms, 3), (dto.bathrooms, 3),
        (dto.legal_document, 3), (dto.direction, 2),
        (dto.phone_full, 10),
    ]
    for val, pts in checks:
        if val: s += pts
    return min(s, 100)


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

        # Step 5: Quality score
        dto.quality_score = calc_quality_score(dto)

        results.append(dto)

    # Sort by quality
    results.sort(key=lambda x: x.quality_score, reverse=True)

    return results


def run_pipeline(source: str, input_file: str, output_file: str = None):
    """Run full pipeline on a JSON file."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    log.info(f"Loading {input_file}...")
    with open(input_file) as f:
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
    avg_quality = sum(r.quality_score for r in results) / max(total, 1)

    log.info(f"\n{'='*60}")
    log.info(f"  PIPELINE RESULTS — {source}")
    log.info(f"{'='*60}")
    log.info(f"  Total:        {total}")
    log.info(f"  Full phone:   {has_phone} ({has_phone/max(total,1)*100:.0f}%)")
    log.info(f"  Avg quality:  {avg_quality:.0f}/100")

    # Quality distribution
    excellent = sum(1 for r in results if r.quality_score >= 80)
    good = sum(1 for r in results if 60 <= r.quality_score < 80)
    fair = sum(1 for r in results if 40 <= r.quality_score < 60)
    poor = sum(1 for r in results if r.quality_score < 40)
    log.info(f"  Quality: Excellent={excellent} Good={good} Fair={fair} Poor={poor}")

    # Save
    if not output_file:
        import sys as _sys
        _cfg = str(Path(__file__).resolve().parent.parent)
        if _cfg not in _sys.path: _sys.path.insert(0, _cfg)
        from config import clean_path
        output_file = clean_path(source)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump({
            "source": source,
            "total": total,
            "full_phone": has_phone,
            "avg_quality": round(avg_quality, 1),
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
