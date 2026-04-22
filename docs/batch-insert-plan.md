# Batch Insert Plan — Scraper Data → PostgreSQL

**Ngày lập:** 2026-04-17  
**Phạm vi:** Insert 244 listings từ `data/sample/2026-04-14_merged.json` vào bảng `listings` trong PostgreSQL.

---

## 1. Phân tích hiện trạng

### 1.1 Dữ liệu nguồn (JSON)

| Field JSON | Kiểu | Ghi chú |
|---|---|---|
| `source` / `source_id` / `source_url` | str | Không có cột riêng trong DB |
| `title` / `description` | str | Map thẳng |
| `property_type` | slug scraper (vd: `nha-mat-tien`) | **Cần dịch** sang slug DB trước khi lookup FK |
| `transaction_type` | `ban` / `cho-thue` | **Cần dịch** sang `sale` / `rent` |
| `price` | int VND | → `price` BIGINT |
| `area` | float m² | → `area_m2` |
| `lat` / `lng` | float | → `latitude` / `longitude` |
| `full_address` / `province` / `ward` | str | Map thẳng |
| `legal_document` | str (vd: `so-hong`) | Map thẳng |
| `images` | list[str] | → `image_urls TEXT[]` |
| `posted_at` | ISO datetime | → `published_at` |
| `bedrooms`, `bathrooms`, `floors`, `direction` | mixed | → `attributes JSONB` |
| `phone_full`, `contact_name`, `poster_type` | str | → `attributes JSONB` |
| `price_per_m2`, `price_display`, `district_legacy`, `street` | mixed | → `attributes JSONB` |
| `quality_score`, `scraped_at` | mixed | → `attributes JSONB` |

### 1.2 Property types đã định hình (`property_types.sql`)

File `property_types.sql` đã có sẵn 22 hàng (12 sale + 10 rent). Quy ước:
- `listing_type`: `sale` hoặc `rent` (không phải `ban`/`cho-thue`)
- Slug rent dùng prefix `thue-` để đảm bảo `slug UNIQUE`
- Bảng này **chạy một lần** trước khi insert listings.

### 1.3 Vấn đề slug không khớp

Scraper dùng slug của portal BĐS, DB dùng slug chuẩn nội bộ. Cần bảng dịch:

| Scraper slug | transaction_type | → DB slug | listing_type |
|---|---|---|---|
| `chung-cu` | `ban` | `can-ho-chung-cu` | `sale` |
| `chung-cu` | `cho-thue` | `thue-can-ho-chung-cu` | `rent` |
| `biet-thu` | `ban` | `nha-biet-thu-lien-ke` | `sale` |
| `biet-thu` | `cho-thue` | `thue-nha-biet-thu-lien-ke` | `rent` |
| `dat-nen` | `ban` | `dat-nen-du-an` | `sale` |
| `dat-nen` | `cho-thue` | `thue-kho-nha-xuong-dat` | `rent` |
| `nha-hem` | `ban` | `nha-rieng` | `sale` |
| `nha-hem` | `cho-thue` | `thue-nha-rieng` | `rent` |
| `nha-mat-tien` | `ban` | `nha-mat-pho` | `sale` |
| `nha-mat-tien` | `cho-thue` | `thue-nha-mat-pho` | `rent` |
| `phong-tro` | `cho-thue` | `thue-nha-tro-phong-tro` | `rent` |
| `van-phong` | `ban` | `loai-bds-khac` | `sale` |
| `van-phong` | `cho-thue` | `thue-van-phong` | `rent` |
| `khac` | `ban` | `loai-bds-khac` | `sale` |

> **Lưu ý mapping mờ:**
> - `dat-nen × cho-thue` → không có slug thuê đất riêng trong DB, tạm dùng `thue-kho-nha-xuong-dat`.
> - `van-phong × ban` → không có loại văn phòng bán trong DB, fallback về `loai-bds-khac`.
> Nên xác nhận lại 2 case này với team trước khi chạy production.

### 1.4 Database schema

- `listings.user_id` — không có NOT NULL → để `NULL` cho scraped data (xác nhận với Spring Boot team).
- `listings.property_type_id` — FK tới `property_types`, cần seed trước.
- Không có cột `source_id` → lưu vào `attributes JSONB`, tạo unique index để idempotent.

---

## 2. Những việc cần làm

### Bước 0 — Chuẩn bị DB (chạy một lần)

**0a. Chạy `property_types.sql`** — seed 22 hàng vào `property_types`.

```bash
psql $DATABASE_URL -f property_types.sql
```

**0b. Tạo unique index chống duplicate scraped listings**

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_listings_source_id
  ON listings ((attributes->>'source_id'), (attributes->>'source'));
```

Cho phép re-run script nhiều lần mà không tạo bản ghi trùng.

---

### Bước 1 — Viết script `scripts/batch_insert.py`

**Input:** `data/sample/2026-04-14_merged.json` (hoặc bất kỳ merged JSON nào)  
**Output:** Insert vào PostgreSQL, in summary log.

**Logic chính:**

```
# Khởi động: load slug translation table vào dict
SLUG_MAP = { ("chung-cu", "ban"): "can-ho-chung-cu", ... }  # 14 entries

# Load property_type_id cache: slug → uuid từ DB
pt_cache = { row.slug: row.id for row in SELECT id, slug FROM property_types }

for each listing in JSON:
    1. Dịch slug:
       db_slug = SLUG_MAP.get((listing.property_type, listing.transaction_type))
       → nếu None: log warning, skip hoặc dùng 'loai-bds-khac'

    2. Lookup FK:
       property_type_id = pt_cache[db_slug]

    3. Build INSERT dict:
       id             = uuid4()
       user_id        = NULL
       property_type_id = property_type_id
       title          = listing.title
       description    = listing.description
       price          = listing.price
       area_m2        = listing.area
       full_address   = listing.full_address
       legal_document = listing.legal_document
       province       = listing.province
       ward           = listing.ward
       latitude       = listing.lat
       longitude      = listing.lng
       status         = 'active'
       image_urls     = listing.images        # list → TEXT[]
       published_at   = listing.posted_at     # ISO → TIMESTAMPTZ
       attributes     = {                     # JSONB
           source, source_id, source_url,
           bedrooms, bathrooms, floors, direction,
           price_per_m2, price_display, street, district_legacy,
           phone_full, contact_name, poster_type,
           quality_score, scraped_at
       }

    4. INSERT INTO listings (...) VALUES (...)
       ON CONFLICT ((attributes->>'source_id'), (attributes->>'source'))
       DO NOTHING
```

---

### Bước 2 — Mapping field chi tiết

| DB column | Nguồn JSON | Transform |
|---|---|---|
| `id` | — | `uuid4()` |
| `user_id` | — | `NULL` |
| `property_type_id` | `property_type` + `transaction_type` | SLUG_MAP → pt_cache lookup |
| `title` | `title` | Direct |
| `description` | `description` | Direct |
| `price` | `price` | int → BIGINT |
| `area_m2` | `area` | float |
| `full_address` | `full_address` | Direct |
| `legal_document` | `legal_document` | Direct |
| `province` | `province` | Direct |
| `ward` | `ward` | Direct |
| `latitude` | `lat` | float |
| `longitude` | `lng` | float |
| `status` | — | Hardcode `'active'` |
| `image_urls` | `images` | list[str] → TEXT[] |
| `published_at` | `posted_at` | ISO string → TIMESTAMPTZ |
| `attributes` | nhiều fields | JSONB (xem bên dưới) |

**`attributes` JSONB sẽ chứa:**
```json
{
  "source": "nhatot",
  "source_id": "175327293",
  "source_url": "https://www.nhatot.com/...",
  "bedrooms": 10,
  "bathrooms": 7,
  "floors": 7,
  "direction": "dong-nam",
  "price_per_m2": 93660714,
  "price_display": "10,49 tỷ",
  "street": "Đường Nguyễn Thị Sóc",
  "district_legacy": "Huyện Hóc Môn",
  "phone_full": "0985239435",
  "contact_name": "Nhà Đất Sài Gòn",
  "poster_type": "moi_gioi",
  "quality_score": 100,
  "scraped_at": "2026-04-14T16:51:12"
}
```

---

### Bước 3 — Xử lý edge cases

| Case | Xử lý |
|---|---|
| Slug scraper không có trong `SLUG_MAP` | Log warning, fallback `loai-bds-khac` |
| `price = null` | Insert NULL |
| `lat/lng = null` | Insert NULL |
| `images = []` hoặc thiếu | Insert `ARRAY[]::TEXT[]` |
| `posted_at = null` | Insert NULL vào `published_at` |
| Duplicate `source_id` cùng `source` | `ON CONFLICT DO NOTHING` |
| Re-run cùng file | Idempotent nhờ unique index |

---

### Bước 4 — Test & Verify

```bash
# Dry-run: parse + map nhưng không commit
python scripts/batch_insert.py --dry-run

# Chạy thật
python scripts/batch_insert.py

# Kiểm tra kết quả
psql $DATABASE_URL -c "
  SELECT attributes->>'source' AS source, COUNT(*)
  FROM listings
  GROUP BY 1;
"
```

---

## 3. Checklist thực hiện

- [ ] **0a** Chạy `property_types.sql` để seed bảng `property_types`
- [ ] **0b** Tạo `idx_listings_source_id` unique index trên `listings`
- [ ] **1** Xác nhận mapping mờ với team: `dat-nen × cho-thue`, `van-phong × ban`
- [ ] **2** Viết `scripts/batch_insert.py` với `SLUG_MAP` và logic mapping ở trên
- [ ] **3** Test dry-run với 244 listings trong sample
- [ ] **4** Chạy thật, verify count và sample records

---

## 4. Phụ thuộc & Lưu ý

- **Cần** `.env` với `DATABASE_URL` (PostgreSQL connection string)
- **Thư viện:** `psycopg2-binary` (kiểm tra `requirements.txt` đã có chưa)
- Nếu backend cần query theo `source`: thêm GIN index `CREATE INDEX ON listings USING GIN (attributes);`
- `user_id = NULL` có thể vi phạm business rule của backend — xác nhận trước khi insert production.
