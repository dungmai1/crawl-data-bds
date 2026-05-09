# Nhatot Full Crawler — Design Spec

**Date:** 2026-05-09
**Status:** Draft
**Author:** Claude (brainstorming session)

## Goal

Tạo một scraper standalone cho Nhatot, chạy độc lập với `scrapers/nhatot_fast_scraper.py`, với 3 yêu cầu:

1. Cào **tất cả field** API trả về (list + detail), không reduce/normalize về `PropertyDTO`.
2. **Bắt buộc có số điện thoại** — listing không reveal được phone sẽ bị loại bỏ khỏi output.
3. Vẫn áp dụng **`pipeline/reference/`** (province/ward mapping) để chuẩn hóa địa chỉ — nhưng đính kèm dưới key riêng, không ghi đè field gốc.

## Non-Goals

- Không tích hợp với `unified_pipeline.process_batch` (không trả ra `PropertyDTO`).
- Không tích hợp với `merge_pipeline` (không cross-source dedup).
- Không dùng `data/phone_cache.json` — mỗi run reveal độc lập.
- Không sửa hoặc thay thế `nhatot_fast_scraper.py`.

## File Layout

| Item | Path |
|------|------|
| Source | `scrapers/nhatot_full_scraper.py` |
| Output | `data/raw/nhatot_full/{YYYY-MM-DD}/{HHMMSS}_raw.json` |
| Log | `logs/{YYYY-MM-DD}/{HHMMSS}_nhatot_full.log` |

Output tách thư mục `nhatot_full/` riêng để khỏi đụng pipeline / merge hiện tại đang đọc `data/raw/nhatot/`.

## Architecture — 3 Stages

### Stage 1: Listing API (httpx async)

- Endpoint: `https://gateway.chotot.com/v1/public/ad-listing?cg={category}&limit=50&o={offset}&st=s&region_v2={region}`
- Categories: `[1010, 1020, 1030, 1040]` (Chung cư, Nhà ở, VP/MB, Đất)
- Concurrency: 10 requests song song qua `asyncio.Semaphore`
- Round-robin distribution: với mỗi vòng lấy 1 ad từ mỗi category để phân bổ đều khi `--count` nhỏ
- Dedup theo `ad_id` ngay trong stage này

### Stage 2: Detail API (httpx async)

- Endpoint: `https://gateway.chotot.com/v1/public/ad-listing/{list_id}`
- Concurrency: 10 song song
- **Merge toàn bộ field từ detail response vào ad gốc** (detail có nhiều field hơn list view: `body`, `street_name`, `ward_name`, `property_legal_document`, ...). Strategy: shallow merge với detail thắng list khi trùng key.
- Ad nào detail fail → giữ nguyên data từ list (không loại bỏ ở stage này, để stage 3 xử lý theo phone).

### Stage 3: Phone Reveal (Playwright, N tabs song song)

- Reuse logic từ `nhatot_fast_scraper.py`:
  - Build URL map từ search page (Playwright thuần) để lấy correct listing URL.
  - Mỗi tab worker: goto detail page → check expired → click nút "Hiện số" → intercept `gateway.chotot.com/.../phone` → fallback đọc text từ button.
  - Anti-detection: `--disable-blink-features=AutomationControlled` + override `navigator.webdriver`.
- **Không dùng `phone_cache.json`** — mỗi run thử reveal lại cho mọi listing.
- **Filter cứng**: chỉ giữ listing có `phone` trong output cuối cùng.

## Address Normalization

- Load `AddressMapper` từ `pipeline.unified_pipeline` (đã có sẵn, đọc 4 file trong `pipeline/reference/`).
- Với mỗi ad có phone:
  ```python
  addr = mapper.normalize(
      old_province=ad.get("region_name"),
      old_district=ad.get("area_name"),
      old_ward=ad.get("ward_name"),
      street=ad.get("street_name"),
  )
  ad["address_normalized"] = addr
  ```
- AddressMapper.normalize trả về dict gồm: `province`, `ward`, `street`, `full_address`, `district_legacy`, `province_old`, `ward_old`. Đính kèm cả dict vào key `address_normalized` của ad — **không ghi đè** field gốc `region_name`/`area_name`/`ward_name`.

## Output Schema

```json
{
  "source": "nhatot",
  "scraped_at": "2026-05-09T14:23:11",
  "total": 320,
  "cycle_time_sec": 412,
  "listings": [
    {
      "ad_id": 123456789,
      "list_id": 987654321,
      "subject": "...",
      "body": "...",
      "price": 5500000000,
      "area": 75,
      "rooms": 3,
      "region_name": "Tp Hồ Chí Minh",
      "area_name": "Quận 1",
      "ward_name": "Phường Bến Nghé",
      "street_name": "Lê Lợi",
      "...": "tất cả field khác từ list + detail API",
      "phone": "0912345678",
      "address_normalized": {
        "province": "Thành phố Hồ Chí Minh",
        "ward": "Phường Bến Thành",
        "ward_old": "Phường Bến Nghé",
        "province_old": null,
        "street": "Lê Lợi",
        "full_address": "Lê Lợi, Phường Bến Thành, Thành phố Hồ Chí Minh",
        "district_legacy": "Quận 1"
      }
    }
  ]
}
```

Tất cả listing trong `listings` đều **có phone** (listing không phone bị loại trong Stage 3).

## CLI

```
python scrapers/nhatot_full_scraper.py [--count N] [--tabs N] [--region R] [--offset-shift N]
```

| Flag | Default | Mô tả |
|------|---------|-------|
| `--count` | 500 | Max listings / cycle |
| `--tabs` | 5 | Số Playwright tab chạy song song khi reveal phone |
| `--region` | 13000 | `13000`=HCM, `12000`=HN |
| `--offset-shift` | 0 | Cộng vào API offset để nhảy qua page đã cào |

Không có `--loop` cho version đầu (YAGNI). Có thể thêm sau nếu cần.

## Logging

- Mỗi stage log số ads input/output.
- Stage 3 log per-listing: `[T{tab_id}] ({idx}/{total}) {OK|NO_PHONE|EXPIRED|ERROR}`.
- Cuối cycle log summary: total, kept (có phone), dropped (no phone), elapsed.

## Error Handling

- API fail (httpx): log warning, return empty list cho category đó. Không abort cycle.
- Detail API fail: giữ ad từ list, không bổ sung detail field. Không abort.
- Phone reveal fail: ad bị loại khỏi output (đúng theo yêu cầu).
- AddressMapper fail load: log warning, scraper vẫn chạy nhưng `address_normalized` = null cho mọi ad.

## Open Questions

Không có. Tất cả decisions đã được duyệt qua brainstorming.
