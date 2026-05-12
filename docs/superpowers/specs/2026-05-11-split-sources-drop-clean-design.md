# Design — Tách 2 nguồn (nhatot, muaban) riêng, bỏ bước merge & bỏ `data/clean`

**Ngày:** 2026-05-11
**Trạng thái:** Đã duyệt design, chờ implementation plan

## Mục tiêu

Hiện pipeline có 3 bước: SCRAPE → PIPELINE (ghi `data/clean/{source}/...`) → MERGE (gộp 2 nguồn, dedup, ghi 1 file `data/final/{date}/{HHMMSS}_merged.json`).

Người dùng muốn:
- **Không merge** 2 nguồn nữa — giữ nhatot và muaban tách biệt.
- Chỉ còn **2 thư mục dữ liệu**: `data/raw/` và `data/final/`.
- Bỏ thư mục trung gian `data/clean/`.

## Luồng mới (2 bước)

```
STEP 1 — SCRAPE                              STEP 2 — PIPELINE (normalize + classify)
nhatot → data/raw/nhatot/{date}/{HHMMSS}_raw.json  →  data/final/nhatot/{date}/{HHMMSS}.json
muaban → data/raw/muaban/{date}/{HHMMSS}_raw.json  →  data/final/muaban/{date}/{HHMMSS}.json
```

- Mỗi nguồn đi qua `unified_pipeline.process_batch()` → `PropertyDTO` (35 field, đã có `quality_score`), ghi **thẳng** ra `data/final/{source}/{date}/{HHMMSS}.json`.
- Không còn bước MERGE, không còn cross-source dedup, không còn `data/clean/`.
- Format file final mỗi nguồn giữ như output hiện tại của `run_pipeline_for_source`, có thể bổ sung vài stat tóm tắt:
  ```json
  {
    "source": "nhatot",
    "total": 200,
    "phone_full": 120,
    "avg_quality": 67.4,
    "processed_at": "2026-05-11T14:41:18",
    "listings": [ /* PropertyDTO ... */ ]
  }
  ```
- Cấu trúc `data/final/{source}/{date}/{HHMMSS}.json` cố ý mirror `data/raw/{source}/{date}/{HHMMSS}_raw.json` để dễ đối chiếu raw↔final theo từng nguồn. Giữ convention "never overwrite — mỗi run 1 file riêng".

## Thay đổi theo file

### `config.py`
- `final_path(source, date=None)` — thêm tham số `source` (bắt buộc, không default), trả về `data/final/{source}/{date}/{HHMMSS}.json` (bỏ hậu tố `_merged`).
- `find_latest_final(source)` — thêm tham số `source`, tìm file mới nhất trong `data/final/{source}/`.
- Xoá `clean_path()` và `find_latest_clean()`.
- Cập nhật docstring đầu file (sơ đồ thư mục: bỏ `clean/`, đổi `final/` thành per-source).

### `run.py`
- Bỏ import `run_merge` từ `merge_pipeline`; bỏ `clean_path`, `find_latest_clean` khỏi import từ `config`.
- `run_pipeline_for_source(source, input_file, mapper)` — ghi output ra `final_path(source)` thay vì `clean_path(source)`; thêm vài stat (`phone_full`, `avg_quality`, `processed_at`) vào dict ghi ra. Nhánh "nhatot đã là clean DTO" cũng ghi ra `final_path(source)`.
- Bỏ hẳn STEP 3 (MERGE) trong `full_cycle()`. STEP 2 (PIPELINE) là bước cuối; mỗi nguồn ghi file final riêng. Bỏ các biến `nhatot_clean` / `muaban_clean` (đổi tên thành `*_final` hoặc tương đương).
- Bỏ `"data/clean"` khỏi vòng `os.makedirs([...])`.
- Phần SUMMARY: in stats cho **từng nguồn** đã chạy (nhatot riêng, muaban riêng — total, phone_full, avg_quality, đường dẫn file) thay vì đọc 1 file merged duy nhất.
- Cập nhật docstring module (mô tả output mới).

### `pipeline/merge_pipeline.py`
- **Giữ lại, không xoá.** File không còn được `run.py` gọi nhưng vẫn chạy tay được (`python merge_pipeline.py --nhatot ... --muaban ...`) nếu sau này cần gộp thủ công.
- Sửa **1 chỗ duy nhất**: lời gọi `final_path()` (hiện không tham số) → `final_path("merged")`, để khớp chữ ký mới của `config.final_path`. Output gộp thủ công sẽ vào `data/final/merged/{date}/{HHMMSS}.json`. Ngoài dòng đó, không đụng logic merge.

### `pipeline/unified_pipeline.py`
- Không thay đổi.

### `CLAUDE.md`
- Mục **Layout**: bỏ `data/clean/`, đổi `data/final/{YYYY-MM-DD}/{HHMMSS}_merged.json` → `data/final/{source}/{YYYY-MM-DD}/{HHMMSS}.json`.
- Mục **Architecture**: bỏ mô tả bước "Merge"; "Pipeline flow" kết thúc ở `PropertyDTO` ghi ra `data/final/{source}/...`.
- Mục **Commands** / **Conventions**: cập nhật mô tả output cho khớp.

### Không nằm trong phạm vi
- Không động vào logic scrape (`nhatot_fast_scraper.py`, `muaban_scraper.py`).
- Không động vào `unified_pipeline.py`.
- Không tự xoá dữ liệu cũ trong `data/clean/` đang có trên đĩa (người dùng tự dọn).
- Không cập nhật `data/sample/2026-04-14_merged.json` (chỉ là reference cũ, để nguyên).

## Verify
- Repo không có test thực sự (chỉ test trong `.venv`), nên không có test phải sửa.
- Verify bằng:
  1. `python -c "import config, run, merge_pipeline"` — tất cả import resolve, không còn tham chiếu `clean_path` / `find_latest_clean` / `run_merge`.
  2. `python -c "from config import final_path, find_latest_final; print(final_path('nhatot')); print(final_path('muaban'))"` — đường dẫn đúng dạng `data/final/{source}/{date}/{HHMMSS}.json`.
  3. Chạy thử pipeline trên raw có sẵn (không scrape lại): một đoạn one-off gọi `run_pipeline_for_source("muaban", find_latest_raw("muaban"), mapper)` → kiểm tra file xuất hiện dưới `data/final/muaban/{date}/...` và đọc lại được JSON hợp lệ với key `listings`.
- Không tự xoá `data/clean/` cũ; không commit dữ liệu trong `data/`.
