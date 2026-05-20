# CLAUDE.md — V-Nexus Scraper

This repo chứa **Python scrapers** cho V-Nexus Vietnamese real estate data pipeline.
Backend API (Spring Boot) và Frontend (Next.js + MUI) nằm ở repo riêng.

## Project scope

Python 3.11 scrapers thu thập property listings từ các portal BĐS Việt Nam, normalize, classify, dedup rồi ghi vào shared PostgreSQL (cùng DB với backend Spring Boot).

## Layout

```
.
├── run.py                   # File-only runner: scrape → pipeline → JSON final + R2 image upload (KHÔNG ghi DB)
├── config.py                # Path convention + session timestamp
├── requirements.txt
├── .env.example
├── scrapers/                # Per-source scrapers
│   ├── nhatot_fast_scraper.py   # httpx fast layer + Playwright phone reveal
│   └── muaban_scraper.py        # Playwright (Cloudflare bypass) + API intercept
├── pipeline/                # Normalization
│   ├── unified_pipeline.py      # Raw → PropertyDTO (31 fields)
│   ├── normalize_to_db.py       # PropertyDTO → row khớp schema bảng data_sources
│   ├── phone_history.py         # Đếm tin cùng (source, phone_full) trong DB — signal phân loại
│   ├── merge_pipeline.py        # (manual only) cross-source dedup — not in run.py flow
│   └── reference/               # VN admin divisions (province/ward JSON)
├── db/                      # DB layer (SQLAlchemy 2.0, sync psycopg2)
│   ├── session.py               # Engine + session_scope() (đọc .env)
│   ├── models.py                # ORM model bảng data_sources (sync thủ công với data_source.sql)
│   └── repository.py            # upsert_data_sources — INSERT ... ON CONFLICT DO UPDATE
├── classification/          # Phân loại người đăng
│   └── poster.py                # classify_batch_in_db — rule-based moi_gioi | chu_nha
├── scheduler/               # Pipeline TỰ ĐỘNG crawl → DB (APScheduler)
│   ├── main.py                  # BlockingScheduler — chạy mỗi N phút (default 5)
│   └── jobs.py                  # 1 cycle/source: crawl → normalize → upsert → classify
├── data_source.sql          # Tham chiếu schema bảng data_sources (truth = Flyway backend)
├── data/                    # Scraped output (gitignored except sample/)
│   ├── raw/{source}/{YYYY-MM-DD}/{HHMMSS}_raw.json
│   ├── final/{source}/{YYYY-MM-DD}/{HHMMSS}.json
│   └── sample/              # Committed sample output (reference for backend)
└── logs/{YYYY-MM-DD}/{HHMMSS}_{source}.log
```

## Commands

```bash
# Install
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env                   # cần DB_* khi chạy scheduler ghi PostgreSQL

# Run — file output only (KHÔNG ghi DB)
python run.py                          # scrape + pipeline → JSON final + upload ảnh R2
python run.py --nhatot-only            # Only nhatot
python run.py --muaban-only            # Only muaban
python run.py --loop --interval 60     # Loop mỗi 60 min (vẫn chỉ ra file)

# Run — pipeline TỰ ĐỘNG ghi vào PostgreSQL (APScheduler)
python -m scheduler.main               # crawl → DB mỗi 5 phút (mặc định)
python -m scheduler.main --interval 1  # mỗi 1 phút
python -m scheduler.main --once        # chạy 1 cycle rồi thoát
python -m scheduler.main --nhatot-only # chỉ nhatot

# Tests
pytest
```

## Architecture

**Hai entrypoint riêng biệt (dùng chung scraper + `unified_pipeline`, khác đích đến):**
- `run.py` — scrape → pipeline → **file JSON** `data/final/...` + upload ảnh Cloudflare R2. **KHÔNG ghi DB.**
- `scheduler/` — pipeline **TỰ ĐỘNG ghi PostgreSQL**. KHÔNG upload ảnh R2 (row giữ URL ảnh gốc của portal).

**Two-tier scraping (nhatot):**
- Fast layer: `httpx` with 10 concurrent requests
- Slow layer: Playwright phone reveal, only for new listings

**File pipeline flow (`run.py`, per source):** Raw JSON → Source Adapter → Address Mapping → Price Validation → Property Classification → Broker Detection → `PropertyDTO` → `data/final/{source}/...`

**DB pipeline flow (`scheduler/`, per source, mỗi cycle):** crawl tin mới → `unified_pipeline` → `PropertyDTO` → `normalize_to_db.dto_to_row` → row `data_sources` → `repository.upsert_data_sources` (INSERT ... ON CONFLICT (source, source_id) DO UPDATE, bảo toàn `phone_full`) → `classification.poster.classify_batch_in_db` (gán `poster_type`). Tất cả trong `db.session.session_scope()` (auto commit/rollback). 2 source chạy song song (`asyncio.gather`), lỗi nguồn này không block nguồn kia. APScheduler `coalesce=True` + `max_instances=1` tránh chồng cycle. Production: chạy `scheduler.main` như systemd/supervisor service.

**No cross-source merge:** nhatot and muaban stay in separate files. `merge_pipeline.py` is kept for manual ad-hoc merging (`python pipeline/merge_pipeline.py --nhatot ... --muaban ...` → `data/final/merged/...`) but is not part of `run.py`.

## Conventions

- Python 3.11, async/await
- Path: `BASE_DIR = Path(__file__).resolve().parent` — no hardcoded paths
- Output naming: `YYYY-MM-DD/HHMMSS_*.json` — never overwrite, each run is a new file
- Env: copy `.env.example` → `.env` (gồm `DB_HOST/PORT/NAME/USER/PASSWORD` cho DB flow)
- Scraping ethics: xem `../.claude/rules/scraping-ethics.md` (2s rate limit, robots.txt, no CAPTCHA bypass)

## Data schema

Output conforms to `PropertyDTO` (31 fields). Sample output: `data/sample/2026-04-14_merged.json`.
Bảng đích khi ghi DB: `data_sources` (xem `data_source.sql` để tham chiếu cột; ORM ở `db/models.py`).
Shared PostgreSQL schema owned by Spring Boot backend — scraper reads Flyway migrations cho truth.

## Không thuộc phạm vi repo này

- REST API, business logic, analytics → Spring Boot repo (repo này chỉ còn luồng scraper → DB; KHÔNG còn FastAPI / analytics Python)
- Web UI → Next.js + MUI repo
- Database schema / migrations → Flyway trong Spring Boot repo
