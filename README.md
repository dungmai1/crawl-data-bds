# V-Nexus Scraper

Python scrapers thu thập dữ liệu bất động sản từ các portal Việt Nam (nhatot, muaban) cho nền tảng V-Nexus.

Backend API (Spring Boot) và Frontend (Next.js) ở repo khác.

## Quick start

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
cp .env.example .env       # sau đó sửa DB creds

# Run full cycle: cào → lọc → merge → 1 file sạch
python run.py

# Các chế độ khác
python run.py --skip-scrape          # Chỉ chạy pipeline trên raw có sẵn
python run.py --nhatot-only          # Chỉ nhatot
python run.py --muaban-only          # Chỉ muaban
python run.py --loop --interval 60   # Chạy liên tục mỗi 60 phút
```

Output: `data/final/{YYYY-MM-DD}/{HHMMSS}_merged.json`

## Cấu trúc

- `scrapers/` — Scrapers per-source (nhatot, muaban)
- `pipeline/` — Normalize + merge
- `data/` — Output (gitignored, chỉ giữ `data/sample/`)
- `logs/` — Logs theo ngày

Chi tiết: xem [CLAUDE.md](CLAUDE.md).

## Pipeline

```
Raw JSON
  → Source Adapter
  → Address Mapping (province/ward/street normalize)
  → Price Validation
  → Property Classification
  → Broker Detection
  → PropertyDTO (31 fields)
  → Cross-source merge (ward + price ±15% + area ±10%)
  → Final merged file
```

## Sources

| Source    | Strategy                                           |
|-----------|----------------------------------------------------|
| nhatot    | httpx fast (10 concurrent) + Playwright phone reveal |
| muaban    | Playwright (Cloudflare bypass) + API intercept     |

## Ethics

Xem `../.claude/rules/scraping-ethics.md` — rate limit 2s, respect robots.txt, không bypass CAPTCHA, dừng ngay khi nhận 429.

## Dependencies

Python 3.11, `httpx`, `playwright`, `psycopg2-binary`, `sqlalchemy`, `pandas`, `apscheduler`, `loguru`. Full list: `requirements.txt`.
"# crawl-data-bds" 
"# crawl-data-bds" 
