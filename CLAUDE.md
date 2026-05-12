# CLAUDE.md — V-Nexus Scraper

This repo chứa **Python scrapers** cho V-Nexus Vietnamese real estate data pipeline.
Backend API (Spring Boot) và Frontend (Next.js + MUI) nằm ở repo riêng.

## Project scope

Python 3.11 scrapers thu thập property listings từ các portal BĐS Việt Nam, normalize, classify, dedup rồi ghi vào shared PostgreSQL (cùng DB với backend Spring Boot).

## Layout

```
.
├── run.py                   # Master runner: scrape → pipeline (per-source final, no merge)
├── config.py                # Path convention + session timestamp
├── requirements.txt
├── .env.example
├── scrapers/                # Per-source scrapers
│   ├── nhatot_fast_scraper.py   # httpx fast layer + Playwright phone reveal
│   └── muaban_scraper.py        # Playwright (Cloudflare bypass) + API intercept
├── pipeline/                # Normalization
│   ├── unified_pipeline.py      # Raw → PropertyDTO (31 fields)
│   ├── merge_pipeline.py        # (manual only) cross-source dedup — not in run.py flow
│   └── reference/               # VN admin divisions (province/ward JSON)
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

# Run
python run.py                          # Full cycle: scrape + pipeline (per-source final)
python run.py --nhatot-only            # Only nhatot
python run.py --muaban-only            # Only muaban
python run.py --loop --interval 60     # Loop every 60 min

# Tests
pytest
```

## Architecture

**Two-tier scraping (nhatot):**
- Fast layer: `httpx` with 10 concurrent requests
- Slow layer: Playwright phone reveal, only for new listings

**Pipeline flow (per source):** Raw JSON → Source Adapter → Address Mapping → Price Validation → Property Classification → Broker Detection → `PropertyDTO` → `data/final/{source}/...`

**No cross-source merge:** nhatot and muaban stay in separate files. `merge_pipeline.py` is kept for manual ad-hoc merging (`python pipeline/merge_pipeline.py --nhatot ... --muaban ...` → `data/final/merged/...`) but is not part of `run.py`.

## Conventions

- Python 3.11, async/await
- Path: `BASE_DIR = Path(__file__).resolve().parent` — no hardcoded paths
- Output naming: `YYYY-MM-DD/HHMMSS_*.json` — never overwrite, each run is a new file
- Env: copy `.env.example` → `.env`
- Scraping ethics: xem `../.claude/rules/scraping-ethics.md` (2s rate limit, robots.txt, no CAPTCHA bypass)

## Data schema

Output conforms to `PropertyDTO` (31 fields). Sample output: `data/sample/2026-04-14_merged.json`.
Shared PostgreSQL schema owned by Spring Boot backend — scraper reads Flyway migrations cho truth.

## Không thuộc phạm vi repo này

- REST API, business logic → Spring Boot repo
- Web UI → Next.js + MUI repo
- Database schema / migrations → Flyway trong Spring Boot repo
