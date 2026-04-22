# CLAUDE.md — V-Nexus Scraper

This repo chứa **Python scrapers** cho V-Nexus Vietnamese real estate data pipeline.
Backend API (Spring Boot) và Frontend (Next.js + MUI) nằm ở repo riêng.

## Project scope

Python 3.11 scrapers thu thập property listings từ các portal BĐS Việt Nam, normalize, classify, dedup rồi ghi vào shared PostgreSQL (cùng DB với backend Spring Boot).

## Layout

```
.
├── run.py                   # Master runner: scrape → pipeline → merge
├── config.py                # Path convention + session timestamp
├── requirements.txt
├── .env.example
├── scrapers/                # Per-source scrapers
│   ├── nhatot_fast_scraper.py   # httpx fast layer + Playwright phone reveal
│   └── muaban_scraper.py        # Playwright (Cloudflare bypass) + API intercept
├── pipeline/                # Normalization & merging
│   ├── unified_pipeline.py      # Raw → PropertyDTO (35 fields)
│   ├── merge_pipeline.py        # Cross-source dedup (ward + price ±15% + area ±10%)
│   └── reference/               # VN admin divisions (province/ward JSON)
├── data/                    # Scraped output (gitignored except sample/)
│   ├── raw/{source}/{YYYY-MM-DD}/{HHMMSS}_raw.json
│   ├── clean/{source}/{YYYY-MM-DD}/{HHMMSS}_clean.json
│   ├── final/{YYYY-MM-DD}/{HHMMSS}_merged.json
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
python run.py                          # Full cycle: scrape + pipeline + merge
python run.py --skip-scrape            # Re-run pipeline on latest raw
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

**Pipeline flow:** Raw JSON → Source Adapter → Address Mapping → Price Validation → Property Classification → Broker Detection → Quality Score → `PropertyDTO`

**Merge:** Match listings across sources by `ward + price (±15%) + area (±10%)`, take strongest field from each source, dedupe.

## Conventions

- Python 3.11, async/await
- Path: `BASE_DIR = Path(__file__).resolve().parent` — no hardcoded paths
- Output naming: `YYYY-MM-DD/HHMMSS_*.json` — never overwrite, each run is a new file
- Env: copy `.env.example` → `.env`
- Scraping ethics: xem `../.claude/rules/scraping-ethics.md` (2s rate limit, robots.txt, no CAPTCHA bypass)

## Data schema

Output conforms to `PropertyDTO` (35 fields). Sample output: `data/sample/2026-04-14_merged.json`.
Shared PostgreSQL schema owned by Spring Boot backend — scraper reads Flyway migrations cho truth.

## Không thuộc phạm vi repo này

- REST API, business logic → Spring Boot repo
- Web UI → Next.js + MUI repo
- Database schema / migrations → Flyway trong Spring Boot repo
