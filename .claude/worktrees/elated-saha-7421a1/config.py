"""
V-Nexus Scraper — Naming & Path Convention

Data structure:
  data/
  ├── raw/                           Raw scraped data (per source, per date, per run)
  │   ├── nhatot/2026-04-14/143000_raw.json
  │   ├── nhatot/2026-04-14/200000_raw.json    ← 2nd run same day
  │   ├── muaban/2026-04-14/143000_raw.json
  ├── clean/                         Pipeline output
  │   ├── nhatot/2026-04-14/143000_clean.json
  │   └── muaban/2026-04-14/143000_clean.json
  └── final/                         Merged output
      ├── 2026-04-14/143000_merged.json
      └── 2026-04-14/200000_merged.json        ← 2nd run same day

Naming rules:
  - Folder by date: YYYY-MM-DD
  - File prefix by time: HHMMSS (giờ chạy)
  - Multiple runs same day → KHÔNG ghi đè, mỗi run 1 file riêng
  - Logs: logs/YYYY-MM-DD/HHMMSS_{source}.log
"""

import os
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Session timestamp: set once per run, shared across all files in same cycle
_session_ts = None


def _get_session_ts() -> str:
    """Get or create session timestamp (HHMMSS). Same value within 1 run cycle."""
    global _session_ts
    if _session_ts is None:
        _session_ts = datetime.now().strftime("%H%M%S")
    return _session_ts


def reset_session():
    """Reset session timestamp for a new run cycle."""
    global _session_ts
    _session_ts = None


def today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def raw_path(source: str, date: str = None) -> str:
    """data/raw/{source}/{date}/{HHMMSS}_raw.json"""
    date = date or today()
    ts = _get_session_ts()
    path = BASE_DIR / "data" / "raw" / source / date
    os.makedirs(path, exist_ok=True)
    return str(path / f"{ts}_raw.json")


def clean_path(source: str, date: str = None) -> str:
    """data/clean/{source}/{date}/{HHMMSS}_clean.json"""
    date = date or today()
    ts = _get_session_ts()
    path = BASE_DIR / "data" / "clean" / source / date
    os.makedirs(path, exist_ok=True)
    return str(path / f"{ts}_clean.json")


def final_path(date: str = None) -> str:
    """data/final/{date}/{HHMMSS}_merged.json"""
    date = date or today()
    ts = _get_session_ts()
    path = BASE_DIR / "data" / "final" / date
    os.makedirs(path, exist_ok=True)
    return str(path / f"{ts}_merged.json")


def log_path(source: str, date: str = None) -> str:
    """logs/{date}/{HHMMSS}_{source}.log"""
    date = date or today()
    ts = _get_session_ts()
    path = BASE_DIR / "logs" / date
    os.makedirs(path, exist_ok=True)
    return str(path / f"{ts}_{source}.log")


def find_latest_raw(source: str):
    """Find the most recent raw file for a source."""
    raw_dir = BASE_DIR / "data" / "raw" / source
    if not raw_dir.exists():
        return None
    # Sort by date desc, then by filename (timestamp) desc
    for date_dir in sorted(raw_dir.iterdir(), key=lambda d: d.name, reverse=True):
        if date_dir.is_dir():
            files = sorted(date_dir.glob("*_raw.json"), reverse=True)
            if files:
                return str(files[0])
    return None


def find_latest_clean(source: str):
    """Find the most recent clean file for a source."""
    clean_dir = BASE_DIR / "data" / "clean" / source
    if not clean_dir.exists():
        return None
    for date_dir in sorted(clean_dir.iterdir(), key=lambda d: d.name, reverse=True):
        if date_dir.is_dir():
            files = sorted(date_dir.glob("*_clean.json"), reverse=True)
            if files:
                return str(files[0])
    return None


def find_latest_final():
    """Find the most recent merged file."""
    final_dir = BASE_DIR / "data" / "final"
    if not final_dir.exists():
        return None
    for date_dir in sorted(final_dir.iterdir(), key=lambda d: d.name, reverse=True):
        if date_dir.is_dir():
            files = sorted(date_dir.glob("*_merged.json"), reverse=True)
            if files:
                return str(files[0])
    return None
