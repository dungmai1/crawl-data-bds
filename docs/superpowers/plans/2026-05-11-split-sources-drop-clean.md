# Split Sources, Drop Merge & `data/clean` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `run.py` produce one normalized `PropertyDTO` JSON per source under `data/final/{source}/{date}/{HHMMSS}.json` — no cross-source merge, no `data/clean/` intermediate.

**Architecture:** Two-step pipeline (SCRAPE → PIPELINE). Each source's raw JSON goes through `unified_pipeline.process_batch()` and is written straight to `data/final/{source}/...`. The `merge_pipeline.py` module is kept on disk for manual ad-hoc merges but is no longer imported by `run.py`. `config.py` gains a `source` parameter on `final_path()` / `find_latest_final()` and loses `clean_path()` / `find_latest_clean()`.

**Tech Stack:** Python 3.11, pytest 9, stdlib `json`/`pathlib`. No new dependencies.

**Working dir:** `c:\Users\win11\Desktop\crawl-data-bds`. If `.venv` is not active, prefix Python commands with `.venv\Scripts\python.exe` instead of `python`.

---

## File Structure

| File | Action | Responsibility after change |
|------|--------|------------------------------|
| `config.py` | Modify | Path helpers — `final_path(source, date)`, `find_latest_final(source)`; no clean helpers; updated module docstring |
| `tests/test_config_paths.py` | Create | Pytest coverage for `raw_path`, `final_path`, `find_latest_final` shapes |
| `run.py` | Modify | Two-step cycle: scrape → per-source pipeline → per-source final file; per-source summary; no merge import |
| `pipeline/merge_pipeline.py` | Modify (1 line) | Still works when run manually — calls `final_path("merged")` |
| `pipeline/unified_pipeline.py` | Modify (2 lines) | Standalone `run_pipeline()` script path writes to `data/final/{source}/...` instead of `data/clean/...` |
| `CLAUDE.md` | Modify | Layout + Architecture sections reflect no-merge / no-clean flow |

`.gitignore` already ignores `/data/clean/`, `/data/final/`, `/data/raw/` — leave it untouched (a leftover `data/clean/` on disk stays ignored).

---

## Task 1: `config.py` — per-source `final_path`, drop clean helpers

**Files:**
- Modify: `config.py` (docstring lines 1-22; `clean_path` lines 61-67; `final_path` lines 70-76; `find_latest_clean` lines 102-112; `find_latest_final` lines 115-125)
- Test: `tests/test_config_paths.py` (create)

- [ ] **Step 1: Write the failing test**

Create `tests/test_config_paths.py`:

```python
import re
import config


def test_raw_path_shape():
    config.reset_session()
    p = config.raw_path("muaban").replace("\\", "/")
    assert re.search(r"/data/raw/muaban/\d{4}-\d{2}-\d{2}/\d{6}_raw\.json$", p), p


def test_final_path_is_per_source_no_merged_suffix():
    config.reset_session()
    p = config.final_path("nhatot").replace("\\", "/")
    assert re.search(r"/data/final/nhatot/\d{4}-\d{2}-\d{2}/\d{6}\.json$", p), p
    assert "_merged" not in p


def test_final_path_different_sources_different_dirs():
    config.reset_session()
    a = config.final_path("nhatot").replace("\\", "/")
    b = config.final_path("muaban").replace("\\", "/")
    assert "/data/final/nhatot/" in a
    assert "/data/final/muaban/" in b


def test_find_latest_final_returns_none_for_unknown_source(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "BASE_DIR", tmp_path)
    assert config.find_latest_final("does-not-exist") is None


def test_clean_helpers_are_removed():
    assert not hasattr(config, "clean_path")
    assert not hasattr(config, "find_latest_clean")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `python -m pytest tests/test_config_paths.py -v`
Expected: FAILs — `test_final_path_is_per_source_no_merged_suffix` errors with `TypeError: final_path() takes ... 'date'` (currently `final_path()` has no `source` param), and `test_clean_helpers_are_removed` fails because `clean_path` still exists.

- [ ] **Step 3: Update the module docstring**

Replace `config.py` lines 1-22 (the opening `"""..."""`) with:

```python
"""
V-Nexus Scraper — Naming & Path Convention

Data structure:
  data/
  ├── raw/                           Raw scraped data (per source, per date, per run)
  │   ├── nhatot/2026-04-14/143000_raw.json
  │   ├── nhatot/2026-04-14/200000_raw.json    ← 2nd run same day
  │   ├── muaban/2026-04-14/143000_raw.json
  └── final/                         Pipeline output — normalized PropertyDTO, per source
      ├── nhatot/2026-04-14/143000.json
      ├── nhatot/2026-04-14/200000.json        ← 2nd run same day
      └── muaban/2026-04-14/143000.json

Naming rules:
  - Folder by date: YYYY-MM-DD
  - File prefix by time: HHMMSS (giờ chạy)
  - Multiple runs same day → KHÔNG ghi đè, mỗi run 1 file riêng
  - Logs: logs/YYYY-MM-DD/HHMMSS_{source}.log
"""
```

- [ ] **Step 4: Delete `clean_path`**

Remove this function from `config.py` entirely (currently lines 61-67):

```python
def clean_path(source: str, date: str = None) -> str:
    """data/clean/{source}/{date}/{HHMMSS}_clean.json"""
    date = date or today()
    ts = _get_session_ts()
    path = BASE_DIR / "data" / "clean" / source / date
    os.makedirs(path, exist_ok=True)
    return str(path / f"{ts}_clean.json")
```

- [ ] **Step 5: Replace `final_path` with the per-source version**

Replace `final_path` (currently lines 70-76) with:

```python
def final_path(source: str, date: str = None) -> str:
    """data/final/{source}/{date}/{HHMMSS}.json"""
    date = date or today()
    ts = _get_session_ts()
    path = BASE_DIR / "data" / "final" / source / date
    os.makedirs(path, exist_ok=True)
    return str(path / f"{ts}.json")
```

- [ ] **Step 6: Delete `find_latest_clean`**

Remove this function from `config.py` entirely (currently lines 102-112):

```python
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
```

- [ ] **Step 7: Replace `find_latest_final` with the per-source version**

Replace `find_latest_final` (currently lines 115-125) with:

```python
def find_latest_final(source: str):
    """Find the most recent final file for a source."""
    final_dir = BASE_DIR / "data" / "final" / source
    if not final_dir.exists():
        return None
    for date_dir in sorted(final_dir.iterdir(), key=lambda d: d.name, reverse=True):
        if date_dir.is_dir():
            files = sorted(date_dir.glob("*.json"), reverse=True)
            if files:
                return str(files[0])
    return None
```

- [ ] **Step 8: Run the tests to verify they pass**

Run: `python -m pytest tests/test_config_paths.py -v`
Expected: 5 passed.

- [ ] **Step 9: Commit**

```bash
git add config.py tests/test_config_paths.py
git commit -m "refactor(config): per-source final_path, drop clean path helpers"
```

---

## Task 2: `run.py` — two-step cycle, per-source final output, no merge

**Files:**
- Modify: `run.py` (module docstring lines 1-13; imports lines 30-32; `run_pipeline_for_source` lines 66-99; `full_cycle` makedirs line 119, STEP 2 lines 159-176, STEP 3 lines 178-196, SUMMARY lines 198-217)

- [ ] **Step 1: Update the module docstring**

Replace `run.py` lines 1-13 (the opening `"""..."""`) with:

```python
"""
V-Nexus: Master Scraper Runner
1 lệnh: cào nhatot + muaban → lọc (normalize/classify) → ghi file final riêng từng nguồn

Usage:
    python run.py                          # Full cycle: cào cả 2 + lọc
    python run.py --nhatot-only            # Chỉ cào nhatot
    python run.py --muaban-only            # Chỉ cào muaban
    python run.py --loop --interval 60     # Chạy liên tục mỗi 60 phút

Output (per source, no merge):
    data/final/{source}/{YYYY-MM-DD}/{HHMMSS}.json
"""
```

- [ ] **Step 2: Update imports**

Replace `run.py` lines 30-32:

```python
from config import clean_path, final_path, find_latest_raw, find_latest_final, reset_session
from unified_pipeline import process_batch, AddressMapper
from merge_pipeline import run_merge
```

with:

```python
from config import final_path, find_latest_raw, reset_session
from unified_pipeline import process_batch, AddressMapper
```

- [ ] **Step 3: Replace `run_pipeline_for_source` and add `_write_final`**

Replace the whole `run_pipeline_for_source` function (currently lines 66-99) with:

```python
def _write_final(out: str, source: str, listings: list) -> None:
    """Write per-source final JSON with summary stats."""
    total = len(listings)
    phone_full = sum(1 for x in listings if x.get("phone_full"))
    avg_quality = round(sum(x.get("quality_score", 0) for x in listings) / max(total, 1), 1)
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "source": source,
            "total": total,
            "phone_full": phone_full,
            "avg_quality": avg_quality,
            "processed_at": datetime.now().isoformat(),
            "listings": listings,
        }, f, ensure_ascii=False, indent=2)


def run_pipeline_for_source(source: str, input_file: str, mapper: AddressMapper) -> str:
    """Run unified pipeline on raw data. Returns final file path (data/final/{source}/...)."""
    with open(input_file, encoding="utf-8") as f:
        data = json.load(f)

    # nhatot_fast_scraper already outputs clean DTOs in "listings"
    # muaban_scraper outputs raw items in "items"
    if source == "nhatot":
        items = data.get("listings", data.get("ads", []))
        # Check if already processed (has 'source' field = DTO)
        if items and isinstance(items[0], dict) and items[0].get("source") == "nhatot":
            log.info(f"  [{source}] Already clean DTO ({len(items)} listings), skipping pipeline")
            out = final_path(source)
            _write_final(out, source, items)
            log.info(f"  [{source}] Final: {len(items)} listings → {out}")
            return out
        items = data.get("ads", items)
    else:
        items = data.get("items", [])

    log.info(f"  [{source}] Processing {len(items)} raw items through pipeline...")
    results = process_batch(items, source, mapper)

    from dataclasses import asdict
    out = final_path(source)
    _write_final(out, source, [asdict(r) for r in results])
    log.info(f"  [{source}] Final: {len(results)} listings → {out}")
    return out
```

- [ ] **Step 4: Update the `os.makedirs` list in `full_cycle`**

In `full_cycle` (currently line 119), replace:

```python
    for d in ["data/raw/nhatot", "data/raw/muaban", "data/clean", "data/final"]:
        os.makedirs(d, exist_ok=True)
```

with:

```python
    for d in ["data/raw/nhatot", "data/raw/muaban", "data/final/nhatot", "data/final/muaban"]:
        os.makedirs(d, exist_ok=True)
```

- [ ] **Step 5: Replace STEP 2 + STEP 3 + SUMMARY**

Replace everything in `full_cycle` from the `# === STEP 2: PIPELINE` comment (currently line 159) through the `return final_file` at the end of the function (currently line 217) with:

```python
    # === STEP 2: PIPELINE (normalize + classify) → per-source final ===
    log.info(f"\n{'='*50}")
    log.info(f"  STEP 2: PIPELINE (normalize + classify)")
    log.info(f"{'='*50}")

    ref_dir = str(_base / "pipeline" / "reference")
    try:
        mapper = AddressMapper(ref_dir)
    except Exception as e:
        log.warning(f"AddressMapper failed: {e}")
        mapper = None

    final_files = []
    if nhatot_raw:
        final_files.append(("nhatot", run_pipeline_for_source("nhatot", nhatot_raw, mapper)))
    if muaban_raw:
        final_files.append(("muaban", run_pipeline_for_source("muaban", muaban_raw, mapper)))

    if not final_files:
        log.error("No clean data produced")
        return None

    # === SUMMARY ===
    elapsed = int(time.time() - start)
    log.info(f"\n{'#'*60}")
    log.info(f"  CYCLE COMPLETE in {elapsed}s")
    for src, fp in final_files:
        with open(fp, encoding="utf-8") as f:
            d = json.load(f)
        log.info(
            f"  [{src}] total={d.get('total', 0)}  "
            f"phone_full={d.get('phone_full', 0)}  avg_quality={d.get('avg_quality', 0)}"
        )
        log.info(f"         → {fp}")
    log.info(f"{'#'*60}")

    return [fp for _, fp in final_files]
```

- [ ] **Step 6: Verify the module imports and CLI parses**

Run: `python -c "import run; print('ok')"`
Expected: prints `ok` with no traceback (confirms no dangling `clean_path` / `run_merge` / `find_latest_final` references).

Run: `python run.py --help`
Expected: argparse help text prints, exit 0.

- [ ] **Step 7: Commit**

```bash
git add run.py
git commit -m "feat(run): per-source final output, drop merge step and data/clean"
```

---

## Task 3: Fix the other `config.final_path` / `clean_path` callers

Two standalone scripts call `config.final_path()` / `config.clean_path()` with the old signature and would crash after Task 1. Neither is used by `run.py`, but both are runnable on their own (`python pipeline/merge_pipeline.py ...`, `python pipeline/unified_pipeline.py --source ...`) so keep them working: route their default output to `data/final/{source}/...`.

**Files:**
- Modify: `pipeline/merge_pipeline.py` (the `out = final_path()` call inside `run_merge`, near `from config import final_path`)
- Modify: `pipeline/unified_pipeline.py` (the `from config import clean_path` / `output_file = clean_path(source)` lines inside `run_pipeline`, near "Save")

- [ ] **Step 1: `merge_pipeline.py` — pass a source to `final_path`**

In `pipeline/merge_pipeline.py`, find:

```python
    from config import final_path
    out = final_path()
```

and change the second line to:

```python
    from config import final_path
    out = final_path("merged")
```

(No other changes — the merge logic stays as-is. Manual merges now land in `data/final/merged/{date}/{HHMMSS}.json`.)

- [ ] **Step 2: `unified_pipeline.py` — use `final_path(source)` instead of `clean_path(source)`**

In `pipeline/unified_pipeline.py`, inside `run_pipeline()`'s "Save" block, find:

```python
        from config import clean_path
        output_file = clean_path(source)
```

and change it to:

```python
        from config import final_path
        output_file = final_path(source)
```

(No other changes — the rest of `run_pipeline()` and the whole `process_batch` path stay as-is. `run.py` does NOT use `run_pipeline()`; this is only the standalone-script path.)

- [ ] **Step 3: Verify both import cleanly**

Run: `python -c "import sys; sys.path.insert(0, 'pipeline'); import merge_pipeline, unified_pipeline; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 4: Confirm no remaining `clean_path` / `find_latest_clean` references anywhere**

Run: `git grep -nE "clean_path|find_latest_clean"`
Expected: no matches (exit 1).

- [ ] **Step 5: Commit**

```bash
git add pipeline/merge_pipeline.py pipeline/unified_pipeline.py
git commit -m "fix(pipeline): route standalone scripts to data/final, drop clean_path usage"
```

---

## Task 4: `CLAUDE.md` — document the no-merge / no-clean flow

**Files:**
- Modify: `CLAUDE.md` (Layout block; Architecture "Pipeline flow" / "Merge" lines)

- [ ] **Step 1: Update the Layout block**

In `CLAUDE.md`, replace this part of the ``` ``` layout block:

```
├── pipeline/                # Normalization & merging
│   ├── unified_pipeline.py      # Raw → PropertyDTO (35 fields)
│   ├── merge_pipeline.py        # Cross-source dedup (ward + price ±15% + area ±10%)
│   └── reference/               # VN admin divisions (province/ward JSON)
├── data/                    # Scraped output (gitignored except sample/)
│   ├── raw/{source}/{YYYY-MM-DD}/{HHMMSS}_raw.json
│   ├── clean/{source}/{YYYY-MM-DD}/{HHMMSS}_clean.json
│   ├── final/{YYYY-MM-DD}/{HHMMSS}_merged.json
│   └── sample/              # Committed sample output (reference for backend)
```

with:

```
├── pipeline/                # Normalization
│   ├── unified_pipeline.py      # Raw → PropertyDTO (35 fields)
│   ├── merge_pipeline.py        # (manual only) cross-source dedup — not in run.py flow
│   └── reference/               # VN admin divisions (province/ward JSON)
├── data/                    # Scraped output (gitignored except sample/)
│   ├── raw/{source}/{YYYY-MM-DD}/{HHMMSS}_raw.json
│   ├── final/{source}/{YYYY-MM-DD}/{HHMMSS}.json
│   └── sample/              # Committed sample output (reference for backend)
```

- [ ] **Step 2: Update the Architecture section**

In `CLAUDE.md`, replace:

```
**Pipeline flow:** Raw JSON → Source Adapter → Address Mapping → Price Validation → Property Classification → Broker Detection → Quality Score → `PropertyDTO`

**Merge:** Match listings across sources by `ward + price (±15%) + area (±10%)`, take strongest field from each source, dedupe.
```

with:

```
**Pipeline flow (per source):** Raw JSON → Source Adapter → Address Mapping → Price Validation → Property Classification → Broker Detection → Quality Score → `PropertyDTO` → `data/final/{source}/...`

**No cross-source merge:** nhatot and muaban stay in separate files. `merge_pipeline.py` is kept for manual ad-hoc merging (`python pipeline/merge_pipeline.py --nhatot ... --muaban ...` → `data/final/merged/...`) but is not part of `run.py`.
```

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: CLAUDE.md reflects per-source output, no merge/clean"
```

---

## Task 5: End-to-end smoke test on existing raw data

**Files:** none modified — verification only.

- [ ] **Step 1: Confirm a muaban raw file exists**

Run: `python -c "import sys; sys.path.insert(0,'.'); from config import find_latest_raw; print(find_latest_raw('muaban'))"`
Expected: prints a path like `...\data\raw\muaban\2026-05-11\144118_raw.json` (not `None`). If it prints `None`, skip to Step 3 and note that no raw data was available to smoke-test.

- [ ] **Step 2: Run the pipeline on that raw file and check the output**

Run:

```bash
python -c "import sys; sys.path[:0]=['.', 'scrapers', 'pipeline']; from config import find_latest_raw, find_latest_final; from unified_pipeline import AddressMapper; from run import run_pipeline_for_source; m=AddressMapper('pipeline/reference'); out=run_pipeline_for_source('muaban', find_latest_raw('muaban'), m); print('WROTE', out); import json; d=json.load(open(out, encoding='utf-8')); print('source=', d['source'], 'total=', d['total'], 'keys=', sorted(d.keys())); print('latest=', find_latest_final('muaban'))"
```

Expected output, in order:
- a line `[muaban] Processing N raw items through pipeline...`
- `WROTE ...\data\final\muaban\2026-05-11\HHMMSS.json`
- `source= muaban total= <int> keys= ['avg_quality', 'listings', 'phone_full', 'processed_at', 'source', 'total']`
- `latest= ...\data\final\muaban\2026-05-11\HHMMSS.json` (same file)

- [ ] **Step 3: Run the full test suite**

Run: `python -m pytest -q`
Expected: `5 passed` (the `tests/test_config_paths.py` tests; no other tests in repo).

- [ ] **Step 4: Confirm no leftover references to removed names**

Run: `git grep -nE "clean_path|find_latest_clean|run_merge" -- '*.py'`
Expected: no matches (exit 1). If `merge_pipeline.py`'s own `run_merge` definition shows up, that is fine — only *imports/uses of it from `run.py`* should be gone. (Run `git grep -n "from merge_pipeline import"` — expected: no matches.)

- [ ] **Step 5: Final commit (cleanup, if anything uncommitted)**

```bash
git status
git add -A
git commit -m "chore: verify split-sources pipeline end-to-end" || echo "nothing to commit"
```

(The smoke test writes a file under `data/final/muaban/` which is gitignored, so `git status` should be clean — this step is a safety net only.)

---

## Done criteria

- `python run.py --help` works; `import run` has no dangling references.
- `python -m pytest -q` → all green.
- A muaban raw file processed through `run_pipeline_for_source` lands at `data/final/muaban/{date}/{HHMMSS}.json` with keys `source, total, phone_full, avg_quality, processed_at, listings`.
- `data/clean/` is never written by the pipeline anymore; `data/final/` is per-source.
- `merge_pipeline.py` still imports cleanly and (if run manually) writes to `data/final/merged/...`.
- `CLAUDE.md` Layout + Architecture sections match the new flow.
