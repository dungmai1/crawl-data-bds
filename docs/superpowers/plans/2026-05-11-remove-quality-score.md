# Remove `quality_score` Logic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete the quality-score concept from the scraper — the `calc_quality_score()` functions, the `quality_score` field on `PropertyDTO`, all quality-based sorting, and every `avg_quality` / quality-distribution statistic and log line.

**Architecture:** Pure subtractive refactor across `pipeline/unified_pipeline.py`, `scrapers/nhatot_fast_scraper.py`, `run.py`, `scripts/generate_seed_sql.py`, `pipeline/merge_pipeline.py`, plus `CLAUDE.md` / `README.md`. `unified_pipeline.py` and `nhatot_fast_scraper.py` are coupled (the latter imports `calc_quality_score`), so they change together in Task 1. No behavior changes beyond removing the field and stats; output JSON drops `quality_score` on listings and the `avg_quality` summary key; `PropertyDTO` goes 35 → 34 fields.

**Tech Stack:** Python 3.11, dataclasses, pytest.

**Reference spec:** `docs/superpowers/specs/2026-05-11-remove-quality-score-design.md`

---

## File Structure

| File | Change |
|------|--------|
| `pipeline/unified_pipeline.py` | Remove `quality_score` field, `calc_quality_score()`, Step 5 in `process_batch`, sort-by-quality, `avg_quality`/distribution stats in `run_pipeline`, docstring/comment updates |
| `scrapers/nhatot_fast_scraper.py` | Drop `calc_quality_score` import + re-score call, `avg_quality` stat/output-key/log |
| `run.py` | Drop `avg_quality` calc + output key in `_write_final()`, trim summary log line |
| `scripts/generate_seed_sql.py` | `trust_score` value → literal `NULL` |
| `pipeline/merge_pipeline.py` | Remove `calc_quality_score()`, STEP 3 rescore, `avg_quality`/distribution stats, docstring/step renumber |
| `CLAUDE.md` | Drop "Quality Score" from pipeline-flow lines; "35 fields" → "34 fields" |
| `README.md` | Drop `→ Quality Score` step; "35 fields" → "34 fields" |

---

## Task 1: Remove quality_score from `unified_pipeline.py` + `nhatot_fast_scraper.py`

**Files:**
- Modify: `pipeline/unified_pipeline.py` (docstring lines ~1-11; comment line ~26; field line ~91; section header line ~552; `process_batch` lines ~651-657; `run_pipeline` lines ~681-713)
- Modify: `scrapers/nhatot_fast_scraper.py` (import line ~39; phone-fill loop lines ~586-590; stats/output lines ~597-617)
- Test: `tests/` (existing suite — no new test file)

- [ ] **Step 1: Edit `pipeline/unified_pipeline.py` — module docstring**

Replace the top docstring block:

```python
"""
V-Nexus Unified Data Pipeline
Chuẩn hóa data từ TẤT CẢ sources (nhatot, muaban, facebook) vào 1 DTO duy nhất.

Pipeline:
  Raw JSON → Source Adapter → Unified DTO → Address Mapping (34 tỉnh) →
  Price Validation → Property Classification → Broker Detection →
  Quality Score → Clean JSON / DB Insert

Output DTO fields: 35 fields chuẩn, format thống nhất
"""
```

with:

```python
"""
V-Nexus Unified Data Pipeline
Chuẩn hóa data từ TẤT CẢ sources (nhatot, muaban, facebook) vào 1 DTO duy nhất.

Pipeline:
  Raw JSON → Source Adapter → Unified DTO → Address Mapping (34 tỉnh) →
  Price Validation → Property Classification → Broker Detection →
  Clean JSON / DB Insert

Output DTO fields: 34 fields chuẩn, format thống nhất
"""
```

- [ ] **Step 2: Edit `pipeline/unified_pipeline.py` — DTO section comment + remove field**

Change the comment:

```python
# ============================================================
# UNIFIED DTO — 35 fields chuẩn cho mọi source
# ============================================================
```

to:

```python
# ============================================================
# UNIFIED DTO — 34 fields chuẩn cho mọi source
# ============================================================
```

And in the `PropertyDTO` dataclass, delete this line (it's the last field, under `# Metadata`):

```python
    quality_score: int = 0               # 0-100
```

So the `# Metadata` block becomes just:

```python
    # Metadata
    posted_at: Optional[str] = None      # ISO 8601
    scraped_at: str = ""                 # ISO 8601
```

- [ ] **Step 3: Edit `pipeline/unified_pipeline.py` — remove `calc_quality_score()` and rename section header**

Change:

```python
# ============================================================
# VALIDATION + QUALITY SCORE
# ============================================================
```

to:

```python
# ============================================================
# VALIDATION
# ============================================================
```

And delete the entire function (and the blank lines that padded it):

```python
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
```

- [ ] **Step 4: Edit `pipeline/unified_pipeline.py` — `process_batch()` remove Step 5 + sort**

Change the tail of `process_batch`:

```python
        # Step 4: Classify poster
        classify_poster(dto, raw)

        # Step 5: Quality score
        dto.quality_score = calc_quality_score(dto)

        results.append(dto)

    # Sort by quality
    results.sort(key=lambda x: x.quality_score, reverse=True)

    return results
```

to:

```python
        # Step 4: Classify poster
        classify_poster(dto, raw)

        results.append(dto)

    return results
```

- [ ] **Step 5: Edit `pipeline/unified_pipeline.py` — `run_pipeline()` remove quality stats**

Change:

```python
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
```

to:

```python
    # Stats
    total = len(results)
    has_phone = sum(1 for r in results if r.phone_full)

    log.info(f"\n{'='*60}")
    log.info(f"  PIPELINE RESULTS — {source}")
    log.info(f"{'='*60}")
    log.info(f"  Total:        {total}")
    log.info(f"  Full phone:   {has_phone} ({has_phone/max(total,1)*100:.0f}%)")
```

And in the `json.dump(...)` call at the end of `run_pipeline`, remove the `"avg_quality"` line:

```python
        json.dump({
            "source": source,
            "total": total,
            "full_phone": has_phone,
            "avg_quality": round(avg_quality, 1),
            "processed_at": datetime.now().isoformat(),
            "listings": [asdict(r) for r in results],
        }, f, ensure_ascii=False, indent=2)
```

becomes:

```python
        json.dump({
            "source": source,
            "total": total,
            "full_phone": has_phone,
            "processed_at": datetime.now().isoformat(),
            "listings": [asdict(r) for r in results],
        }, f, ensure_ascii=False, indent=2)
```

- [ ] **Step 6: Edit `scrapers/nhatot_fast_scraper.py` — drop import**

Change:

```python
from unified_pipeline import process_batch, AddressMapper, calc_quality_score
```

to:

```python
from unified_pipeline import process_batch, AddressMapper
```

- [ ] **Step 7: Edit `scrapers/nhatot_fast_scraper.py` — phone-fill loop**

Change:

```python
        for dto in results:
            if not dto.phone_full:
                dto.phone_full = phone_lookup.get(dto.source_id)
                if dto.phone_full:
                    dto.quality_score = calc_quality_score(dto)
```

to:

```python
        for dto in results:
            if not dto.phone_full:
                dto.phone_full = phone_lookup.get(dto.source_id)
```

- [ ] **Step 8: Edit `scrapers/nhatot_fast_scraper.py` — stats/output/log**

Change:

```python
    phones_found = sum(1 for r in results if r.phone_full)
    avg_quality = sum(r.quality_score for r in results) / max(len(results), 1)

    output = {
        "source": "nhatot",
        "total": len(results),
        "full_phone": phones_found,
        "avg_quality": round(avg_quality, 1),
        "cycle_time_sec": int(time.time() - start),
        "processed_at": datetime.now().isoformat(),
        "listings": [asdict(r) for r in results],
    }
```

to:

```python
    phones_found = sum(1 for r in results if r.phone_full)

    output = {
        "source": "nhatot",
        "total": len(results),
        "full_phone": phones_found,
        "cycle_time_sec": int(time.time() - start),
        "processed_at": datetime.now().isoformat(),
        "listings": [asdict(r) for r in results],
    }
```

And change:

```python
    log.info(f"  Total: {len(results)} | Phones: {phones_found}")
    log.info(f"  Avg quality: {avg_quality:.0f}/100")
    log.info(f"  Saved: {output_file}")
```

to:

```python
    log.info(f"  Total: {len(results)} | Phones: {phones_found}")
    log.info(f"  Saved: {output_file}")
```

- [ ] **Step 9: Verify no `quality` references remain in these two files**

Run: `python -c "import pathlib,re; [print(p, i+1, l.rstrip()) for p in ['pipeline/unified_pipeline.py','scrapers/nhatot_fast_scraper.py'] for i,l in enumerate(pathlib.Path(p).read_text(encoding='utf-8').splitlines()) if re.search('quality', l, re.I)]"`
Expected: no output (empty).

- [ ] **Step 10: Verify `PropertyDTO` is 34 fields and `calc_quality_score` is gone**

Run: `python -c "import sys; sys.path.insert(0,'pipeline'); import dataclasses; from unified_pipeline import PropertyDTO; print('FIELDS', len(dataclasses.fields(PropertyDTO)))"`
Expected: `FIELDS 34`

Run: `python -c "import sys; sys.path.insert(0,'pipeline'); import unified_pipeline; print(hasattr(unified_pipeline,'calc_quality_score'))"`
Expected: `False`

- [ ] **Step 11: Run the test suite**

Run: `pytest -q`
Expected: all tests pass (4 test files, none reference quality).

- [ ] **Step 12: Commit**

```bash
git add pipeline/unified_pipeline.py scrapers/nhatot_fast_scraper.py
git commit -m "refactor(pipeline): drop quality_score field and calc_quality_score

PropertyDTO is now 34 fields. nhatot scraper no longer imports or
re-scores quality. Output JSON loses quality_score on listings and
the avg_quality summary key.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Remove `avg_quality` from `run.py`

**Files:**
- Modify: `run.py` (`_write_final()` lines ~65-78; summary log line ~199)

- [ ] **Step 1: Edit `_write_final()`**

Change:

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
```

to:

```python
def _write_final(out: str, source: str, listings: list) -> None:
    """Write per-source final JSON with summary stats."""
    total = len(listings)
    phone_full = sum(1 for x in listings if x.get("phone_full"))
    with open(out, "w", encoding="utf-8") as f:
        json.dump({
            "source": source,
            "total": total,
            "phone_full": phone_full,
            "processed_at": datetime.now().isoformat(),
            "listings": listings,
        }, f, ensure_ascii=False, indent=2)
```

- [ ] **Step 2: Edit the cycle-summary log line**

Change:

```python
        log.info(
            f"  [{src}] total={d.get('total', 0)}  "
            f"phone_full={d.get('phone_full', 0)}  avg_quality={d.get('avg_quality', 0)}"
        )
```

to:

```python
        log.info(
            f"  [{src}] total={d.get('total', 0)}  "
            f"phone_full={d.get('phone_full', 0)}"
        )
```

- [ ] **Step 3: Verify no `quality` references remain**

Run: `python -c "import pathlib,re; [print(i+1, l.rstrip()) for i,l in enumerate(pathlib.Path('run.py').read_text(encoding='utf-8').splitlines()) if re.search('quality', l, re.I)]"`
Expected: no output.

- [ ] **Step 4: Syntax check**

Run: `python -c "import ast; ast.parse(open('run.py',encoding='utf-8').read()); print('ok')"`
Expected: `ok`

- [ ] **Step 5: Commit**

```bash
git add run.py
git commit -m "refactor(run): drop avg_quality from per-source summary

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `trust_score` → NULL in `scripts/generate_seed_sql.py`

**Files:**
- Modify: `scripts/generate_seed_sql.py` (INSERT VALUES line ~208)

- [ ] **Step 1: Edit the `trust_score` value**

Change (inside the `out.append("INSERT INTO listings (...) VALUES (...)")` block):

```python
            "'ACTIVE', "
            f"{sql_num(x.get('quality_score'))}, "
            f"{sql_str(listing_types)}, "
```

to:

```python
            "'ACTIVE', "
            "NULL, "  # trust_score — scraper no longer computes a quality score
            f"{sql_str(listing_types)}, "
```

- [ ] **Step 2: Verify no `quality` references remain**

Run: `python -c "import pathlib,re; [print(i+1, l.rstrip()) for i,l in enumerate(pathlib.Path('scripts/generate_seed_sql.py').read_text(encoding='utf-8').splitlines()) if re.search('quality', l, re.I)]"`
Expected: no output.

- [ ] **Step 3: Syntax check**

Run: `python -c "import ast; ast.parse(open('scripts/generate_seed_sql.py',encoding='utf-8').read()); print('ok')"`
Expected: `ok`

- [ ] **Step 4: Commit**

```bash
git add scripts/generate_seed_sql.py
git commit -m "refactor(seed-sql): leave listings.trust_score NULL (no quality_score)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Strip quality logic from `pipeline/merge_pipeline.py`

**Files:**
- Modify: `pipeline/merge_pipeline.py` (docstring lines ~1-20; `calc_quality_score` lines ~159-185; STEP 3 lines ~239-245; STEP 4 stats lines ~247-308)

- [ ] **Step 1: Edit the module docstring strategy list**

Change:

```python
Strategy:
  1. Load clean data từ cả 3 sources (đã qua unified_pipeline)
  2. Cross-reference: match cùng 1 BĐS trên nhiều platform
     Match criteria: same ward + price ±15% + area ±10%
  3. Merge: lấy field mạnh nhất từ mỗi source
     - nhatot: lat/lng, street, direction, legal_document
     - muaban: phone_full
     - merge: cross-reference for phone + GPS + legal enrichment
  4. Dedup: giữ record merged, loại bỏ duplicate
  5. Quality rescore: tính lại quality sau merge
  6. Output: 1 JSON file — data sạch, đầy đủ nhất, sẵn sàng INSERT DB
```

to:

```python
Strategy:
  1. Load clean data từ cả 3 sources (đã qua unified_pipeline)
  2. Cross-reference: match cùng 1 BĐS trên nhiều platform
     Match criteria: same ward + price ±15% + area ±10%
  3. Merge: lấy field mạnh nhất từ mỗi source
     - nhatot: lat/lng, street, direction, legal_document
     - muaban: phone_full
     - merge: cross-reference for phone + GPS + legal enrichment
  4. Dedup: giữ record merged, loại bỏ duplicate
  5. Output: 1 JSON file — data sạch, đầy đủ nhất, sẵn sàng INSERT DB
```

- [ ] **Step 2: Delete `calc_quality_score()`**

Delete the whole function (between `merge_two_items`'s `return merged` and `def run_merge(...)`):

```python
def calc_quality_score(item: dict) -> int:
    """Recalculate quality score after merge."""
    s = 0
    checks = [
        (item.get("title"), 8),
        (item.get("price"), 12),
        (item.get("area"), 12),
        (item.get("province"), 8),
        (item.get("ward"), 5),
        (item.get("street"), 5),
        (item.get("full_address"), 3),
        (item.get("lat") and item.get("lng"), 8),
        (item.get("images") and len(item.get("images", [])) > 0, 4),
        (item.get("description") and len(item.get("description", "")) > 50, 4),
        (item.get("bedrooms"), 3),
        (item.get("bathrooms"), 3),
        (item.get("floors"), 2),
        (item.get("direction"), 3),
        (item.get("legal_document"), 3),
        (item.get("phone_full"), 10),
        (item.get("contact_name"), 2),
        (item.get("property_type") and item["property_type"] != "loai-bds-khac", 3),
        (item.get("poster_type") and item["poster_type"] != "khong_xac_dinh", 2),
    ]
    for val, pts in checks:
        if val: s += pts
    return min(s, 100)
```

(Leave exactly one blank line between `merge_two_items`'s end and `def run_merge`.)

- [ ] **Step 3: Remove STEP 3 (rescore + sort), renumber STEP 4 → STEP 3**

Change:

```python
    all_items = merged_items + unmatched_nt + unmatched_mb
    log.info(f"  Total before dedup: {len(all_items)}")

    # === STEP 3: Recalculate quality scores ===
    log.info("\n--- STEP 3: Quality rescore ---")
    for item in all_items:
        item["quality_score"] = calc_quality_score(item)

    # Sort by quality descending
    all_items.sort(key=lambda x: x["quality_score"], reverse=True)

    # === STEP 4: Final stats ===
    total = len(all_items)
    has_phone = sum(1 for i in all_items if i.get("phone_full"))
    has_gps = sum(1 for i in all_items if i.get("lat") and i.get("lng"))
    has_street = sum(1 for i in all_items if i.get("street"))
    has_legal = sum(1 for i in all_items if i.get("legal_document"))
    has_direction = sum(1 for i in all_items if i.get("direction"))
    merged_count = sum(1 for i in all_items if i.get("_merged_from"))
    avg_quality = sum(i["quality_score"] for i in all_items) / max(total, 1)

    # Quality distribution
    excellent = sum(1 for i in all_items if i["quality_score"] >= 80)
    good = sum(1 for i in all_items if 60 <= i["quality_score"] < 80)
    fair = sum(1 for i in all_items if 40 <= i["quality_score"] < 60)
    poor = sum(1 for i in all_items if i["quality_score"] < 40)

    # Source distribution
    from collections import Counter
    source_dist = Counter(i.get("source") for i in all_items)
```

to:

```python
    all_items = merged_items + unmatched_nt + unmatched_mb
    log.info(f"  Total before dedup: {len(all_items)}")

    # === STEP 3: Final stats ===
    total = len(all_items)
    has_phone = sum(1 for i in all_items if i.get("phone_full"))
    has_gps = sum(1 for i in all_items if i.get("lat") and i.get("lng"))
    has_street = sum(1 for i in all_items if i.get("street"))
    has_legal = sum(1 for i in all_items if i.get("legal_document"))
    has_direction = sum(1 for i in all_items if i.get("direction"))
    merged_count = sum(1 for i in all_items if i.get("_merged_from"))

    # Source distribution
    from collections import Counter
    source_dist = Counter(i.get("source") for i in all_items)
```

- [ ] **Step 4: Remove the `QUALITY:` log block**

Change:

```python
    log.info(f"    Direction:      {has_direction:>6} ({has_direction/total*100:>5.1f}%)")
    log.info(f"")
    log.info(f"  QUALITY:")
    log.info(f"    Average:    {avg_quality:.0f}/100")
    log.info(f"    Excellent:  {excellent}")
    log.info(f"    Good:       {good}")
    log.info(f"    Fair:       {fair}")
    log.info(f"    Poor:       {poor}")
    log.info(f"")
    log.info(f"  BY SOURCE:")
```

to:

```python
    log.info(f"    Direction:      {has_direction:>6} ({has_direction/total*100:>5.1f}%)")
    log.info(f"")
    log.info(f"  BY SOURCE:")
```

- [ ] **Step 5: Remove `"avg_quality"` from the output dict**

Change:

```python
        json.dump({
            "total": total,
            "merged_cross_platform": merged_count,
            "phone_full": has_phone,
            "gps_coverage": has_gps,
            "avg_quality": round(avg_quality, 1),
            "sources": dict(source_dist),
            "processed_at": datetime.now().isoformat(),
            "listings": all_items,
        }, f, ensure_ascii=False, indent=2)
```

to:

```python
        json.dump({
            "total": total,
            "merged_cross_platform": merged_count,
            "phone_full": has_phone,
            "gps_coverage": has_gps,
            "sources": dict(source_dist),
            "processed_at": datetime.now().isoformat(),
            "listings": all_items,
        }, f, ensure_ascii=False, indent=2)
```

- [ ] **Step 6: Verify no `quality` references remain**

Run: `python -c "import pathlib,re; [print(i+1, l.rstrip()) for i,l in enumerate(pathlib.Path('pipeline/merge_pipeline.py').read_text(encoding='utf-8').splitlines()) if re.search('quality', l, re.I)]"`
Expected: no output.

- [ ] **Step 7: Syntax check**

Run: `python -c "import ast; ast.parse(open('pipeline/merge_pipeline.py',encoding='utf-8').read()); print('ok')"`
Expected: `ok`

- [ ] **Step 8: Commit**

```bash
git add pipeline/merge_pipeline.py
git commit -m "refactor(merge): drop quality rescore step and stats

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Update docs (`CLAUDE.md`, `README.md`)

**Files:**
- Modify: `CLAUDE.md` (layout comment line ~22; pipeline-flow line ~56; data-schema line ~70)
- Modify: `README.md` (pipeline diagram lines ~40-51)

- [ ] **Step 1: Edit `CLAUDE.md`**

Change `│   ├── unified_pipeline.py      # Raw → PropertyDTO (35 fields)` to `│   ├── unified_pipeline.py      # Raw → PropertyDTO (34 fields)`.

Change:

```
**Pipeline flow (per source):** Raw JSON → Source Adapter → Address Mapping → Price Validation → Property Classification → Broker Detection → Quality Score → `PropertyDTO` → `data/final/{source}/...`
```

to:

```
**Pipeline flow (per source):** Raw JSON → Source Adapter → Address Mapping → Price Validation → Property Classification → Broker Detection → `PropertyDTO` → `data/final/{source}/...`
```

Change `Output conforms to `PropertyDTO` (35 fields).` to `Output conforms to `PropertyDTO` (34 fields).` (leave the rest of that sentence unchanged).

- [ ] **Step 2: Edit `README.md` pipeline diagram**

Change:

```
Raw JSON
  → Source Adapter
  → Address Mapping (province/ward/street normalize)
  → Price Validation
  → Property Classification
  → Broker Detection
  → Quality Score
  → PropertyDTO (35 fields)
  → Cross-source merge (ward + price ±15% + area ±10%)
  → Final merged file
```

to:

```
Raw JSON
  → Source Adapter
  → Address Mapping (province/ward/street normalize)
  → Price Validation
  → Property Classification
  → Broker Detection
  → PropertyDTO (34 fields)
  → Cross-source merge (ward + price ±15% + area ±10%)
  → Final merged file
```

- [ ] **Step 3: Verify no stray `quality` mentions in docs**

Run: `python -c "import pathlib,re; [print(f,i+1,l.rstrip()) for f in ['CLAUDE.md','README.md'] for i,l in enumerate(pathlib.Path(f).read_text(encoding='utf-8').splitlines()) if re.search('quality', l, re.I)]"`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: drop Quality Score from pipeline flow, PropertyDTO now 34 fields

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Final repo-wide verification

**Files:** none (verification only)

- [ ] **Step 1: Repo-wide grep for `quality_score`**

Run: `git grep -i quality_score -- 'pipeline/*.py' 'scrapers/*.py' 'scripts/*.py' 'run.py' 'tests/*.py' 'CLAUDE.md' 'README.md'`
Expected: no output (exit code 1).

- [ ] **Step 2: Repo-wide grep for `quality` (catches stray comments/logs)**

Run: `git grep -i quality -- 'pipeline/*.py' 'scrapers/*.py' 'scripts/*.py' 'run.py' 'tests/*.py' 'CLAUDE.md' 'README.md'`
Expected: no output (exit code 1).

- [ ] **Step 3: Full test suite**

Run: `pytest -q`
Expected: all tests pass.

- [ ] **Step 4: Import + field-count smoke checks**

Run: `python -c "import sys; sys.path.insert(0,'pipeline'); import dataclasses; from unified_pipeline import PropertyDTO; assert len(dataclasses.fields(PropertyDTO)) == 34; import unified_pipeline; assert not hasattr(unified_pipeline,'calc_quality_score'); print('OK')"`
Expected: `OK`

- [ ] **Step 5: Compile-all check**

Run: `python -m compileall -q pipeline scrapers scripts run.py`
Expected: no errors.

(No commit — this task only verifies the prior work.)

---

## Notes

- **Out of scope:** `data/sample/2026-04-14_merged.json` (committed historical snapshot, still has old `quality_score` keys) and the `2026-05-11-split-sources-drop-clean` plan/spec docs — left untouched. Backend `listings.trust_score` column lives in the Spring Boot repo and is not touched here.
- Windows note: shell is PowerShell; the `python -c "..."` verification one-liners are shell-agnostic. `git grep` works the same.
