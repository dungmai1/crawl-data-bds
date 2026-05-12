# Remove `quality_score` Logic — Design

**Date:** 2026-05-11
**Status:** Approved

## Goal

Eliminate the quality-score concept from the scraper entirely:

- Delete the `calc_quality_score()` functions (`unified_pipeline.py` and `merge_pipeline.py`).
- Delete the `quality_score` field on `PropertyDTO` (35 → 34 fields).
- Remove all quality-based sorting of results.
- Remove every `avg_quality` / quality-distribution statistic and log line.
- Output JSON no longer carries `quality_score` on listings, nor the `avg_quality` summary key.

No other behavior changes: the pipeline still adapts, address-maps, validates price, and classifies posters exactly as before — it just stops scoring and stops sorting.

## Changes by file

### 1. `pipeline/unified_pipeline.py`

- **Module docstring:** drop `Quality Score →` from the pipeline-step list; change "35 fields" to "34 fields".
- **`PropertyDTO`:** delete the `quality_score: int = 0` field; update the "35 fields chuẩn" comment to "34 fields".
- **`calc_quality_score()`:** delete the function entirely.
- **Section header:** rename `# VALIDATION + QUALITY SCORE` to `# VALIDATION`.
- **`process_batch()`:** delete the "Step 5: Quality score" block (`dto.quality_score = calc_quality_score(dto)`) and the `results.sort(key=lambda x: x.quality_score, reverse=True)` line. Listings remain in source order.
- **`run_pipeline()`:** remove the `avg_quality` computation, the `Avg quality: …/100` log line, the Excellent/Good/Fair/Poor quality-distribution block, and the `"avg_quality"` key from the output dict.

### 2. `scrapers/nhatot_fast_scraper.py`

- **Import (line ~39):** remove `calc_quality_score` from `from unified_pipeline import …`.
- **Phone-fill loop (~586–590):** keep `dto.phone_full = phone_lookup.get(dto.source_id)`; delete the `dto.quality_score = calc_quality_score(dto)` re-score line.
- **Stats/output:** remove the `avg_quality` computation, the `"avg_quality"` key in the output dict, and the `Avg quality: …/100` log line.

### 3. `run.py`

- **`_write_final()`:** remove the `avg_quality` computation and the `"avg_quality"` key from the JSON written.
- **Summary log:** drop the `avg_quality={…}` segment from the per-source summary `log.info` f-string.

### 4. `scripts/generate_seed_sql.py`

- **INSERT VALUES (line ~208):** replace `f"{sql_num(x.get('quality_score'))}, "` with the literal `"NULL, "`. Keep `trust_score` in the column list so the database default / NULL applies. Update the adjacent comment to note `trust_score` is now left NULL by the seed script.

### 5. `pipeline/merge_pipeline.py` (manual-only script, not in `run.py`)

- **Docstring:** remove strategy step "5. Quality rescore: tính lại quality sau merge"; renumber "6. Output" → "5. Output".
- **`calc_quality_score(item: dict)`:** delete the function.
- **"STEP 3: Recalculate quality scores":** delete the rescore loop and the `all_items.sort(key=lambda x: x["quality_score"], reverse=True)` line. Renumber the subsequent "STEP 4: Final stats" → "STEP 3: Final stats".
- **Stats/output:** remove `avg_quality`, the `excellent/good/fair/poor` counts, the `QUALITY:` log block, and the `"avg_quality"` key from the output dict.

### 6. Docs

- **`CLAUDE.md`:** remove "Quality Score" from the pipeline-flow line; change "PropertyDTO (35 fields)" to "(34 fields)" wherever it appears; drop `Quality Score →` from the architecture "Pipeline flow" line.
- **`README.md` (line ~47):** remove the `→ Quality Score` step from the pipeline diagram.

## Out of scope

- `data/sample/2026-04-14_merged.json` — committed historical snapshot; left untouched (it still contains the old `quality_score` keys).
- `docs/superpowers/plans/2026-05-11-split-sources-drop-clean.md` and `docs/superpowers/specs/2026-05-11-split-sources-drop-clean-design.md` — historical documents; left untouched.
- Backend (`listings.trust_score` column) — owned by the Spring Boot repo; not modified here. The seed script just stops populating it.

## Testing / verification

1. `pytest` — the existing test files (`test_config_paths.py`, `test_image_adaptation.py`, `test_muaban_geo_extract.py`, `test_muaban_field_mapping.py`) do not reference quality; they must stay green.
2. Field-count smoke check:
   ```
   python -c "import dataclasses; from pipeline.unified_pipeline import PropertyDTO; print(len(dataclasses.fields(PropertyDTO)))"
   ```
   expect `34`.
3. Import smoke check: `from pipeline.unified_pipeline import calc_quality_score` must now raise `ImportError`.
4. `grep -ri quality_score pipeline/ scrapers/ scripts/ run.py` returns nothing; `grep -ri quality pipeline/ scrapers/ scripts/ run.py` returns nothing.
5. (Optional, if sample raw data is available) run `python run.py --nhatot-only` and confirm the produced `data/final/nhatot/.../*.json` has no `quality_score` on listings and no `avg_quality` summary key.
