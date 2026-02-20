# Session Coordination Hub

**Project:** VoltMarket Transaction Intelligence Engine
**Repo:** https://github.com/nicolasc-yuno/voltmarket-transaction-intelligence
**Constraint:** 2 hours total

## Data Contracts

All schemas are defined in `src/contracts/schemas.py`. This is the **single source of truth**.

| Layer | File | Schema | Producer | Consumer |
|-------|------|--------|----------|----------|
| Raw | `data/raw/transactions.parquet` | `RAW_TRANSACTION_SCHEMA` | Session A | Session B |
| Segments | `data/processed/segments.parquet` | `SEGMENT_SCHEMA` | Session B | Session C |
| Trends | `data/processed/weekly_trends.parquet` | `WEEKLY_TREND_SCHEMA` | Session B | Session C, D |
| Insights | `data/analytics/insights.json` | `INSIGHT_SCHEMA` | Session C | Session D |
| Anomalies | `data/analytics/anomalies.parquet` | `ANOMALY_SCHEMA` | Session C | Session D |

**Rule:** If you need to change a schema, update `src/contracts/schemas.py` AND this file. Commit and push immediately so other sessions can pull.

## Session Status Board

Update your section when you start, make progress, or finish. **Always commit + push after updating.**

### Session A: Data Generation
- **Branch:** `feature/data-generator`
- **Status:** COMPLETE
- **Files:** `src/data_generator/generate.py`
- **Output:** `data/raw/transactions.parquet`
- **Notes:** 8,000 transactions across 6 weeks. Weeks 1-3: ~81% approval, Weeks 4-6: ~62%. Three patterns embedded: BBVA MX (82%→38%), High-value >$200 (79%→56%), Post-8PM (77%→56%). Parquet pushed to feature/data-generator branch (force-added, bypasses .gitignore). Run: `python -m src.data_generator.generate`

### Session B: Pipeline
- **Branch:** `feature/pipeline`
- **Status:** COMPLETE
- **Files:** `src/pipeline/ingest.py`, `src/pipeline/transform.py`, `src/pipeline/segment.py`, `src/pipeline/__main__.py`
- **Output:** `data/processed/segments.parquet`, `data/processed/weekly_trends.parquet`
- **Notes:** 11 segment types × (6 weeks + 2 half-periods + 1 overall) = 2,652 segment rows. Both outputs match SEGMENT_SCHEMA and WEEKLY_TREND_SCHEMA exactly. Reads real 8,000-row data from Session A. CLI: `python -m src.pipeline`. Approval story confirmed: weeks 1-3 ~81%, weeks 4-6 ~62%.

### Session C: Analytics
- **Branch:** `feature/analytics`
- **Status:** COMPLETE
- **Files:** `src/analytics/anomaly_detection.py`, `src/analytics/insights.py`, `src/analytics/__main__.py`
- **Output:** `data/analytics/insights.json`, `data/analytics/insights.parquet`, `data/analytics/anomalies.parquet`, `data/analytics/summary.json`
- **Notes:** Deduplicates segment_keys across segment_types before z-score computation. Falls back to mock data if pipeline output missing. BBVA Mexico ranks #1 (~$181K monthly impact, 41pp drop). `python -m src.analytics` works. All schemas match contracts.

### Session D: Visualization
- **Branch:** `feature/visualization`
- **Status:** NOT STARTED
- **Files:** `src/visualization/dashboard.py`
- **Output:** `output/` (HTML dashboard, PNG charts)
- **Notes:**

## Integration Checklist

- [x] Data generator produces valid parquet matching `RAW_TRANSACTION_SCHEMA`
- [x] Pipeline reads raw data and outputs segments + trends
- [x] Analytics reads segments and outputs ranked insights + anomalies
- [ ] Visualization reads all layers and produces dashboard
- [ ] CLI entrypoint runs full pipeline end-to-end
- [ ] README complete with setup, architecture, findings

## Blockers / Decisions Log

| Date | Session | Issue | Resolution |
|------|---------|-------|------------|
| | | | |

## How to Sync

```bash
# Before starting work
git checkout main && git pull
git checkout your-branch && git merge main

# After completing a milestone
git add -A && git commit -m "Session X: description"
git push origin your-branch

# To update COORDINATION.md (checkout main briefly)
git stash
git checkout main && git pull
# Update your session status in COORDINATION.md
git add COORDINATION.md && git commit -m "Update Session X status" && git push
git checkout your-branch && git stash pop
```
