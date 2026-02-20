# Session Prompts

Copy-paste these into new Claude Code sessions to start parallel work.

---

## Session A: Data Generator (Sonnet)

```
I'm working on the VoltMarket Transaction Intelligence Engine challenge. This session is SESSION A: Data Generation.

Repo: ~/Code/voltmarket-transaction-intelligence
Branch: feature/data-generator

1. cd into the repo, checkout `feature/data-generator`, activate the venv: `source venv/bin/activate`
2. Read `PLAN_A_DATA_GENERATOR.md` for your full instructions
3. Read `src/contracts/schemas.py` for data contracts — you MUST use these schemas
4. Read `CLAUDE.md` for project conventions

Your job: Build `src/data_generator/generate.py` that creates ~8,000 synthetic transactions with three embedded degradation patterns (BBVA Mexico collapse, high-value degradation, evening underperformance). The overall approval rate must trend from 82% → 64% across 6 weeks.

Output: `data/raw/transactions.parquet`

When done: commit, push to `feature/data-generator`, and update your status in COORDINATION.md on master. Also push the generated parquet file so other sessions can use it.
```

---

## Session B: Pipeline (Sonnet)

```
I'm working on the VoltMarket Transaction Intelligence Engine challenge. This session is SESSION B: Pipeline.

Repo: ~/Code/voltmarket-transaction-intelligence
Branch: feature/pipeline

1. cd into the repo, checkout `feature/pipeline`, activate the venv: `source venv/bin/activate`
2. Read `PLAN_B_PIPELINE.md` for your full instructions
3. Read `src/contracts/schemas.py` for data contracts — you MUST use these schemas
4. Read `CLAUDE.md` for project conventions

Your job: Build the Polars-based segmentation pipeline in `src/pipeline/`. Three files:
- `ingest.py`: Read raw parquet, validate, add derived columns (amount_bucket, hour_bucket, period_half)
- `segment.py`: Calculate approval rates across 10+ dimension combinations
- `transform.py`: Produce weekly trend rollups

If `data/raw/transactions.parquet` doesn't exist yet (Session A may still be running), create a mock dataset of ~200 rows for development. Build everything against the contracts, not the data.

Output: `data/processed/segments.parquet` and `data/processed/weekly_trends.parquet`

When done: commit, push to `feature/pipeline`, and update your status in COORDINATION.md on master.
```

---

## Session C: Analytics (Sonnet)

```
I'm working on the VoltMarket Transaction Intelligence Engine challenge. This session is SESSION C: Analytics & Anomaly Detection.

Repo: ~/Code/voltmarket-transaction-intelligence
Branch: feature/analytics

1. cd into the repo, checkout `feature/analytics`, activate the venv: `source venv/bin/activate`
2. Read `PLAN_C_ANALYTICS.md` for your full instructions
3. Read `src/contracts/schemas.py` for data contracts — you MUST use these schemas
4. Read `CLAUDE.md` for project conventions

Your job: Build anomaly detection and insight generation in `src/analytics/`. Two files:
- `anomaly_detection.py`: Compare weeks 1-3 vs 4-6 for every segment, calculate z-scores, p-values, flag anomalies, estimate revenue impact
- `insights.py`: Rank top 3-5 insights by weighted score (impact 40%, magnitude 30%, significance 20%, breadth 10%), generate titles and descriptions

If `data/processed/segments.parquet` doesn't exist yet (Session B may still be running), create mock segment data for development. Build everything against the contracts.

Output: `data/analytics/insights.json`, `data/analytics/anomalies.parquet`, `data/analytics/summary.json`

When done: commit, push to `feature/analytics`, and update your status in COORDINATION.md on master.
```

---

## Session D: Visualization (Opus)

```
I'm working on the VoltMarket Transaction Intelligence Engine challenge. This session is SESSION D: Visualization & Dashboard.

Repo: ~/Code/voltmarket-transaction-intelligence
Branch: feature/visualization

1. cd into the repo, checkout `feature/visualization`, activate the venv: `source venv/bin/activate`
2. Read `PLAN_D_VISUALIZATION.md` for your full instructions
3. Read `src/contracts/schemas.py` for data contracts — you MUST use these schemas
4. Read `CLAUDE.md` for project conventions

Your job: Build the visual intelligence dashboard in `src/visualization/`. Two files:
- `charts.py`: 6 Plotly chart functions (headline trend, country breakdown, issuer heatmap, waterfall, amount distribution, hour-of-day pattern)
- `dashboard.py`: Streamlit app assembling all charts + insight cards into a polished single-page layout

Design for non-technical payment ops managers. Use color coding (green/yellow/red for approval rates), clear annotations, and "aha moment" callouts.

If upstream data doesn't exist yet, create mock DataFrames that match the contracts for development. Everything must work with mock data.

Also export static PNGs and a standalone HTML dashboard to `output/`.

When done: commit, push to `feature/visualization`, and update your status in COORDINATION.md on master.
```

---

## Session E: Integration (Opus)

**Only start this after Sessions A-D are complete.**

```
I'm working on the VoltMarket Transaction Intelligence Engine challenge. This session is SESSION E: Integration & Polish.

Repo: ~/Code/voltmarket-transaction-intelligence
Branch: master

1. cd into the repo, stay on master, activate the venv: `source venv/bin/activate`
2. Read `PLAN_E_INTEGRATION.md` for your full instructions
3. Check `COORDINATION.md` to confirm all sessions are complete
4. Read `CLAUDE.md` for project conventions

Your job:
1. Merge all feature branches into master (data-generator, pipeline, analytics, visualization)
2. Create `src/main.py` — a Click CLI that runs the full pipeline end-to-end: generate → pipeline → analyze → visualize
3. Run `python -m src.main run-all` and verify everything works
4. Fix any integration issues
5. Write a comprehensive README.md with: overview, architecture, setup instructions, key findings, dashboard screenshot, design decisions
6. Final cleanup and push

The end result should be a repo that someone can clone, run `pip install -r requirements.txt && python -m src.main run-all`, and see the full analysis.
```

---

## Model Recommendations

| Session | Recommended Model | Reasoning |
|---------|------------------|-----------|
| A (Data Gen) | **Sonnet** | Straightforward generation logic, numpy/statistical distributions |
| B (Pipeline) | **Sonnet** | Standard Polars transformations, well-defined inputs/outputs |
| C (Analytics) | **Sonnet** | Statistical analysis + text generation for insights, well-scoped |
| D (Visualization) | **Opus** | Most creative/design-intensive, layout decisions, UX polish |
| E (Integration) | **Opus** | Needs to understand all pieces, resolve conflicts, write README |
