# CLAUDE.md - VoltMarket Transaction Intelligence Engine

## Project Overview

Data pipeline + analytics tool to diagnose payment approval rate collapse (82% -> 64%) for VoltMarket. Built with Polars, Plotly, Streamlit.

## Structure

```
src/
├── contracts/schemas.py    # Data contracts (SINGLE SOURCE OF TRUTH)
├── data_generator/         # Session A: Synthetic transaction data
├── pipeline/               # Session B: Ingestion + segmentation
├── analytics/              # Session C: Anomaly detection + insights
└── visualization/          # Session D: Dashboard + charts
```

## Key Rules

- **ALWAYS** import schemas from `src.contracts.schemas` — never hardcode column names
- **Polars only** — no pandas unless required for Plotly/Streamlit compat
- Each module must work standalone with mock data if upstream isn't available yet
- Output files go to `data/` (intermediate) or `output/` (final artifacts)
- Update `COORDINATION.md` after completing any milestone

## Commands

```bash
source venv/bin/activate
python -m src.data_generator.generate  # Generate test data
python -m src.pipeline                 # Run pipeline
python -m src.analytics                # Run analytics
streamlit run src/visualization/dashboard.py  # Launch dashboard
```

## Parallel Session Protocol

This repo is worked on by multiple Claude Code sessions simultaneously. Each session:
1. Works on its own branch (`feature/data-generator`, `feature/pipeline`, etc.)
2. Reads its plan file (`PLAN_A_*.md`, `PLAN_B_*.md`, etc.)
3. Updates `COORDINATION.md` when done
4. Commits and pushes frequently
