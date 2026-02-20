# Plan B: Pipeline

**Branch:** `feature/pipeline`
**Input:** `data/raw/transactions.parquet`
**Output:** `data/processed/segments.parquet`, `data/processed/weekly_trends.parquet`
**Estimated time:** 20-25 min

## Objective

Build a Polars-based segmentation pipeline that reads raw transactions and produces multi-dimensional approval rate breakdowns ready for analytics.

## Tasks

### 1. Ingest (`src/pipeline/ingest.py`)

- Read parquet from `data/raw/transactions.parquet`
- Validate schema against `RAW_TRANSACTION_SCHEMA` from contracts
- Add derived columns:
  - `amount_bucket`: Categorize amount_usd into buckets from contracts
  - `hour_bucket`: Categorize hour_of_day into buckets from contracts
  - `period_half`: "weeks_1_3" or "weeks_4_6" based on week_number
- Return validated + enriched DataFrame

**If raw data doesn't exist yet** (Session A not done), create a small mock:
```python
# Generate 200 random rows matching RAW_TRANSACTION_SCHEMA for development
# This lets you build and test the pipeline before real data arrives
```

### 2. Segment Engine (`src/pipeline/segment.py`)

Build a flexible segmentation function that calculates approval rates for ANY combination of dimensions.

Required segments (each with per-week AND period_half breakdowns):

| Segment Type | Group By |
|-------------|----------|
| `time_weekly` | week_number |
| `country` | country |
| `card_brand` | card_brand |
| `card_type` | card_type |
| `issuer` | country + issuer_bank |
| `amount_bucket` | amount_bucket |
| `hour_bucket` | hour_bucket |
| `country_brand` | country + card_brand |
| `country_brand_type` | country + card_brand + card_type |
| `country_issuer` | country + issuer_bank |
| `issuer_brand_type` | country + issuer_bank + card_brand + card_type |

For each segment, calculate:
- total_transactions, approved_transactions, declined_transactions
- approval_rate (approved / total)
- total_amount_usd, approved_amount_usd

Output must match `SEGMENT_SCHEMA` from contracts.

### 3. Weekly Trends (`src/pipeline/transform.py`)

- Aggregate to weekly level matching `WEEKLY_TREND_SCHEMA`
- This is a simple rollup used by visualization for the headline trend chart

### 4. CLI Entrypoint

- Add: `python -m src.pipeline` runs the full pipeline
- Print summary: number of segments generated, overall approval rate

### 5. Commit & update coordination

- Push to `feature/pipeline`
- Update COORDINATION.md Session B status

## Acceptance Criteria

- [ ] Reads raw parquet and validates schema
- [ ] Produces segments.parquet matching `SEGMENT_SCHEMA`
- [ ] Produces weekly_trends.parquet matching `WEEKLY_TREND_SCHEMA`
- [ ] At least 10 segment types with per-week and half-period breakdowns
- [ ] CLI works: `python -m src.pipeline`
- [ ] Works with mock data if raw doesn't exist yet
