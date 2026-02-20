# VoltMarket Transaction Intelligence Engine

A data pipeline and analytics tool that diagnoses why VoltMarket's payment approval rates collapsed from **82% to 64%** over 6 weeks, costing approximately **$1.2M monthly**.

## Key Findings

The system identified **3 root causes** behind the approval rate collapse:

| # | Finding | Rate Change | Est. Monthly Impact |
|---|---------|-------------|-------------------|
| 1 | **High-value transactions ($350-500)** degraded significantly | 78.9% → 55.7% (-23pp) | $173K |
| 2 | **BBVA Mexico** approval rate collapsed | 82.2% → 38.3% (-44pp) | $24K |
| 3 | **Night hours (20h-24h)** underperform across all segments | 76.9% → 55.7% (-21pp) | $78K |

**"Aha" Moments:**
- BBVA Mexico experienced the single largest rate drop (-44 percentage points) of any issuer, suggesting a policy change or technical issue at the bank
- Transactions above $200 USD saw disproportionate declines, pointing to tighter risk scoring thresholds
- Evening hours (post-8 PM) degraded more than daytime, indicating potential batch processing timeouts or fraud rule changes

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌────────────────┐
│  Data Generator  │───▷│   Pipeline    │───▷│   Analytics     │───▷│ Visualization  │
│  (8K synthetic   │    │  (Polars      │    │  (Anomaly       │    │ (Plotly +      │
│   transactions)  │    │   segments)   │    │   detection)    │    │  Streamlit)    │
└─────────────────┘    └──────────────┘    └─────────────────┘    └────────────────┘
        │                      │                    │                      │
        ▼                      ▼                    ▼                      ▼
  data/raw/              data/processed/      data/analytics/        output/
  transactions.parquet   segments.parquet     insights.json          dashboard.html
                         weekly_trends.parquet anomalies.parquet     *.png charts
```

**Tech Stack:** Python 3.9+ | Polars | Plotly | Streamlit | Faker | SciPy

## Quick Start

```bash
# Clone and setup
git clone https://github.com/nicolasc-yuno/voltmarket-transaction-intelligence.git
cd voltmarket-transaction-intelligence
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Run full pipeline (generate → process → analyze → visualize)
python -m src.main run-all

# Launch interactive dashboard
streamlit run src/visualization/dashboard.py
```

## Individual Steps

```bash
python -m src.main generate    # Generate 8,000 synthetic transactions
python -m src.main pipeline    # Run segmentation pipeline (2,400+ segments)
python -m src.main analyze     # Anomaly detection + ranked insights
python -m src.main visualize   # Export charts (PNG) + HTML dashboard
```

## Project Structure

```
src/
├── contracts/schemas.py       # Data contracts (single source of truth)
├── data_generator/generate.py # Synthetic transaction generator
├── pipeline/
│   ├── ingest.py              # Load, validate, enrich raw data
│   ├── segment.py             # Multi-dimensional approval rate segmentation
│   └── transform.py           # Weekly trend aggregation
├── analytics/
│   ├── anomaly_detection.py   # Z-score + p-value anomaly detection
│   └── insights.py            # Ranked insight generation
├── visualization/
│   ├── charts.py              # 6 Plotly chart functions
│   └── dashboard.py           # Streamlit dashboard
└── main.py                    # CLI entrypoint (Click)

data/
├── raw/                       # Generated transaction data
├── processed/                 # Pipeline output (segments, trends)
└── analytics/                 # Insights, anomalies, summary

output/                        # Static charts (PNG) + HTML dashboard
```

## Design Decisions

- **Polars over Pandas**: Faster execution, better memory efficiency, more expressive API for group-by operations
- **Contract-first architecture**: All schemas defined in `src/contracts/schemas.py` — enabled 4 parallel development sessions
- **Mock data fallbacks**: Each module can generate mock data if upstream isn't available, enabling independent development
- **Diversity in insight ranking**: Weighted scoring with specificity bonus ensures root causes (issuer-level, time-based) surface above broad symptoms (card brand, country)
- **Static HTML export**: Dashboard works as a standalone file without running Streamlit — ideal for sharing with stakeholders

## Embedded Data Patterns

The synthetic data generator embeds three degradation patterns that mirror real-world payment processing issues:

1. **BBVA Mexico Collapse** (Primary): Approval rate drops from ~85% to ~38% in weeks 4-6, simulating a bank-side policy change
2. **High-Value Degradation**: Transactions >$200 USD see approval rates drop ~24pp, simulating tighter risk scoring
3. **Evening Underperformance**: Post-8 PM transactions decline ~21pp more than daytime, simulating batch processing or fraud rule changes

## Test Data Specifications

- **8,000 transactions** across 6 weeks
- **3 countries**: Brazil (50%), Mexico (30%), Colombia (20%)
- **Card brands**: Visa (50%), Mastercard (35%), Amex (15%)
- **Card types**: Credit (60%), Debit (35%), Prepaid (5%)
- **8 issuer banks** per country
- **Amounts**: $10-$500 in local currencies (BRL, MXN, COP)
