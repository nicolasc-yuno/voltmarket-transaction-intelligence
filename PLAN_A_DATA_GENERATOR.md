# Plan A: Data Generator

**Branch:** `feature/data-generator`
**Output:** `data/raw/transactions.parquet`
**Estimated time:** 20-25 min

## Objective

Generate 8,000 realistic synthetic transactions across 6 weeks with **three embedded degradation patterns** that the analytics layer must discover.

## Tasks

### 1. Build the generator (`src/data_generator/generate.py`)

- Import schemas and constants from `src/contracts/schemas.py`
- Use `faker` for merchant_id, customer_id; `numpy` for statistical distributions
- Generate timestamps spread across 6 weeks (use a fixed start date like 2026-01-05)
- Distribute countries, card brands, card types per weights in contracts
- Assign issuer banks randomly within each country's bank list
- Generate amounts between 10-500 in local currency, convert to USD using fx_rates

### 2. Embed the three degradation patterns

These are the "hidden patterns" the challenge requires:

**Pattern 1 - BBVA Mexico collapse:**
- Weeks 1-3: BBVA Mexico approval rate ~85%
- Weeks 4-6: BBVA Mexico approval rate ~45%
- This is the PRIMARY driver of the overall decline
- Decline reasons should skew toward "do_not_honor" and "fraud_suspected"

**Pattern 2 - High-value transaction degradation:**
- Weeks 1-3: Transactions >$200 USD approve at ~80%
- Weeks 4-6: Transactions >$200 USD approve at ~60%
- Affects all countries but more pronounced in MX
- Decline reasons should skew toward "restricted_card" and "do_not_honor"

**Pattern 3 - Evening underperformance:**
- Weeks 1-3: Post-8PM transactions approve at ~78%
- Weeks 4-6: Post-8PM transactions approve at ~58%
- Subtle pattern across all segments
- Decline reasons should skew toward "fraud_suspected"

**Baseline (everything else):**
- Weeks 1-3: ~85% approval rate
- Weeks 4-6: ~75% approval rate (mild general decline, but the three patterns above are much worse)
- The combination of all patterns should produce the overall 82% -> 64% trajectory

### 3. Validation

After generating, print summary statistics:
- Total transactions per week
- Overall approval rate per week (should show 82% -> 64% trend)
- Approval rate for BBVA Mexico per week
- Approval rate for >$200 transactions per week
- Approval rate for post-8PM per week

### 4. Save output

- Save as parquet to `data/raw/transactions.parquet`
- Also save a CSV sample (first 100 rows) to `data/raw/sample.csv` for quick inspection
- Add a CLI command: `python -m src.data_generator.generate`

### 5. Commit & update coordination

- Push to `feature/data-generator`
- Also push the generated parquet to main so other sessions can use it
- Update COORDINATION.md Session A status to COMPLETE

## Acceptance Criteria

- [ ] Parquet file with ~8,000 rows matching `RAW_TRANSACTION_SCHEMA`
- [ ] Weekly approval rates show clear 82% -> 64% decline
- [ ] BBVA Mexico pattern is detectable
- [ ] High-value pattern is detectable
- [ ] Evening pattern is detectable
- [ ] CLI entrypoint works: `python -m src.data_generator.generate`
