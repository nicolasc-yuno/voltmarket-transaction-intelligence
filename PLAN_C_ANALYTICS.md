# Plan C: Analytics & Anomaly Detection

**Branch:** `feature/analytics`
**Input:** `data/processed/segments.parquet`, `data/processed/weekly_trends.parquet`
**Output:** `data/analytics/insights.json`, `data/analytics/anomalies.parquet`
**Estimated time:** 25-30 min

## Objective

Analyze segmented data to detect anomalies, rank insights by revenue impact, and produce 3-5 actionable findings that explain the approval rate collapse.

## Tasks

### 1. Anomaly Detection (`src/analytics/anomaly_detection.py`)

**Compare weeks 1-3 (baseline) vs weeks 4-6 (current) for every segment:**

- Filter segments to `period = "weeks_1_3"` and `period = "weeks_4_6"`
- Join on segment_key
- Calculate rate_change = current_rate - baseline_rate
- Calculate z-score: how many standard deviations this segment's change is from the mean change across all segments
- Calculate p-value using `scipy.stats` (proportions z-test between baseline and current)
- Flag as anomaly if: `|z_score| > 2.0` OR `p_value < 0.05` AND `affected_transactions > 50`

**Revenue impact estimation:**
```
estimated_revenue_impact_usd = affected_transactions * avg_ticket_usd * abs(rate_change) * 4.33
# 4.33 = weeks per month, to annualize to monthly impact
```

**If pipeline output doesn't exist yet**, create mock segment data:
```python
# Generate ~50 segment rows with realistic values
# Include a few with large negative rate_change to simulate anomalies
```

Output must match `ANOMALY_SCHEMA`.

### 2. Insight Generation (`src/analytics/insights.py`)

From the anomalies, generate the top 3-5 ranked insights:

**Ranking criteria (weighted score):**
- Revenue impact (40%): Higher dollar impact = higher rank
- Rate change magnitude (30%): Bigger drop = higher rank
- Statistical significance (20%): Lower p-value = higher rank
- Breadth (10%): More affected transactions = higher rank

**For each insight, generate:**
- `title`: Short headline (e.g., "BBVA Mexico Approval Rate Collapsed")
- `description`: Full explanation with numbers (e.g., "BBVA Mexico's approval rate dropped from 85% to 45% starting Week 4, affecting 1,200 transactions and causing an estimated $180K monthly revenue loss. Primary decline reasons: do_not_honor (45%), fraud_suspected (30%).")
- `severity`: critical (>$100K impact), high (>$50K), medium (>$10K), low (<$10K)

**Expected insights the system should find:**
1. BBVA Mexico collapse (should be #1, ~$180K impact)
2. High-value transaction degradation (~$80K impact)
3. Evening transaction underperformance (~$50K impact)
4. General cross-segment decline (~$40K impact)
5. Possibly: specific brand+type combinations

Output as both parquet (`INSIGHT_SCHEMA`) and JSON for easy dashboard consumption.

### 3. Summary Statistics

Generate a summary dict saved to `data/analytics/summary.json`:
```json
{
  "overall_baseline_rate": 0.82,
  "overall_current_rate": 0.64,
  "total_rate_change": -0.18,
  "total_monthly_revenue_impact_usd": 1200000,
  "anomalies_detected": 12,
  "critical_insights": 1,
  "high_insights": 2,
  "analysis_timestamp": "2026-02-20T..."
}
```

### 4. CLI Entrypoint

- `python -m src.analytics` runs full analysis
- Print top 5 insights to console using `rich` for formatting

### 5. Commit & update coordination

- Push to `feature/analytics`
- Update COORDINATION.md Session C status

## Acceptance Criteria

- [ ] Anomaly detection identifies statistically significant segments
- [ ] 3-5 ranked insights with quantified revenue impact
- [ ] BBVA Mexico is ranked #1 or #2
- [ ] All three embedded patterns are detected
- [ ] JSON output for dashboard consumption
- [ ] CLI works: `python -m src.analytics`
- [ ] Works with mock data if pipeline output doesn't exist yet
