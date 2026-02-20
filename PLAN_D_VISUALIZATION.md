# Plan D: Visualization & Dashboard

**Branch:** `feature/visualization`
**Input:** All data layers (raw, processed, analytics)
**Output:** `output/` (HTML dashboard, PNG charts, Streamlit app)
**Estimated time:** 25-30 min

## Objective

Build a visual intelligence dashboard that clearly communicates the approval rate collapse and its root causes to non-technical payment ops managers.

## Tasks

### 1. Static Charts (`src/visualization/charts.py`)

Build reusable chart functions using Plotly. Each returns a `plotly.graph_objects.Figure`:

**Chart 1 - Headline Trend:**
- Line chart showing weekly approval rate (82% -> 64%)
- Annotate the inflection point at Week 4
- Add a reference line at 70% (target threshold)
- Title: "VoltMarket Approval Rate: 6-Week Decline"

**Chart 2 - Country Breakdown:**
- Grouped bar chart: approval rate by country, weeks 1-3 vs weeks 4-6
- Mexico should visually stand out as the worst performer
- Color-coded: green (>75%), yellow (65-75%), red (<65%)

**Chart 3 - Issuer Bank Heatmap:**
- Heatmap: rows = issuer banks, columns = weeks 1-6, values = approval rate
- BBVA Mexico row should be clearly red in weeks 4-6
- Sort by severity (worst performers at top)

**Chart 4 - Dimensional Waterfall:**
- Waterfall/bridge chart showing contribution of each factor to the overall decline
- E.g., "BBVA Mexico: -8pp", "High-value txns: -4pp", "Evening hours: -3pp", "Other: -3pp"

**Chart 5 - Amount Distribution:**
- Dual histogram or box plot: transaction amounts for approved vs declined
- Show how >$200 transactions shifted toward decline in weeks 4-6

**Chart 6 - Hour of Day Pattern:**
- Line chart: approval rate by hour_of_day, with separate lines for weeks 1-3 vs 4-6
- Evening dip should be visible

### 2. Insight Cards

For each of the top 3-5 insights, generate a formatted card:
- Severity badge (color-coded)
- Title + description
- Key metric: "85% -> 45% (-40pp)"
- Estimated impact: "$180K/month"

### 3. Streamlit Dashboard (`src/visualization/dashboard.py`)

Assemble everything into a single-page Streamlit app:

```
┌─────────────────────────────────────────────────┐
│  VoltMarket Transaction Intelligence Report     │
│  Overall: 82% → 64% | Impact: ~$1.2M/month     │
├─────────────────────────────────────────────────┤
│  [Chart 1: Headline Trend - full width]         │
├──────────────────────┬──────────────────────────┤
│  [Chart 2: Country]  │  [Chart 3: Heatmap]      │
├──────────────────────┴──────────────────────────┤
│  Key Findings                                    │
│  ┌────────┐ ┌────────┐ ┌────────┐              │
│  │ Card 1 │ │ Card 2 │ │ Card 3 │              │
│  │ BBVA   │ │ Hi-val │ │ Evening│              │
│  └────────┘ └────────┘ └────────┘              │
├─────────────────────────────────────────────────┤
│  [Chart 4: Waterfall] │ [Chart 5: Amounts]      │
├─────────────────────────────────────────────────┤
│  [Chart 6: Hour of Day Pattern]                  │
└─────────────────────────────────────────────────┘
```

### 4. Static Export

- Export each chart as PNG to `output/`
- Export the full dashboard as a standalone HTML file: `output/dashboard.html`
- This ensures deliverables exist even without running Streamlit

### 5. Mock Data Support

**If upstream data doesn't exist yet**, create mock DataFrames:
```python
# Mock weekly_trends: 6 rows showing 82% -> 64% decline
# Mock segments: ~20 rows with country/brand/issuer breakdowns
# Mock insights: 3-5 sample insights
# This lets you build and style the dashboard before real data arrives
```

### 6. CLI Entrypoints

- `python -m src.visualization.charts` — generates static PNGs
- `streamlit run src/visualization/dashboard.py` — launches interactive dashboard

### 7. Commit & update coordination

- Push to `feature/visualization`
- Update COORDINATION.md Session D status

## Acceptance Criteria

- [ ] 6 distinct chart types covering all major dimensions
- [ ] Streamlit dashboard with layout matching wireframe above
- [ ] Insight cards with severity, metrics, and impact
- [ ] Static HTML export in `output/dashboard.html`
- [ ] PNG exports for all charts in `output/`
- [ ] Works with mock data if upstream not available yet
- [ ] Non-technical audience can understand findings at a glance
