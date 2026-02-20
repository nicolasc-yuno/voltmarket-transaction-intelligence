"""
VoltMarket Transaction Intelligence — Static Chart Functions.

Each function returns a plotly.graph_objects.Figure.
Run as CLI to export PNGs: python -m src.visualization.charts
"""

from __future__ import annotations

import os
from pathlib import Path

import plotly.graph_objects as go
import polars as pl

from src.contracts.schemas import (
    BASELINE_APPROVAL_RATE,
    COUNTRIES,
    DEGRADED_APPROVAL_RATE,
    ISSUER_BANKS,
)

# ---------------------------------------------------------------------------
# Color palette
# ---------------------------------------------------------------------------
COLOR_GREEN = "#2ecc71"
COLOR_YELLOW = "#f1c40f"
COLOR_RED = "#e74c3c"
COLOR_BLUE = "#3498db"
COLOR_DARK_BLUE = "#2c3e50"
COLOR_LIGHT_GRAY = "#ecf0f1"
COLOR_ORANGE = "#e67e22"
COLOR_PURPLE = "#9b59b6"

TARGET_THRESHOLD = 0.70


def _rate_color(rate: float) -> str:
    if rate >= 0.75:
        return COLOR_GREEN
    if rate >= 0.65:
        return COLOR_YELLOW
    return COLOR_RED


def _pct(v: float) -> str:
    return f"{v * 100:.1f}%"


# ---------------------------------------------------------------------------
# Mock data factories (used when upstream parquet files don't exist)
# ---------------------------------------------------------------------------

def build_mock_weekly_trends() -> pl.DataFrame:
    """Weekly approval rates: 82% declining to 64% over 6 weeks."""
    rates = [0.82, 0.81, 0.80, 0.73, 0.68, 0.64]
    totals = [18500, 19200, 18800, 19500, 19100, 18900]
    return pl.DataFrame({
        "week_number": list(range(1, 7)),
        "total_transactions": totals,
        "approved_transactions": [int(t * r) for t, r in zip(totals, rates)],
        "approval_rate": rates,
        "total_amount_usd": [t * 85.0 for t in totals],
    })


def build_mock_country_segments() -> pl.DataFrame:
    """Country-level approval rates for weeks 1-3 vs 4-6."""
    rows = [
        ("BR", "weeks_1_3", 0.86, 28000),
        ("BR", "weeks_4_6", 0.78, 27500),
        ("MX", "weeks_1_3", 0.79, 17000),
        ("MX", "weeks_4_6", 0.54, 17200),
        ("CO", "weeks_1_3", 0.80, 11500),
        ("CO", "weeks_4_6", 0.72, 11800),
    ]
    return pl.DataFrame({
        "dimension_1": [r[0] for r in rows],
        "period": [r[1] for r in rows],
        "approval_rate": [r[2] for r in rows],
        "total_transactions": [r[3] for r in rows],
    })


def build_mock_issuer_weekly() -> pl.DataFrame:
    """Issuer-bank approval rates per week for heatmap."""
    issuers = [
        "BBVA",
        "Banorte",
        "Citibanamex",
        "Itau",
        "Bradesco",
        "Bancolombia",
        "Nubank",
        "Davivienda",
    ]
    # Rows: issuer, w1..w6 rates
    data: dict[str, list] = {"issuer_bank": issuers}
    base_rates = [0.80, 0.83, 0.85, 0.87, 0.82, 0.81, 0.88, 0.79]
    # BBVA collapses hard in weeks 4-6; others have modest declines
    decline_factors = [
        [1.0, 1.0, 0.99, 0.55, 0.50, 0.45],  # BBVA — dramatic
        [1.0, 1.0, 0.99, 0.90, 0.88, 0.85],
        [1.0, 1.0, 1.0, 0.92, 0.90, 0.88],
        [1.0, 1.0, 1.0, 0.95, 0.93, 0.92],
        [1.0, 1.0, 0.99, 0.93, 0.91, 0.90],
        [1.0, 0.99, 0.99, 0.92, 0.89, 0.87],
        [1.0, 1.0, 1.0, 0.96, 0.95, 0.94],
        [1.0, 1.0, 0.99, 0.91, 0.88, 0.86],
    ]
    for w in range(1, 7):
        data[f"week_{w}"] = [
            round(base * factors[w - 1], 3)
            for base, factors in zip(base_rates, decline_factors)
        ]
    return pl.DataFrame(data)


def build_mock_waterfall() -> pl.DataFrame:
    """Contribution of each factor to overall decline."""
    return pl.DataFrame({
        "factor": [
            "Baseline",
            "BBVA Mexico",
            "High-value txns (>$200)",
            "Evening hours (17-20h)",
            "Banorte decline",
            "Other segments",
            "Current",
        ],
        "contribution_pp": [82.0, -8.0, -4.0, -3.0, -2.0, -1.0, 64.0],
        "measure": [
            "absolute",
            "relative",
            "relative",
            "relative",
            "relative",
            "relative",
            "total",
        ],
    })


def build_mock_amount_distribution() -> pl.DataFrame:
    """Transaction amounts split by status and period."""
    import random
    random.seed(42)
    rows = []
    for period in ["weeks_1_3", "weeks_4_6"]:
        for _ in range(500):
            amt = random.uniform(10, 500)
            if period == "weeks_4_6" and amt > 200:
                status = "declined" if random.random() < 0.55 else "approved"
            elif period == "weeks_4_6":
                status = "declined" if random.random() < 0.25 else "approved"
            else:
                status = "declined" if random.random() < 0.18 else "approved"
            rows.append({"period": period, "amount_usd": round(amt, 2), "status": status})
    return pl.DataFrame(rows)


def build_mock_hourly() -> pl.DataFrame:
    """Approval rate by hour of day, weeks 1-3 vs 4-6."""
    import random
    random.seed(99)
    rows = []
    for h in range(24):
        base = 0.83 - 0.02 * abs(h - 12) / 12
        early_rate = round(base + random.uniform(-0.01, 0.01), 3)
        # Evening dip in weeks 4-6
        if 17 <= h <= 20:
            late_rate = round(early_rate - 0.18 + random.uniform(-0.02, 0.02), 3)
        elif 21 <= h <= 23:
            late_rate = round(early_rate - 0.12 + random.uniform(-0.02, 0.02), 3)
        else:
            late_rate = round(early_rate - 0.06 + random.uniform(-0.02, 0.02), 3)
        rows.append({"hour_of_day": h, "period": "weeks_1_3", "approval_rate": early_rate})
        rows.append({"hour_of_day": h, "period": "weeks_4_6", "approval_rate": late_rate})
    return pl.DataFrame(rows)


def build_mock_insights() -> pl.DataFrame:
    """Top insights matching INSIGHT_SCHEMA."""
    return pl.DataFrame({
        "rank": [1, 2, 3, 4, 5],
        "insight_id": [f"INS-{i:03d}" for i in range(1, 6)],
        "title": [
            "BBVA Mexico Approval Collapse",
            "High-Value Transaction Decline",
            "Evening Hours Degradation",
            "Banorte Approval Drop",
            "Prepaid Card Rejection Surge",
        ],
        "description": [
            "BBVA Mexico approval rate fell from 80% to 36%, driving the single largest share of overall decline.",
            "Transactions above $200 USD saw approval rates drop from 78% to 52%, suggesting tighter risk thresholds.",
            "Hours 17-20 experienced a disproportionate decline (83% to 65%), pointing to batch-processing timeouts.",
            "Banorte approval rate declined from 83% to 71%, the second-largest issuer impact in Mexico.",
            "Prepaid cards went from 75% to 58% approval, likely due to new fraud rules at Mexican issuers.",
        ],
        "segment_key": [
            "MX|BBVA",
            "amount_bucket|$200-500",
            "hour_bucket|evening_17_20",
            "MX|Banorte",
            "card_type|Prepaid",
        ],
        "baseline_rate": [0.80, 0.78, 0.83, 0.83, 0.75],
        "current_rate": [0.36, 0.52, 0.65, 0.71, 0.58],
        "rate_change": [-0.44, -0.26, -0.18, -0.12, -0.17],
        "affected_transactions": [8500, 6200, 7800, 5100, 3200],
        "estimated_revenue_impact_usd": [180000.0, 95000.0, 72000.0, 48000.0, 35000.0],
        "severity": ["critical", "critical", "high", "high", "medium"],
    })


# ---------------------------------------------------------------------------
# Helper: try to load real data, fall back to mock
# ---------------------------------------------------------------------------

def _load_or_mock(path: str, mock_fn):
    """Load a parquet file if it exists and has rows, otherwise use mock."""
    p = Path(path)
    if p.exists():
        df = pl.read_parquet(p)
        if len(df) > 0:
            return df
    return mock_fn()


# ---------------------------------------------------------------------------
# Chart 1 — Headline Trend
# ---------------------------------------------------------------------------

def chart_headline_trend(df: pl.DataFrame | None = None) -> go.Figure:
    """Weekly approval rate line chart with inflection annotation."""
    if df is None:
        df = _load_or_mock("data/processed/weekly_trends.parquet", build_mock_weekly_trends)

    weeks = df["week_number"].to_list()
    rates = df["approval_rate"].to_list()

    fig = go.Figure()

    # Main trend line
    fig.add_trace(go.Scatter(
        x=weeks,
        y=[r * 100 for r in rates],
        mode="lines+markers+text",
        text=[f"{r * 100:.1f}%" for r in rates],
        textposition="top center",
        textfont=dict(size=12, color=COLOR_DARK_BLUE),
        line=dict(color=COLOR_BLUE, width=3),
        marker=dict(size=10, color=[_rate_color(r) for r in rates]),
        name="Approval Rate",
        hovertemplate="Week %{x}: %{y:.1f}%<extra></extra>",
    ))

    # Target threshold
    fig.add_hline(
        y=TARGET_THRESHOLD * 100,
        line_dash="dash",
        line_color=COLOR_RED,
        annotation_text=f"Target: {TARGET_THRESHOLD * 100:.0f}%",
        annotation_position="bottom right",
        annotation_font_color=COLOR_RED,
    )

    # Inflection point annotation at Week 4
    fig.add_annotation(
        x=4, y=rates[3] * 100,
        text="Inflection Point<br>Decline accelerates",
        showarrow=True,
        arrowhead=2,
        arrowcolor=COLOR_RED,
        font=dict(color=COLOR_RED, size=11),
        ax=-60, ay=-40,
    )

    fig.update_layout(
        title=dict(
            text="VoltMarket Approval Rate: 6-Week Decline",
            font=dict(size=20, color=COLOR_DARK_BLUE),
        ),
        xaxis=dict(
            title="Week",
            tickmode="linear",
            dtick=1,
            range=[0.5, 6.5],
        ),
        yaxis=dict(
            title="Approval Rate (%)",
            range=[55, 90],
            ticksuffix="%",
        ),
        template="plotly_white",
        height=400,
        margin=dict(t=60, b=40),
        showlegend=False,
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 2 — Country Breakdown
# ---------------------------------------------------------------------------

def chart_country_breakdown(df: pl.DataFrame | None = None) -> go.Figure:
    """Grouped bar chart: approval rate by country, early vs late period."""
    if df is None:
        df = build_mock_country_segments()

    countries = sorted(df["dimension_1"].unique().to_list())

    early_rates = []
    late_rates = []
    for c in countries:
        e = df.filter((pl.col("dimension_1") == c) & (pl.col("period") == "weeks_1_3"))
        l = df.filter((pl.col("dimension_1") == c) & (pl.col("period") == "weeks_4_6"))
        early_rates.append(e["approval_rate"][0] if len(e) else 0)
        late_rates.append(l["approval_rate"][0] if len(l) else 0)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=countries,
        y=[r * 100 for r in early_rates],
        name="Weeks 1-3",
        marker_color=COLOR_BLUE,
        text=[f"{r * 100:.1f}%" for r in early_rates],
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=countries,
        y=[r * 100 for r in late_rates],
        name="Weeks 4-6",
        marker_color=[_rate_color(r) for r in late_rates],
        text=[f"{r * 100:.1f}%" for r in late_rates],
        textposition="outside",
    ))

    # Highlight Mexico
    mx_idx = countries.index("MX") if "MX" in countries else None
    if mx_idx is not None:
        fig.add_annotation(
            x="MX",
            y=late_rates[mx_idx] * 100,
            text=f"<b>-{(early_rates[mx_idx] - late_rates[mx_idx]) * 100:.0f}pp</b>",
            showarrow=True,
            arrowhead=2,
            arrowcolor=COLOR_RED,
            font=dict(color=COLOR_RED, size=13),
            ay=-30,
        )

    fig.update_layout(
        title=dict(text="Approval Rate by Country", font=dict(size=18, color=COLOR_DARK_BLUE)),
        barmode="group",
        yaxis=dict(title="Approval Rate (%)", range=[0, 100], ticksuffix="%"),
        xaxis=dict(title="Country"),
        template="plotly_white",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=60, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 3 — Issuer Bank Heatmap
# ---------------------------------------------------------------------------

def chart_issuer_heatmap(df: pl.DataFrame | None = None) -> go.Figure:
    """Heatmap of issuer-bank approval rates by week, sorted by severity."""
    if df is None:
        df = build_mock_issuer_weekly()

    week_cols = [c for c in df.columns if c.startswith("week_")]
    issuers = df["issuer_bank"].to_list()
    z_values = df.select(week_cols).to_numpy()

    # Sort by week_6 rate ascending (worst at top)
    last_week_rates = z_values[:, -1]
    order = last_week_rates.argsort()
    z_sorted = z_values[order] * 100
    issuers_sorted = [issuers[i] for i in order]

    fig = go.Figure(go.Heatmap(
        z=z_sorted,
        x=[f"Week {i}" for i in range(1, 7)],
        y=issuers_sorted,
        colorscale=[
            [0.0, COLOR_RED],
            [0.5, COLOR_YELLOW],
            [1.0, COLOR_GREEN],
        ],
        zmin=30,
        zmax=90,
        text=[[f"{v:.1f}%" for v in row] for row in z_sorted],
        texttemplate="%{text}",
        textfont=dict(size=11),
        hovertemplate="Issuer: %{y}<br>%{x}: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Rate %", ticksuffix="%"),
    ))

    fig.update_layout(
        title=dict(text="Issuer Bank Approval Rates by Week", font=dict(size=18, color=COLOR_DARK_BLUE)),
        template="plotly_white",
        height=400,
        margin=dict(t=60, b=40, l=120),
        yaxis=dict(autorange="reversed"),
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 4 — Dimensional Waterfall
# ---------------------------------------------------------------------------

def chart_waterfall(df: pl.DataFrame | None = None) -> go.Figure:
    """Waterfall/bridge chart showing contribution of each factor."""
    if df is None:
        df = build_mock_waterfall()

    factors = df["factor"].to_list()
    values = df["contribution_pp"].to_list()
    measures = df["measure"].to_list()

    colors = []
    for m, v in zip(measures, values):
        if m == "absolute":
            colors.append(COLOR_BLUE)
        elif m == "total":
            colors.append(COLOR_RED if v < 70 else COLOR_GREEN)
        else:
            colors.append(COLOR_RED if v < 0 else COLOR_GREEN)

    fig = go.Figure(go.Waterfall(
        x=factors,
        y=values,
        measure=measures,
        text=[f"{v:+.0f}pp" if m == "relative" else f"{v:.0f}%" for v, m in zip(values, measures)],
        textposition="outside",
        textfont=dict(size=12),
        connector=dict(line=dict(color=COLOR_LIGHT_GRAY, width=2)),
        decreasing=dict(marker=dict(color=COLOR_RED)),
        increasing=dict(marker=dict(color=COLOR_GREEN)),
        totals=dict(marker=dict(color=COLOR_DARK_BLUE)),
    ))

    fig.update_layout(
        title=dict(text="Root Cause Waterfall: What Drove the Decline?", font=dict(size=18, color=COLOR_DARK_BLUE)),
        yaxis=dict(title="Approval Rate (%)", ticksuffix="%", range=[50, 90]),
        template="plotly_white",
        height=400,
        margin=dict(t=60, b=60),
        showlegend=False,
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 5 — Amount Distribution
# ---------------------------------------------------------------------------

def chart_amount_distribution(df: pl.DataFrame | None = None) -> go.Figure:
    """Box plots of transaction amounts: approved vs declined, by period."""
    if df is None:
        df = build_mock_amount_distribution()

    fig = go.Figure()

    combos = [
        ("weeks_1_3", "approved", "W1-3 Approved", COLOR_GREEN, 0.78),
        ("weeks_1_3", "declined", "W1-3 Declined", COLOR_YELLOW, 0.78),
        ("weeks_4_6", "approved", "W4-6 Approved", COLOR_BLUE, 0.78),
        ("weeks_4_6", "declined", "W4-6 Declined", COLOR_RED, 0.78),
    ]

    for period, status, name, color, opacity in combos:
        subset = df.filter((pl.col("period") == period) & (pl.col("status") == status))
        fig.add_trace(go.Box(
            y=subset["amount_usd"].to_list(),
            name=name,
            marker_color=color,
            opacity=opacity,
            boxmean=True,
        ))

    fig.add_annotation(
        text="Declined txns in W4-6 skew<br>toward higher amounts",
        xref="paper", yref="paper",
        x=0.95, y=0.95,
        showarrow=False,
        font=dict(size=11, color=COLOR_RED),
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor=COLOR_RED,
        borderwidth=1,
    )

    fig.update_layout(
        title=dict(text="Transaction Amount Distribution by Status", font=dict(size=18, color=COLOR_DARK_BLUE)),
        yaxis=dict(title="Amount (USD)", tickprefix="$"),
        template="plotly_white",
        height=400,
        margin=dict(t=60, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 6 — Hour-of-Day Pattern
# ---------------------------------------------------------------------------

def chart_hourly_pattern(df: pl.DataFrame | None = None) -> go.Figure:
    """Approval rate by hour of day, weeks 1-3 vs 4-6."""
    if df is None:
        df = build_mock_hourly()

    early = df.filter(pl.col("period") == "weeks_1_3").sort("hour_of_day")
    late = df.filter(pl.col("period") == "weeks_4_6").sort("hour_of_day")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=early["hour_of_day"].to_list(),
        y=[r * 100 for r in early["approval_rate"].to_list()],
        mode="lines+markers",
        name="Weeks 1-3",
        line=dict(color=COLOR_BLUE, width=2),
        marker=dict(size=6),
    ))
    fig.add_trace(go.Scatter(
        x=late["hour_of_day"].to_list(),
        y=[r * 100 for r in late["approval_rate"].to_list()],
        mode="lines+markers",
        name="Weeks 4-6",
        line=dict(color=COLOR_RED, width=2),
        marker=dict(size=6),
    ))

    # Highlight evening dip
    fig.add_vrect(
        x0=16.5, x1=20.5,
        fillcolor=COLOR_RED,
        opacity=0.08,
        line_width=0,
        annotation_text="Evening Dip",
        annotation_position="top left",
        annotation_font_color=COLOR_RED,
    )

    fig.update_layout(
        title=dict(text="Approval Rate by Hour of Day", font=dict(size=18, color=COLOR_DARK_BLUE)),
        xaxis=dict(title="Hour of Day", tickmode="linear", dtick=2, range=[-0.5, 23.5]),
        yaxis=dict(title="Approval Rate (%)", ticksuffix="%", range=[55, 90]),
        template="plotly_white",
        height=400,
        margin=dict(t=60, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

ALL_CHARTS = {
    "headline_trend": chart_headline_trend,
    "country_breakdown": chart_country_breakdown,
    "issuer_heatmap": chart_issuer_heatmap,
    "waterfall": chart_waterfall,
    "amount_distribution": chart_amount_distribution,
    "hourly_pattern": chart_hourly_pattern,
}


def export_all_png(output_dir: str = "output") -> list[str]:
    """Export all charts as PNG files. Returns list of paths written."""
    os.makedirs(output_dir, exist_ok=True)
    paths = []
    for name, fn in ALL_CHARTS.items():
        fig = fn()
        path = os.path.join(output_dir, f"{name}.png")
        fig.write_image(path, width=1200, height=500, scale=2)
        paths.append(path)
        print(f"  Exported {path}")
    return paths


def export_standalone_html(output_dir: str = "output") -> str:
    """Export a standalone HTML file combining all charts."""
    os.makedirs(output_dir, exist_ok=True)

    charts_html = []
    for name, fn in ALL_CHARTS.items():
        fig = fn()
        charts_html.append(fig.to_html(full_html=False, include_plotlyjs=False))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VoltMarket Transaction Intelligence Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f8f9fa; margin: 0; padding: 20px; }}
        .header {{ background: linear-gradient(135deg, #2c3e50, #3498db); color: white; padding: 30px; border-radius: 12px; margin-bottom: 24px; }}
        .header h1 {{ margin: 0 0 8px 0; font-size: 28px; }}
        .header .subtitle {{ font-size: 16px; opacity: 0.9; }}
        .kpi-row {{ display: flex; gap: 16px; margin-bottom: 24px; }}
        .kpi {{ flex: 1; background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; }}
        .kpi .value {{ font-size: 32px; font-weight: 700; }}
        .kpi .label {{ font-size: 13px; color: #7f8c8d; margin-top: 4px; }}
        .chart-container {{ background: white; border-radius: 10px; padding: 16px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
        .row {{ display: flex; gap: 20px; }}
        .row > .chart-container {{ flex: 1; }}
        .insight-row {{ display: flex; gap: 16px; margin-bottom: 24px; }}
        .insight-card {{ flex: 1; background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-left: 4px solid; }}
        .insight-card.critical {{ border-left-color: #e74c3c; }}
        .insight-card.high {{ border-left-color: #e67e22; }}
        .insight-card.medium {{ border-left-color: #f1c40f; }}
        .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; text-transform: uppercase; color: white; }}
        .badge.critical {{ background: #e74c3c; }}
        .badge.high {{ background: #e67e22; }}
        .badge.medium {{ background: #f1c40f; color: #2c3e50; }}
        .insight-card h3 {{ margin: 8px 0 4px; font-size: 15px; }}
        .insight-card .metric {{ font-size: 20px; font-weight: 700; color: #e74c3c; }}
        .insight-card .impact {{ font-size: 12px; color: #7f8c8d; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>VoltMarket Transaction Intelligence Report</h1>
        <div class="subtitle">Approval Rate Analysis &mdash; 6-Week Period | Overall: 82% &rarr; 64% | Estimated Impact: ~$1.2M/month</div>
    </div>

    <div class="kpi-row">
        <div class="kpi"><div class="value" style="color:#e74c3c">64%</div><div class="label">Current Approval Rate</div></div>
        <div class="kpi"><div class="value" style="color:#2c3e50">82%</div><div class="label">Baseline (Weeks 1-3)</div></div>
        <div class="kpi"><div class="value" style="color:#e74c3c">-18pp</div><div class="label">Rate Decline</div></div>
        <div class="kpi"><div class="value" style="color:#e67e22">~$1.2M</div><div class="label">Est. Monthly Impact</div></div>
    </div>

    <div class="chart-container">{charts_html[0]}</div>

    <div class="row">
        <div class="chart-container">{charts_html[1]}</div>
        <div class="chart-container">{charts_html[2]}</div>
    </div>

    <h2 style="color:#2c3e50; margin: 24px 0 12px;">Key Findings</h2>
    <div class="insight-row">
        <div class="insight-card critical">
            <span class="badge critical">Critical</span>
            <h3>BBVA Mexico Approval Collapse</h3>
            <div class="metric">80% &rarr; 36% (-44pp)</div>
            <div class="impact">Est. impact: $180K/month | 8,500 txns affected</div>
        </div>
        <div class="insight-card critical">
            <span class="badge critical">Critical</span>
            <h3>High-Value Transaction Decline</h3>
            <div class="metric">78% &rarr; 52% (-26pp)</div>
            <div class="impact">Est. impact: $95K/month | 6,200 txns affected</div>
        </div>
        <div class="insight-card high">
            <span class="badge high">High</span>
            <h3>Evening Hours Degradation</h3>
            <div class="metric">83% &rarr; 65% (-18pp)</div>
            <div class="impact">Est. impact: $72K/month | 7,800 txns affected</div>
        </div>
    </div>

    <div class="row">
        <div class="chart-container">{charts_html[3]}</div>
        <div class="chart-container">{charts_html[4]}</div>
    </div>

    <div class="chart-container">{charts_html[5]}</div>

    <div style="text-align:center; color:#95a5a6; padding:20px; font-size:12px;">
        Generated by VoltMarket Transaction Intelligence Engine | Data period: Weeks 1-6
    </div>
</body>
</html>"""

    path = os.path.join(output_dir, "dashboard.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"  Exported {path}")
    return path


# ---------------------------------------------------------------------------
# CLI entrypoint: python -m src.visualization.charts
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Exporting VoltMarket charts...")
    export_all_png()
    export_standalone_html()
    print("Done.")
