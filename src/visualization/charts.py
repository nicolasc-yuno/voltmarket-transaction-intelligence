"""
VoltMarket Transaction Intelligence — Static Chart Functions.

Each function returns a plotly.graph_objects.Figure.
Run as CLI to export PNGs: python -m src.visualization.charts
"""

from __future__ import annotations

import html
import os
import random
from pathlib import Path
from string import Template

import plotly.graph_objects as go
import polars as pl

from src.contracts.schemas import (
    BASELINE_APPROVAL_RATE,
    DEGRADED_APPROVAL_RATE,
    INSIGHTS_JSON_PATH,
    INSIGHTS_OUTPUT_PATH,
    SEGMENT_OUTPUT_PATH,
    WEEKLY_TREND_OUTPUT_PATH,
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

TARGET_THRESHOLD = 0.70

# Thresholds for color coding (shared between _rate_color and heatmap)
RATE_GREEN_THRESHOLD = 0.75
RATE_YELLOW_THRESHOLD = 0.65


def _rate_color(rate: float) -> str:
    if rate >= RATE_GREEN_THRESHOLD:
        return COLOR_GREEN
    if rate >= RATE_YELLOW_THRESHOLD:
        return COLOR_YELLOW
    return COLOR_RED


def _pct(v: float) -> str:
    return f"{v * 100:.1f}%"


# ---------------------------------------------------------------------------
# Helper: try to load real data, fall back to mock
# ---------------------------------------------------------------------------

def _load_or_mock(path: str, mock_fn):
    """Load a parquet file if it exists and has rows, otherwise use mock."""
    p = Path(path)
    if p.exists():
        try:
            df = pl.read_parquet(p)
            if len(df) > 0:
                return df
        except Exception:
            pass  # corrupt file — fall back to mock
    return mock_fn()


def _load_json_or_mock(path: str, mock_fn):
    """Load a JSON file as a DataFrame if it exists, otherwise use mock."""
    p = Path(path)
    if p.exists():
        try:
            df = pl.read_json(p)
            if len(df) > 0:
                return df
        except Exception:
            pass
    return mock_fn()


# ---------------------------------------------------------------------------
# Mock data factories (used when upstream parquet files don't exist)
# ---------------------------------------------------------------------------

def build_mock_weekly_trends() -> pl.DataFrame:
    """Weekly approval rates: 82% declining to 64% over 6 weeks."""
    rates = [BASELINE_APPROVAL_RATE, 0.81, 0.80, 0.73, 0.68, DEGRADED_APPROVAL_RATE]
    totals = [18500, 19200, 18800, 19500, 19100, 18900]
    return pl.DataFrame({
        "week_number": list(range(1, 7)),
        "total_transactions": totals,
        "approved_transactions": [int(t * r) for t, r in zip(totals, rates)],
        "approval_rate": rates,
        "total_amount_usd": [t * 85.0 for t in totals],
    })


def build_mock_country_segments() -> pl.DataFrame:
    """Country-level approval rates for weeks 1-3 vs 4-6.

    Uses SEGMENT_SCHEMA columns so real data can flow in directly.
    """
    rows = [
        ("country", "BR", "weeks_1_3", 0.86, 28000),
        ("country", "BR", "weeks_4_6", 0.78, 27500),
        ("country", "MX", "weeks_1_3", 0.79, 17000),
        ("country", "MX", "weeks_4_6", 0.54, 17200),
        ("country", "CO", "weeks_1_3", 0.80, 11500),
        ("country", "CO", "weeks_4_6", 0.72, 11800),
    ]
    return pl.DataFrame({
        "segment_type": [r[0] for r in rows],
        "dimension_1": [r[1] for r in rows],
        "period": [r[2] for r in rows],
        "approval_rate": [r[3] for r in rows],
        "total_transactions": [r[4] for r in rows],
    })


def build_mock_issuer_segments() -> pl.DataFrame:
    """Issuer-bank approval rates per week in SEGMENT_SCHEMA format.

    One row per (issuer, week) — compatible with the real pipeline output.
    The chart function pivots this into the wide format needed for the heatmap.
    """
    issuers = [
        "BBVA", "Banorte", "Citibanamex", "Itau",
        "Bradesco", "Bancolombia", "Nubank", "Davivienda",
    ]
    base_rates = [0.80, 0.83, 0.85, 0.87, 0.82, 0.81, 0.88, 0.79]
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
    rows = []
    for idx, (issuer, base) in enumerate(zip(issuers, base_rates)):
        for w in range(1, 7):
            rate = round(base * decline_factors[idx][w - 1], 3)
            rows.append({
                "segment_type": "issuer",
                "dimension_1": issuer,
                "period": f"week_{w}",
                "approval_rate": rate,
                "total_transactions": 800,
            })
    return pl.DataFrame(rows)


def _pivot_issuer_to_wide(df: pl.DataFrame) -> pl.DataFrame:
    """Pivot long-format issuer segments into wide format for heatmap."""
    week_periods = sorted(
        [p for p in df["period"].unique().to_list() if p.startswith("week_")],
    )
    issuer_col = "dimension_1" if "dimension_1" in df.columns else "issuer_bank"
    issuers = df[issuer_col].unique().to_list()

    data: dict[str, list] = {"issuer_bank": issuers}
    for wp in week_periods:
        col_vals = []
        for iss in issuers:
            row = df.filter(
                (pl.col(issuer_col) == iss) & (pl.col("period") == wp)
            )
            col_vals.append(row["approval_rate"][0] if len(row) else 0.0)
        data[wp] = col_vals
    return pl.DataFrame(data)


def build_mock_waterfall() -> pl.DataFrame:
    """Contribution of each factor to overall decline."""
    baseline = BASELINE_APPROVAL_RATE * 100
    current = DEGRADED_APPROVAL_RATE * 100
    return pl.DataFrame({
        "factor": [
            "Baseline", "BBVA Mexico", "High-value txns (>$200)",
            "Evening hours (17-20h)", "Banorte decline", "Other segments", "Current",
        ],
        "contribution_pp": [baseline, -8.0, -4.0, -3.0, -2.0, -1.0, current],
        "measure": [
            "absolute", "relative", "relative", "relative",
            "relative", "relative", "total",
        ],
    })


def build_mock_amount_distribution() -> pl.DataFrame:
    """Transaction amounts split by status and period."""
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
    random.seed(99)
    rows = []
    for h in range(24):
        base = 0.83 - 0.02 * abs(h - 12) / 12
        early_rate = round(base + random.uniform(-0.01, 0.01), 3)
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
            "MX|BBVA", "amount_bucket|$200-500", "hour_bucket|evening_17_20",
            "MX|Banorte", "card_type|Prepaid",
        ],
        "baseline_rate": [0.80, 0.78, 0.83, 0.83, 0.75],
        "current_rate": [0.36, 0.52, 0.65, 0.71, 0.58],
        "rate_change": [-0.44, -0.26, -0.18, -0.12, -0.17],
        "affected_transactions": [8500, 6200, 7800, 5100, 3200],
        "estimated_revenue_impact_usd": [180000.0, 95000.0, 72000.0, 48000.0, 35000.0],
        "severity": ["critical", "critical", "high", "high", "medium"],
    })


def load_insights() -> pl.DataFrame:
    """Load insights from real data (JSON then parquet), fall back to mock."""
    df = _load_json_or_mock(INSIGHTS_JSON_PATH, lambda: pl.DataFrame())
    if len(df) > 0:
        return df
    return _load_or_mock(INSIGHTS_OUTPUT_PATH, build_mock_insights)


# ---------------------------------------------------------------------------
# Chart 1 — Headline Trend
# ---------------------------------------------------------------------------

def chart_headline_trend(df: pl.DataFrame | None = None) -> go.Figure:
    """Weekly approval rate line chart with inflection annotation."""
    if df is None:
        df = _load_or_mock(WEEKLY_TREND_OUTPUT_PATH, build_mock_weekly_trends)

    weeks = df["week_number"].to_list()
    rates = df["approval_rate"].to_list()

    fig = go.Figure()
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

    fig.add_hline(
        y=TARGET_THRESHOLD * 100,
        line_dash="dash",
        line_color=COLOR_RED,
        annotation_text=f"Target: {TARGET_THRESHOLD * 100:.0f}%",
        annotation_position="bottom right",
        annotation_font_color=COLOR_RED,
    )

    # Inflection annotation — find week 4 by value, not by index
    week4_rows = df.filter(pl.col("week_number") == 4)
    if len(week4_rows) > 0:
        week4_rate = week4_rows["approval_rate"][0]
        fig.add_annotation(
            x=4, y=week4_rate * 100,
            text="Inflection Point<br>Decline accelerates",
            showarrow=True, arrowhead=2, arrowcolor=COLOR_RED,
            font=dict(color=COLOR_RED, size=11),
            ax=-60, ay=-40,
        )

    fig.update_layout(
        title=dict(text="VoltMarket Approval Rate: 6-Week Decline", font=dict(size=20, color=COLOR_DARK_BLUE)),
        xaxis=dict(title="Week", tickmode="linear", dtick=1, range=[0.5, 6.5]),
        yaxis=dict(title="Approval Rate (%)", range=[55, 90], ticksuffix="%"),
        template="plotly_white",
        height=400, margin=dict(t=60, b=40), showlegend=False,
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 2 — Country Breakdown
# ---------------------------------------------------------------------------

def _load_country_segments() -> pl.DataFrame:
    """Load segment data filtered to country type, fall back to mock."""
    df = _load_or_mock(SEGMENT_OUTPUT_PATH, build_mock_country_segments)
    if "segment_type" in df.columns:
        country_df = df.filter(pl.col("segment_type") == "country")
        if len(country_df) > 0:
            return country_df
    return df


def chart_country_breakdown(df: pl.DataFrame | None = None) -> go.Figure:
    """Grouped bar chart: approval rate by country, early vs late period."""
    if df is None:
        df = _load_country_segments()

    # Sort by worst late-period rate first (Mexico should lead)
    late_df = df.filter(pl.col("period") == "weeks_4_6").sort("approval_rate")
    severity_order = late_df["dimension_1"].to_list()
    all_countries = df["dimension_1"].unique().to_list()
    countries = severity_order + [c for c in all_countries if c not in severity_order]

    early_rates = []
    late_rates = []
    for country in countries:
        early_rows = df.filter(
            (pl.col("dimension_1") == country) & (pl.col("period") == "weeks_1_3")
        )
        late_rows = df.filter(
            (pl.col("dimension_1") == country) & (pl.col("period") == "weeks_4_6")
        )
        early_rates.append(early_rows["approval_rate"][0] if len(early_rows) else 0.0)
        late_rates.append(late_rows["approval_rate"][0] if len(late_rows) else 0.0)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=countries, y=[r * 100 for r in early_rates],
        name="Weeks 1-3", marker_color=COLOR_BLUE,
        text=[f"{r * 100:.1f}%" for r in early_rates], textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=countries, y=[r * 100 for r in late_rates],
        name="Weeks 4-6", marker_color=[_rate_color(r) for r in late_rates],
        text=[f"{r * 100:.1f}%" for r in late_rates], textposition="outside",
    ))

    # Highlight worst performer (first in severity order)
    if countries and late_rates:
        worst_country = countries[0]
        drop = early_rates[0] - late_rates[0]
        if drop > 0:
            fig.add_annotation(
                x=worst_country, y=late_rates[0] * 100,
                text=f"<b>-{drop * 100:.0f}pp</b>",
                showarrow=True, arrowhead=2, arrowcolor=COLOR_RED,
                font=dict(color=COLOR_RED, size=13), ay=-30,
            )

    fig.update_layout(
        title=dict(text="Approval Rate by Country", font=dict(size=18, color=COLOR_DARK_BLUE)),
        barmode="group",
        yaxis=dict(title="Approval Rate (%)", range=[0, 100], ticksuffix="%"),
        xaxis=dict(title="Country"),
        template="plotly_white", height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=60, b=40),
    )
    return fig


# ---------------------------------------------------------------------------
# Chart 3 — Issuer Bank Heatmap
# ---------------------------------------------------------------------------

def _load_issuer_heatmap_data() -> pl.DataFrame:
    """Load issuer segment data and pivot to wide format for heatmap."""
    df = _load_or_mock(SEGMENT_OUTPUT_PATH, build_mock_issuer_segments)
    if "segment_type" in df.columns:
        issuer_df = df.filter(pl.col("segment_type") == "issuer")
        if len(issuer_df) > 0:
            return _pivot_issuer_to_wide(issuer_df)
    # Already in wide format (pre-pivoted)
    if "issuer_bank" in df.columns and any(c.startswith("week_") for c in df.columns):
        return df
    # Long format without segment_type — try pivot
    if "dimension_1" in df.columns and "period" in df.columns:
        return _pivot_issuer_to_wide(df)
    return _pivot_issuer_to_wide(build_mock_issuer_segments())


def chart_issuer_heatmap(df: pl.DataFrame | None = None) -> go.Figure:
    """Heatmap of issuer-bank approval rates by week, sorted by severity."""
    if df is None:
        df = _load_issuer_heatmap_data()

    week_cols = sorted([c for c in df.columns if c.startswith("week_")])
    issuers = df["issuer_bank"].to_list()
    z_values = df.select(week_cols).to_numpy()

    # Sort by last-week rate ascending (worst first in array = top of heatmap)
    last_week_rates = z_values[:, -1]
    order = last_week_rates.argsort()
    z_sorted = z_values[order] * 100
    issuers_sorted = [issuers[i] for i in order]

    # Align heatmap color thresholds with _rate_color thresholds
    zmin, zmax = 30, 90
    yellow_pos = (RATE_YELLOW_THRESHOLD * 100 - zmin) / (zmax - zmin)
    green_pos = (RATE_GREEN_THRESHOLD * 100 - zmin) / (zmax - zmin)

    fig = go.Figure(go.Heatmap(
        z=z_sorted,
        x=[f"Week {c.split('_')[1]}" for c in week_cols],
        y=issuers_sorted,
        colorscale=[
            [0.0, COLOR_RED],
            [yellow_pos, COLOR_YELLOW],
            [green_pos, COLOR_GREEN],
            [1.0, COLOR_GREEN],
        ],
        zmin=zmin, zmax=zmax,
        text=[[f"{v:.1f}%" for v in row] for row in z_sorted],
        texttemplate="%{text}", textfont=dict(size=11),
        hovertemplate="Issuer: %{y}<br>%{x}: %{z:.1f}%<extra></extra>",
        colorbar=dict(title="Rate %", ticksuffix="%"),
    ))

    # No autorange="reversed" — sort order already puts worst at index 0 (top)
    fig.update_layout(
        title=dict(text="Issuer Bank Approval Rates by Week", font=dict(size=18, color=COLOR_DARK_BLUE)),
        template="plotly_white", height=400,
        margin=dict(t=60, b=40, l=120),
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

    fig = go.Figure(go.Waterfall(
        x=factors, y=values, measure=measures,
        text=[f"{v:+.0f}pp" if m == "relative" else f"{v:.0f}%" for v, m in zip(values, measures)],
        textposition="outside", textfont=dict(size=12),
        connector=dict(line=dict(color=COLOR_LIGHT_GRAY, width=2)),
        decreasing=dict(marker=dict(color=COLOR_RED)),
        increasing=dict(marker=dict(color=COLOR_GREEN)),
        totals=dict(marker=dict(color=COLOR_DARK_BLUE)),
    ))

    fig.update_layout(
        title=dict(text="Root Cause Waterfall: What Drove the Decline?", font=dict(size=18, color=COLOR_DARK_BLUE)),
        yaxis=dict(title="Approval Rate (%)", ticksuffix="%", range=[50, 90]),
        template="plotly_white", height=400,
        margin=dict(t=60, b=60), showlegend=False,
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
        ("weeks_1_3", "approved", "W1-3 Approved", COLOR_GREEN),
        ("weeks_1_3", "declined", "W1-3 Declined", COLOR_YELLOW),
        ("weeks_4_6", "approved", "W4-6 Approved", COLOR_BLUE),
        ("weeks_4_6", "declined", "W4-6 Declined", COLOR_RED),
    ]
    for period, status, name, color in combos:
        subset = df.filter((pl.col("period") == period) & (pl.col("status") == status))
        fig.add_trace(go.Box(
            y=subset["amount_usd"].to_list(), name=name,
            marker_color=color, opacity=0.78, boxmean=True,
        ))

    fig.add_annotation(
        text="Declined txns in W4-6 skew<br>toward higher amounts",
        xref="paper", yref="paper", x=0.95, y=0.95, showarrow=False,
        font=dict(size=11, color=COLOR_RED),
        bgcolor="rgba(255,255,255,0.8)", bordercolor=COLOR_RED, borderwidth=1,
    )

    fig.update_layout(
        title=dict(text="Transaction Amount Distribution by Status", font=dict(size=18, color=COLOR_DARK_BLUE)),
        yaxis=dict(title="Amount (USD)", tickprefix="$"),
        template="plotly_white", height=400, margin=dict(t=60, b=40),
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
        mode="lines+markers", name="Weeks 1-3",
        line=dict(color=COLOR_BLUE, width=2), marker=dict(size=6),
    ))
    fig.add_trace(go.Scatter(
        x=late["hour_of_day"].to_list(),
        y=[r * 100 for r in late["approval_rate"].to_list()],
        mode="lines+markers", name="Weeks 4-6",
        line=dict(color=COLOR_RED, width=2), marker=dict(size=6),
    ))

    fig.add_vrect(
        x0=16.5, x1=20.5, fillcolor=COLOR_RED, opacity=0.08, line_width=0,
        annotation_text="Evening Dip", annotation_position="top left",
        annotation_font_color=COLOR_RED,
    )

    fig.update_layout(
        title=dict(text="Approval Rate by Hour of Day", font=dict(size=18, color=COLOR_DARK_BLUE)),
        xaxis=dict(title="Hour of Day", tickmode="linear", dtick=2, range=[-0.5, 23.5]),
        yaxis=dict(title="Approval Rate (%)", ticksuffix="%", range=[55, 90]),
        template="plotly_white", height=400, margin=dict(t=60, b=40),
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
        fig.write_image(path, width=1200, height=400, scale=2)
        paths.append(path)
        print(f"  Exported {path}")
    return paths


def _build_insight_cards_html(insights_df: pl.DataFrame, max_cards: int = 5) -> str:
    """Build HTML insight cards from a DataFrame (data-driven, XSS-safe)."""
    cards = []
    for i in range(min(len(insights_df), max_cards)):
        row = insights_df.row(i, named=True)
        sev = html.escape(str(row["severity"]))
        title_text = html.escape(str(row["title"]))
        baseline_pct = f"{row['baseline_rate'] * 100:.0f}%"
        current_pct = f"{row['current_rate'] * 100:.0f}%"
        change_pp = f"{row['rate_change'] * 100:+.0f}pp"
        impact = f"${row['estimated_revenue_impact_usd'] / 1000:.0f}K/month"
        txns = f"{row['affected_transactions']:,}"
        cards.append(
            f'<div class="insight-card {sev}">'
            f'<span class="badge {sev}">{sev.upper()}</span>'
            f"<h3>{title_text}</h3>"
            f'<div class="metric">{baseline_pct} &rarr; {current_pct} ({change_pp})</div>'
            f'<div class="impact">Est. impact: {impact} | {txns} txns affected</div>'
            f"</div>"
        )
    return "\n".join(cards)


def _build_kpi_html(insights_df: pl.DataFrame, weekly_df: pl.DataFrame) -> str:
    """Build KPI row HTML from actual data."""
    if len(weekly_df) > 0:
        sorted_df = weekly_df.sort("week_number")
        current_rate = sorted_df["approval_rate"][-1]
        baseline_rate = sorted_df["approval_rate"][0]
    else:
        current_rate = DEGRADED_APPROVAL_RATE
        baseline_rate = BASELINE_APPROVAL_RATE

    decline_pp = round((current_rate - baseline_rate) * 100)
    if len(insights_df) > 0 and "estimated_revenue_impact_usd" in insights_df.columns:
        total_impact = sum(insights_df["estimated_revenue_impact_usd"].to_list())
    else:
        total_impact = 430000

    return (
        '<div class="kpi-row">'
        f'<div class="kpi"><div class="value" style="color:#e74c3c">{current_rate * 100:.0f}%</div>'
        '<div class="label">Current Approval Rate</div></div>'
        f'<div class="kpi"><div class="value" style="color:#2c3e50">{baseline_rate * 100:.0f}%</div>'
        '<div class="label">Baseline (Weeks 1-3)</div></div>'
        f'<div class="kpi"><div class="value" style="color:#e74c3c">{decline_pp:+d}pp</div>'
        '<div class="label">Rate Decline</div></div>'
        f'<div class="kpi"><div class="value" style="color:#e67e22">~${total_impact / 1_000_000:.1f}M</div>'
        '<div class="label">Est. Monthly Impact</div></div>'
        "</div>"
    )


_HTML_TEMPLATE = Template("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VoltMarket Transaction Intelligence Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
    <style>
        body { font-family: 'Segoe UI', Arial, sans-serif; background: #f8f9fa; margin: 0; padding: 20px; }
        .header { background: linear-gradient(135deg, #2c3e50, #3498db); color: white; padding: 30px; border-radius: 12px; margin-bottom: 24px; }
        .header h1 { margin: 0 0 8px 0; font-size: 28px; }
        .header .subtitle { font-size: 16px; opacity: 0.9; }
        .kpi-row { display: flex; gap: 16px; margin-bottom: 24px; }
        .kpi { flex: 1; background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center; }
        .kpi .value { font-size: 32px; font-weight: 700; }
        .kpi .label { font-size: 13px; color: #7f8c8d; margin-top: 4px; }
        .chart-container { background: white; border-radius: 10px; padding: 16px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
        .row { display: flex; gap: 20px; }
        .row > .chart-container { flex: 1; }
        .insight-row { display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }
        .insight-card { flex: 1; min-width: 200px; background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.08); border-left: 4px solid; }
        .insight-card.critical { border-left-color: #e74c3c; }
        .insight-card.high { border-left-color: #e67e22; }
        .insight-card.medium { border-left-color: #f1c40f; }
        .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: 700; text-transform: uppercase; color: white; }
        .badge.critical { background: #e74c3c; }
        .badge.high { background: #e67e22; }
        .badge.medium { background: #f1c40f; color: #2c3e50; }
        .insight-card h3 { margin: 8px 0 4px; font-size: 15px; }
        .insight-card .metric { font-size: 20px; font-weight: 700; color: #e74c3c; }
        .insight-card .impact { font-size: 12px; color: #7f8c8d; }
    </style>
</head>
<body>
    <div class="header">
        <h1>VoltMarket Transaction Intelligence Report</h1>
        <div class="subtitle">Approval Rate Analysis &mdash; 6-Week Period</div>
    </div>

    $kpi_section

    <div class="chart-container">$chart_0</div>

    <div class="row">
        <div class="chart-container">$chart_1</div>
        <div class="chart-container">$chart_2</div>
    </div>

    <h2 style="color:#2c3e50; margin: 24px 0 12px;">Key Findings</h2>
    <div class="insight-row">$insight_cards</div>

    <div class="row">
        <div class="chart-container">$chart_3</div>
        <div class="chart-container">$chart_4</div>
    </div>

    <div class="chart-container">$chart_5</div>

    <div style="text-align:center; color:#95a5a6; padding:20px; font-size:12px;">
        Generated by VoltMarket Transaction Intelligence Engine | Data period: Weeks 1-6
    </div>
</body>
</html>""")


def export_standalone_html(output_dir: str = "output") -> str:
    """Export a standalone HTML file combining all charts."""
    os.makedirs(output_dir, exist_ok=True)

    charts_html = []
    for name, fn in ALL_CHARTS.items():
        fig = fn()
        charts_html.append(fig.to_html(full_html=False, include_plotlyjs=False))

    insights_df = load_insights()
    weekly_df = _load_or_mock(WEEKLY_TREND_OUTPUT_PATH, build_mock_weekly_trends)

    html_content = _HTML_TEMPLATE.substitute(
        kpi_section=_build_kpi_html(insights_df, weekly_df),
        chart_0=charts_html[0],
        chart_1=charts_html[1],
        chart_2=charts_html[2],
        chart_3=charts_html[3],
        chart_4=charts_html[4],
        chart_5=charts_html[5],
        insight_cards=_build_insight_cards_html(insights_df),
    )

    path = os.path.join(output_dir, "dashboard.html")
    with open(path, "w") as f:
        f.write(html_content)
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
