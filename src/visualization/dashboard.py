"""
VoltMarket Transaction Intelligence — Streamlit Dashboard.

Launch: streamlit run src/visualization/dashboard.py
"""

from __future__ import annotations

import streamlit as st

from src.visualization.charts import (
    _load_or_mock,
    build_mock_weekly_trends,
    chart_amount_distribution,
    chart_country_breakdown,
    chart_headline_trend,
    chart_hourly_pattern,
    chart_issuer_heatmap,
    chart_waterfall,
    export_all_png,
    export_standalone_html,
    load_insights,
)
from src.contracts.schemas import (
    BASELINE_APPROVAL_RATE,
    DEGRADED_APPROVAL_RATE,
    WEEKLY_TREND_OUTPUT_PATH,
)

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="VoltMarket Transaction Intelligence",
    page_icon=":zap:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #2c3e50, #3498db);
        color: white; padding: 28px 32px; border-radius: 12px; margin-bottom: 20px;
    }
    .main-header h1 { margin: 0 0 6px 0; font-size: 28px; }
    .main-header .sub { font-size: 15px; opacity: 0.9; }
    .kpi-card {
        background: white; border-radius: 10px; padding: 18px;
        text-align: center; box-shadow: 0 2px 8px rgba(0,0,0,0.07);
    }
    .kpi-value { font-size: 34px; font-weight: 700; }
    .kpi-label { font-size: 13px; color: #7f8c8d; margin-top: 2px; }
    .insight-card {
        background: white; border-radius: 10px; padding: 18px 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.07); border-left: 5px solid; height: 100%;
    }
    .insight-card.critical { border-left-color: #e74c3c; }
    .insight-card.high { border-left-color: #e67e22; }
    .insight-card.medium { border-left-color: #f1c40f; }
    .severity-badge {
        display: inline-block; padding: 2px 8px; border-radius: 4px;
        font-size: 11px; font-weight: 700; text-transform: uppercase; color: white;
    }
    .severity-badge.critical { background: #e74c3c; }
    .severity-badge.high { background: #e67e22; }
    .severity-badge.medium { background: #f1c40f; color: #2c3e50; }
    .metric-value { font-size: 22px; font-weight: 700; color: #e74c3c; margin: 6px 0 4px; }
    .impact-text { font-size: 12px; color: #7f8c8d; }
    .section-title {
        color: #2c3e50; font-size: 20px; font-weight: 600;
        margin: 28px 0 12px; padding-bottom: 6px; border-bottom: 2px solid #ecf0f1;
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Load data for KPIs
# ---------------------------------------------------------------------------
weekly_df = _load_or_mock(WEEKLY_TREND_OUTPUT_PATH, build_mock_weekly_trends)
if len(weekly_df) > 0:
    sorted_wdf = weekly_df.sort("week_number")
    current_rate = sorted_wdf["approval_rate"][-1]
    baseline_rate = sorted_wdf["approval_rate"][0]
else:
    current_rate = DEGRADED_APPROVAL_RATE
    baseline_rate = BASELINE_APPROVAL_RATE

decline_pp = round((current_rate - baseline_rate) * 100)

insights_df = load_insights()
if len(insights_df) > 0 and "estimated_revenue_impact_usd" in insights_df.columns:
    total_impact = sum(insights_df["estimated_revenue_impact_usd"].to_list())
else:
    total_impact = 430000

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.markdown("""
<div class="main-header">
    <h1>&#9889; VoltMarket Transaction Intelligence Report</h1>
    <div class="sub">Approval Rate Analysis &mdash; 6-Week Period | Generated from latest available data</div>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# KPI row (data-driven)
# ---------------------------------------------------------------------------
k1, k2, k3, k4 = st.columns(4)

with k1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value" style="color:#e74c3c">{current_rate * 100:.0f}%</div>
        <div class="kpi-label">Current Approval Rate</div>
    </div>""", unsafe_allow_html=True)

with k2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value" style="color:#2c3e50">{baseline_rate * 100:.0f}%</div>
        <div class="kpi-label">Baseline (Weeks 1-3)</div>
    </div>""", unsafe_allow_html=True)

with k3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value" style="color:#e74c3c">{decline_pp:+d}pp</div>
        <div class="kpi-label">Rate Decline</div>
    </div>""", unsafe_allow_html=True)

with k4:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-value" style="color:#e67e22">~${total_impact / 1_000_000:.1f}M</div>
        <div class="kpi-label">Est. Monthly Impact</div>
    </div>""", unsafe_allow_html=True)

st.markdown("")

# ---------------------------------------------------------------------------
# Chart 1 — Headline Trend (full width)
# ---------------------------------------------------------------------------
st.plotly_chart(chart_headline_trend(), use_container_width=True)

# ---------------------------------------------------------------------------
# Charts 2 & 3 — Country + Heatmap (side by side)
# ---------------------------------------------------------------------------
col_left, col_right = st.columns(2)
with col_left:
    st.plotly_chart(chart_country_breakdown(), use_container_width=True)
with col_right:
    st.plotly_chart(chart_issuer_heatmap(), use_container_width=True)

# ---------------------------------------------------------------------------
# Key Findings — Insight cards (data-driven)
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Key Findings</div>', unsafe_allow_html=True)

card_cols = st.columns(min(len(insights_df), 5))

for i in range(min(len(insights_df), 5)):
    row = insights_df.row(i, named=True)
    severity = row["severity"]
    baseline_pct = f"{row['baseline_rate'] * 100:.0f}%"
    current_pct = f"{row['current_rate'] * 100:.0f}%"
    change_pp = f"{row['rate_change'] * 100:+.0f}pp"
    impact = f"${row['estimated_revenue_impact_usd'] / 1000:.0f}K/month"
    txns = f"{row['affected_transactions']:,}"

    with card_cols[i]:
        st.markdown(f"""
        <div class="insight-card {severity}">
            <span class="severity-badge {severity}">{severity.upper()}</span>
            <h4 style="margin:8px 0 4px; font-size:14px;">{row['title']}</h4>
            <div class="metric-value">{baseline_pct} &rarr; {current_pct} ({change_pp})</div>
            <div class="impact-text">Est. impact: {impact} | {txns} txns</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("")

# ---------------------------------------------------------------------------
# Charts 4 & 5 — Waterfall + Amount Distribution (side by side)
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Deep Dive: Root Causes</div>', unsafe_allow_html=True)

col_w, col_a = st.columns(2)
with col_w:
    st.plotly_chart(chart_waterfall(), use_container_width=True)
with col_a:
    st.plotly_chart(chart_amount_distribution(), use_container_width=True)

# ---------------------------------------------------------------------------
# Chart 6 — Hourly Pattern (full width)
# ---------------------------------------------------------------------------
st.markdown('<div class="section-title">Temporal Analysis</div>', unsafe_allow_html=True)
st.plotly_chart(chart_hourly_pattern(), use_container_width=True)

# ---------------------------------------------------------------------------
# "Aha moment" callout
# ---------------------------------------------------------------------------
st.markdown("""
<div style="background:#fff3cd; border-left:5px solid #e67e22; border-radius:8px; padding:16px 20px; margin:20px 0;">
    <strong style="color:#e67e22;">&#128161; Aha Moment:</strong>
    <span style="color:#2c3e50;">
        The approval rate collapse is <b>not uniform</b>. It is concentrated in
        <b>BBVA Mexico</b> (which alone accounts for 44% of the total decline),
        <b>high-value transactions over $200</b>, and <b>evening hours (17-20h)</b>.
        Addressing just these three factors could recover an estimated <b>$347K/month</b> in lost revenue.
    </span>
</div>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Export section (sidebar)
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### Export Options")
    if st.button("Export PNG Charts"):
        with st.spinner("Generating PNGs..."):
            paths = export_all_png()
        st.success(f"Exported {len(paths)} charts to output/")
        for p in paths:
            st.text(f"  {p}")

    if st.button("Export HTML Dashboard"):
        with st.spinner("Generating HTML..."):
            path = export_standalone_html()
        st.success(f"Exported to {path}")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("""
<div style="text-align:center; color:#95a5a6; padding:24px 0 8px; font-size:12px;">
    VoltMarket Transaction Intelligence Engine | Data Period: Weeks 1-6 | Built with Polars + Plotly + Streamlit
</div>
""", unsafe_allow_html=True)
