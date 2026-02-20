"""
Insight Generation for VoltMarket Transaction Intelligence Engine.

Ranks anomalies into top 3-5 actionable insights using a weighted score:
  - Revenue impact   40%
  - Rate change mag  30%
  - Statistical sig  20%
  - Breadth          10%

Outputs: insights.parquet (INSIGHT_SCHEMA) and insights.json.
"""

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import polars as pl

from src.contracts.schemas import (
    INSIGHT_SCHEMA,
    ANOMALIES_OUTPUT_PATH,
    INSIGHTS_OUTPUT_PATH,
    INSIGHTS_JSON_PATH,
    SUMMARY_OUTPUT_PATH,
)


# Segment-key to human-readable label mapping for titles/descriptions
_SEGMENT_LABELS = {
    "MX|Mastercard|Debit|BBVA":         "BBVA Mexico (Mastercard Debit)",
    "MX|BBVA":                           "BBVA Mexico",
    "MX|BBVA|Mastercard|Debit":         "BBVA Mexico (Mastercard Debit)",
    "MX|BBVA|Visa|Credit":              "BBVA Mexico (Visa Credit)",
    "$10-50":                             "Low-value ($10-$50)",
    "$50-100":                            "Mid-value ($50-$100)",
    "$100-200":                           "High-value ($100-$200)",
    "$200-350":                           "High-value ($200-$350)",
    "$350-500":                           "Premium ($350-$500)",
    "evening_17_20":                      "Evening (17h-20h)",
    "night_20_24":                        "Night (20h-24h)",
    "late_night_0_6":                     "Late Night (0h-6h)",
    "morning_6_12":                       "Morning (6h-12h)",
    "afternoon_12_17":                    "Afternoon (12h-17h)",
    "MX":                                 "Mexico",
    "BR":                                 "Brazil",
    "CO":                                 "Colombia",
    "Credit":                             "Credit Cards",
    "Debit":                              "Debit Cards",
    "Visa":                               "Visa",
    "Mastercard":                         "Mastercard",
    "Amex":                               "Amex",
}


def _severity(impact_usd: float) -> str:
    # Thresholds calibrated to the corrected monthly revenue impact formula
    # (weekly txns × avg_ticket × |rate_change| × 4.33).
    if impact_usd > 50_000:
        return "critical"
    if impact_usd > 20_000:
        return "high"
    if impact_usd > 5_000:
        return "medium"
    return "low"


def _build_title(row: dict) -> str:
    seg = row["segment_key"]
    label = _SEGMENT_LABELS.get(seg, seg)
    rate_change_pct = abs(row["rate_change"]) * 100
    if row["rate_change"] < 0:
        return f"{label} Approval Rate Collapsed ({rate_change_pct:.0f}pp Drop)"
    return f"{label} Approval Rate Anomaly ({rate_change_pct:.0f}pp Change)"


def _build_description(row: dict) -> str:
    seg = row["segment_key"]
    label = _SEGMENT_LABELS.get(seg, seg)
    baseline_pct = row["baseline_rate"] * 100
    current_pct = row["current_rate"] * 100
    change_pct = abs(row["rate_change"]) * 100
    impact = row["estimated_revenue_impact_usd"]
    txns = row["affected_transactions"]
    p = row["p_value"]
    z = row["z_score"]

    direction = "dropped" if row["rate_change"] < 0 else "rose"
    return (
        f"{label}'s approval rate {direction} from {baseline_pct:.1f}% to "
        f"{current_pct:.1f}% starting Week 4, a {change_pct:.1f}pp shift affecting "
        f"{txns:,} transactions per 3-week period. Estimated monthly revenue impact: "
        f"${impact:,.0f}. Statistical significance: p={p:.4f}, z={z:.2f}."
    )


def _normalise_column(series: pl.Series) -> pl.Series:
    """Min-max normalise a series to [0, 1]. Returns zeros if all equal."""
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pl.Series([0.0] * len(series))
    return (series - mn) / (mx - mn)


def rank_insights(anomalies: pl.DataFrame, top_n: int = 5, min_n: int = 3) -> pl.DataFrame:
    """
    From the anomaly DataFrame, select flagged anomalies, compute a weighted
    score, and return the top_n as a DataFrame matching INSIGHT_SCHEMA.

    Uses a specificity bonus so root-cause segments (issuer-level, composite)
    rank above broad symptom segments (card_brand, country, card_type).
    Enforces diversity: at most one insight per segment_type category.
    """
    flagged = anomalies.filter(pl.col("is_anomaly")).clone()

    if len(flagged) < min_n:
        needed = min_n - len(flagged)
        rest = (
            anomalies.filter(~pl.col("is_anomaly"))
            .sort("estimated_revenue_impact_usd", descending=True)
            .head(needed)
        )
        flagged = pl.concat([flagged, rest])

    # ---- Specificity bonus: root-cause segments score higher ----
    specificity_map = {
        "issuer_brand_type": 1.0, "composite": 1.0,
        "country_issuer": 0.9, "issuer": 0.85,
        "country_brand_type": 0.7, "country_brand": 0.6,
        "amount_bucket": 0.8, "hour_bucket": 0.8,
        "country": 0.3, "card_brand": 0.2, "card_type": 0.2,
    }
    specificity_scores = [
        specificity_map.get(t, 0.5) for t in flagged["segment_type"].to_list()
    ]

    # ---- Normalised sub-scores ----
    impact_norm = _normalise_column(flagged["estimated_revenue_impact_usd"])
    magnitude_norm = _normalise_column(flagged["rate_change"].abs())
    significance_norm = _normalise_column(1.0 - flagged["p_value"])
    specificity_norm = pl.Series(specificity_scores)

    weighted_score = (
        0.30 * impact_norm
        + 0.30 * magnitude_norm
        + 0.15 * significance_norm
        + 0.25 * specificity_norm
    )

    flagged = flagged.with_columns(
        pl.Series("_score", weighted_score.to_list(), dtype=pl.Float64)
    )

    # ---- Diverse selection: pick top per segment category, then fill ----
    # Group segment types into categories for diversity
    category_map = {
        "issuer": "issuer", "country_issuer": "issuer",
        "issuer_brand_type": "issuer", "composite": "issuer",
        "amount_bucket": "amount", "hour_bucket": "time",
        "country": "geo", "country_brand": "geo",
        "country_brand_type": "geo",
        "card_brand": "instrument", "card_type": "instrument",
    }
    categories = [category_map.get(t, "other") for t in flagged["segment_type"].to_list()]
    flagged = flagged.with_columns(pl.Series("_category", categories))

    sorted_flagged = flagged.sort("_score", descending=True)
    selected_indices = []
    seen_categories: set[str] = set()

    # First pass: pick the best from each category
    for i, row in enumerate(sorted_flagged.iter_rows(named=True)):
        cat = row["_category"]
        if cat not in seen_categories:
            selected_indices.append(i)
            seen_categories.add(cat)
        if len(selected_indices) >= top_n:
            break

    # Second pass: fill remaining slots with highest scoring
    if len(selected_indices) < top_n:
        for i in range(len(sorted_flagged)):
            if i not in selected_indices:
                selected_indices.append(i)
            if len(selected_indices) >= top_n:
                break

    flagged = sorted_flagged[sorted(selected_indices)].sort("_score", descending=True).drop("_category")

    # Build insight rows
    rows = []
    for rank, row in enumerate(flagged.iter_rows(named=True), start=1):
        rows.append({
            "rank": rank,
            "insight_id": str(uuid.uuid4()),
            "title": _build_title(row),
            "description": _build_description(row),
            "segment_key": row["segment_key"],
            "baseline_rate": row["baseline_rate"],
            "current_rate": row["current_rate"],
            "rate_change": row["rate_change"],
            "affected_transactions": int(row["affected_transactions"]),
            "estimated_revenue_impact_usd": row["estimated_revenue_impact_usd"],
            "severity": _severity(row["estimated_revenue_impact_usd"]),
        })

    schema = {k: v for k, v in INSIGHT_SCHEMA.items()}
    return pl.DataFrame(rows, schema=schema)


def save_insights(insights: pl.DataFrame) -> None:
    """Save insights to both parquet and JSON."""
    os.makedirs(os.path.dirname(INSIGHTS_OUTPUT_PATH), exist_ok=True)

    insights.write_parquet(INSIGHTS_OUTPUT_PATH)
    print(f"[insights] Saved {len(insights)} insights to '{INSIGHTS_OUTPUT_PATH}'")

    # JSON — convert to list of dicts, coerce types for JSON serialisability
    records = []
    for row in insights.iter_rows(named=True):
        records.append({
            k: (int(v) if isinstance(v, (pl.Series,)) else v)
            for k, v in row.items()
        })
    with open(INSIGHTS_JSON_PATH, "w") as fh:
        json.dump(records, fh, indent=2, default=str)
    print(f"[insights] Saved JSON to '{INSIGHTS_JSON_PATH}'")


def build_summary(anomalies: pl.DataFrame, insights: pl.DataFrame) -> dict:
    """Build summary statistics dict."""
    # Overall rates from anomalies (simple average across segments)
    overall_baseline = float(anomalies["baseline_rate"].mean())
    overall_current = float(anomalies["current_rate"].mean())
    total_impact = float(insights["estimated_revenue_impact_usd"].sum())

    severity_counts = (
        insights.group_by("severity")
        .agg(pl.len().alias("n"))
        .to_dicts()
    )
    sev_map = {r["severity"]: r["n"] for r in severity_counts}

    return {
        "overall_baseline_rate": round(overall_baseline, 4),
        "overall_current_rate": round(overall_current, 4),
        "total_rate_change": round(overall_current - overall_baseline, 4),
        "total_monthly_revenue_impact_usd": round(total_impact, 2),
        "anomalies_detected": int(anomalies.filter(pl.col("is_anomaly")).height),
        "critical_insights": sev_map.get("critical", 0),
        "high_insights": sev_map.get("high", 0),
        "medium_insights": sev_map.get("medium", 0),
        "low_insights": sev_map.get("low", 0),
        "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def save_summary(summary: dict, path: str = SUMMARY_OUTPUT_PATH) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(summary, fh, indent=2)
    print(f"[insights] Summary saved to '{path}'")


def run(anomalies: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Full insight generation pipeline. Returns insights DataFrame."""
    if anomalies is None:
        if not os.path.exists(ANOMALIES_OUTPUT_PATH):
            raise FileNotFoundError(
                f"Anomalies file not found at '{ANOMALIES_OUTPUT_PATH}'. "
                "Run anomaly_detection first."
            )
        anomalies = pl.read_parquet(ANOMALIES_OUTPUT_PATH)

    insights = rank_insights(anomalies)
    save_insights(insights)

    summary = build_summary(anomalies, insights)
    save_summary(summary)

    return insights


if __name__ == "__main__":
    run()
