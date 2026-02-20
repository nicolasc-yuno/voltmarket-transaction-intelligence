"""
Anomaly Detection for VoltMarket Transaction Intelligence Engine.

Compares weeks 1-3 (baseline) vs weeks 4-6 (current) per segment,
calculates z-scores, p-values, and flags statistically significant anomalies.
"""

import os
import numpy as np
import polars as pl

try:
    from scipy import stats as _scipy_stats
    _SCIPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    _scipy_stats = None
    _SCIPY_AVAILABLE = False

from src.contracts.schemas import (
    SEGMENT_SCHEMA,
    ANOMALY_SCHEMA,
    ANOMALIES_OUTPUT_PATH,
    SEGMENT_OUTPUT_PATH,
    BASELINE_APPROVAL_RATE,
    DEGRADED_APPROVAL_RATE,
    COUNTRIES,
    CARD_BRANDS,
    ISSUER_BANKS,
    WEEKS_PER_PERIOD,
    WEEKS_PER_MONTH,
)


def _generate_mock_segments() -> pl.DataFrame:
    """
    Generate realistic mock segment data when pipeline output doesn't exist yet.
    Embeds the three key anomaly patterns from the project narrative:
      1. BBVA Mexico collapse
      2. High-value transaction degradation
      3. Evening hour underperformance
    """
    rows = []

    # --- Issuer-level segments (composite: country|brand|type|issuer) ---
    issuer_segments = [
        # (segment_key, baseline_rate, current_rate, txns_baseline, txns_current, avg_ticket_usd)
        # BBVA MX: 85pp drop, 1200 txns, $85 avg ticket → ~$180K monthly impact (#1 target)
        ("MX|Mastercard|Debit|BBVA",        0.85, 0.44, 1180, 1200, 85.0),
        ("MX|Visa|Credit|Banorte",          0.82, 0.74,  650,  660, 55.0),
        ("MX|Visa|Credit|Santander MX",     0.81, 0.73,  420,  430, 52.0),
        ("MX|Mastercard|Credit|Citibanamex",0.80, 0.72,  380,  390, 48.0),
        ("MX|Visa|Debit|HSBC MX",           0.79, 0.71,  310,  315, 34.0),
        ("BR|Visa|Credit|Itau",             0.83, 0.69,  900,  910, 45.0),
        ("BR|Mastercard|Debit|Bradesco",    0.82, 0.68,  750,  760, 36.0),
        ("BR|Visa|Credit|Nubank",           0.85, 0.71,  680,  690, 42.0),
        ("BR|Mastercard|Credit|Santander BR",0.81,0.67,  520,  530, 50.0),
        ("BR|Visa|Debit|Caixa",             0.80, 0.66,  480,  490, 30.0),
        ("CO|Visa|Credit|Bancolombia",      0.82, 0.68,  400,  410, 40.0),
        ("CO|Mastercard|Debit|Davivienda",  0.80, 0.66,  300,  305, 32.0),
        ("CO|Visa|Credit|Banco de Bogota",  0.79, 0.65,  250,  255, 38.0),
        ("CO|Mastercard|Credit|BBVA CO",    0.78, 0.64,  220,  225, 44.0),
    ]

    for seg_key, bl_rate, cur_rate, bl_txns, cur_txns, avg_ticket in issuer_segments:
        for period, rate, total_txns in [("weeks_1_3", bl_rate, bl_txns), ("weeks_4_6", cur_rate, cur_txns)]:
            approved = int(total_txns * rate)
            rows.append({
                "segment_type": "composite",
                "segment_key": seg_key,
                "dimension_1": seg_key.split("|")[0],
                "dimension_2": seg_key.split("|")[1] if len(seg_key.split("|")) > 1 else None,
                "dimension_3": seg_key.split("|")[2] if len(seg_key.split("|")) > 2 else None,
                "period": period,
                "total_transactions": total_txns,
                "approved_transactions": approved,
                "declined_transactions": total_txns - approved,
                "approval_rate": rate,
                "total_amount_usd": total_txns * avg_ticket,
                "approved_amount_usd": approved * avg_ticket,
            })

    # --- Amount bucket segments ---
    amount_buckets = [
        ("$10-50",   0.84, 0.72, 1200, 1220, 28.0),
        ("$50-100",  0.83, 0.69, 980,  990,  72.0),
        ("$100-200", 0.81, 0.61, 620,  630,  145.0),  # High-value degradation
        ("$200-350", 0.79, 0.54, 380,  385,  265.0),  # High-value degradation (worse)
        ("$350-500", 0.77, 0.50, 180,  182,  415.0),  # High-value degradation (worst)
    ]
    for seg_key, bl_rate, cur_rate, bl_txns, cur_txns, avg_ticket in amount_buckets:
        for period, rate, total_txns in [("weeks_1_3", bl_rate, bl_txns), ("weeks_4_6", cur_rate, cur_txns)]:
            approved = int(total_txns * rate)
            rows.append({
                "segment_type": "amount_bucket",
                "segment_key": seg_key,
                "dimension_1": seg_key,
                "dimension_2": None,
                "dimension_3": None,
                "period": period,
                "total_transactions": total_txns,
                "approved_transactions": approved,
                "declined_transactions": total_txns - approved,
                "approval_rate": rate,
                "total_amount_usd": total_txns * avg_ticket,
                "approved_amount_usd": approved * avg_ticket,
            })

    # --- Hour bucket segments ---
    hour_buckets = [
        ("morning_6_12",    0.84, 0.71, 800, 810, 45.0),
        ("afternoon_12_17", 0.83, 0.70, 950, 960, 47.0),
        ("evening_17_20",   0.82, 0.55, 700, 710, 50.0),  # Evening underperformance
        ("night_20_24",     0.80, 0.63, 600, 605, 43.0),
        ("late_night_0_6",  0.78, 0.64, 200, 202, 38.0),
    ]
    for seg_key, bl_rate, cur_rate, bl_txns, cur_txns, avg_ticket in hour_buckets:
        for period, rate, total_txns in [("weeks_1_3", bl_rate, bl_txns), ("weeks_4_6", cur_rate, cur_txns)]:
            approved = int(total_txns * rate)
            rows.append({
                "segment_type": "hour_bucket",
                "segment_key": seg_key,
                "dimension_1": seg_key,
                "dimension_2": None,
                "dimension_3": None,
                "period": period,
                "total_transactions": total_txns,
                "approved_transactions": approved,
                "declined_transactions": total_txns - approved,
                "approval_rate": rate,
                "total_amount_usd": total_txns * avg_ticket,
                "approved_amount_usd": approved * avg_ticket,
            })

    # --- Country-level segments ---
    country_segments = [
        ("MX", 0.82, 0.63, 2100, 2130, 45.0),
        ("BR", 0.83, 0.66, 3400, 3450, 40.0),
        ("CO", 0.80, 0.65, 1200, 1210, 38.0),
    ]
    for seg_key, bl_rate, cur_rate, bl_txns, cur_txns, avg_ticket in country_segments:
        for period, rate, total_txns in [("weeks_1_3", bl_rate, bl_txns), ("weeks_4_6", cur_rate, cur_txns)]:
            approved = int(total_txns * rate)
            rows.append({
                "segment_type": "country",
                "segment_key": seg_key,
                "dimension_1": seg_key,
                "dimension_2": None,
                "dimension_3": None,
                "period": period,
                "total_transactions": total_txns,
                "approved_transactions": approved,
                "declined_transactions": total_txns - approved,
                "approval_rate": rate,
                "total_amount_usd": total_txns * avg_ticket,
                "approved_amount_usd": approved * avg_ticket,
            })

    schema = {k: v for k, v in SEGMENT_SCHEMA.items()}
    df = pl.DataFrame(rows, schema=schema)
    return df


def load_segments(path: str = SEGMENT_OUTPUT_PATH) -> tuple[pl.DataFrame, bool]:
    """Load segment data. Returns (dataframe, is_mock)."""
    if os.path.exists(path):
        df = pl.read_parquet(path)
        return df, False
    print(f"[anomaly_detection] '{path}' not found — generating mock segment data.")
    return _generate_mock_segments(), True


def _proportions_z_test(count1: int, nobs1: int, count2: int, nobs2: int) -> float:
    """Two-proportion z-test, returns p-value."""
    if nobs1 == 0 or nobs2 == 0:
        return 1.0
    p1 = count1 / nobs1
    p2 = count2 / nobs2
    p_pool = (count1 + count2) / (nobs1 + nobs2)
    if p_pool in (0.0, 1.0):
        return 1.0
    se = (p_pool * (1 - p_pool) * (1 / nobs1 + 1 / nobs2)) ** 0.5
    if se == 0:
        return 1.0
    z = (p1 - p2) / se
    abs_z = abs(z)

    if _SCIPY_AVAILABLE:
        p_value = 2 * (1 - _scipy_stats.norm.cdf(abs_z))
    else:
        # Rational approximation to Φ(x) (Abramowitz & Stegun, max error < 7.5e-8)
        t = 1.0 / (1.0 + 0.2316419 * abs_z)
        poly = t * (0.319381530 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))))
        p_value = 2.0 * poly * np.exp(-0.5 * abs_z ** 2) / (2 * np.pi) ** 0.5

    return float(p_value)


def _deduplicate_by_segment_key(df: pl.DataFrame) -> pl.DataFrame:
    """
    When the pipeline produces multiple segment_types for the same segment_key
    (e.g. 'MX|BBVA' as both 'issuer' and 'country_issuer'), keep one row per
    segment_key — preferring the most specific segment_type by priority order.
    """
    type_priority = {
        "composite": 0,
        "issuer_brand_type": 1,
        "country_brand_type": 2,
        "country_issuer_brand": 3,
        "country_issuer": 4,
        "issuer_brand": 5,
        "country_brand": 6,
        "issuer": 7,
        "country_card_type": 8,
        "country": 9,
        "card_brand": 10,
        "card_type": 11,
        "amount_bucket": 12,
        "hour_bucket": 13,
    }
    priority_col = pl.Series(
        "type_priority",
        [type_priority.get(t, 99) for t in df["segment_type"].to_list()],
    )
    return (
        df.with_columns(priority_col)
        .sort("type_priority")
        .unique(subset=["segment_key"], keep="first")
        .drop("type_priority")
    )


def detect_anomalies(segments: pl.DataFrame) -> pl.DataFrame:
    """
    Compare weeks_1_3 vs weeks_4_6 per segment and compute anomaly scores.

    Returns a DataFrame matching ANOMALY_SCHEMA.
    """
    baseline_raw = segments.filter(pl.col("period") == "weeks_1_3").select([
        "segment_key", "segment_type",
        pl.col("approval_rate").alias("baseline_rate"),
        pl.col("approved_transactions").alias("baseline_approved"),
        pl.col("total_transactions").alias("baseline_total"),
        pl.col("total_amount_usd").alias("baseline_amount_usd"),
    ])
    current_raw = segments.filter(pl.col("period") == "weeks_4_6").select([
        "segment_key",
        "segment_type",
        pl.col("approval_rate").alias("current_rate"),
        pl.col("approved_transactions").alias("current_approved"),
        pl.col("total_transactions").alias("current_total"),
        pl.col("total_amount_usd").alias("current_amount_usd"),
    ])

    # Deduplicate so each segment_key appears once per period
    baseline = _deduplicate_by_segment_key(baseline_raw)
    current = _deduplicate_by_segment_key(current_raw).drop("segment_type")

    # Left join: keeps segments present in baseline even if absent in weeks 4-6
    # (e.g. a segment that disappeared — rate dropped to 0).
    joined = baseline.join(current, on="segment_key", how="left")

    # Fill nulls for segments with no current-period data (rate = 0, txns = 0)
    joined = joined.with_columns([
        pl.col("current_rate").fill_null(0.0),
        pl.col("current_approved").fill_null(0),
        pl.col("current_total").fill_null(0),
        pl.col("current_amount_usd").fill_null(0.0),
    ])

    # Rate change
    joined = joined.with_columns(
        (pl.col("current_rate") - pl.col("baseline_rate")).alias("rate_change")
    )

    # Z-score: how many SDs this segment's change is from the mean change
    rate_changes = joined["rate_change"].to_numpy()
    mean_change = float(np.mean(rate_changes))
    std_change = float(np.std(rate_changes))

    if std_change == 0:
        z_scores = np.zeros_like(rate_changes)
    else:
        z_scores = (rate_changes - mean_change) / std_change

    joined = joined.with_columns(
        pl.Series("z_score", z_scores.tolist(), dtype=pl.Float64)
    )

    # Affected transactions = current period total
    joined = joined.with_columns(
        pl.col("current_total").alias("affected_transactions")
    )

    # Average ticket USD (from current period)
    # Guard against div-by-zero for segments that disappeared in weeks 4-6
    # (current_total == 0 due to left-join null fill).
    joined = joined.with_columns(
        pl.when(pl.col("current_total") > 0)
        .then(pl.col("current_amount_usd") / pl.col("current_total"))
        .otherwise(0.0)
        .alias("avg_ticket_usd")
    )

    # Revenue impact: weekly_txns * avg_ticket * |rate_change| * weeks_per_month
    # Divide by WEEKS_PER_PERIOD (3) because affected_transactions covers a 3-week
    # window; we want the per-week rate before scaling to a monthly figure.
    joined = joined.with_columns(
        (
            (pl.col("affected_transactions") / WEEKS_PER_PERIOD)
            * pl.col("avg_ticket_usd")
            * pl.col("rate_change").abs()
            * WEEKS_PER_MONTH
        ).alias("estimated_revenue_impact_usd")
    )

    # P-values (proportions z-test per row)
    p_values = []
    for row in joined.iter_rows(named=True):
        p = _proportions_z_test(
            int(row["baseline_approved"]),
            int(row["baseline_total"]),
            int(row["current_approved"]),
            int(row["current_total"]),
        )
        p_values.append(p)

    joined = joined.with_columns(
        pl.Series("p_value", p_values, dtype=pl.Float64)
    )

    # Flag anomaly: (|z_score| > 2.0 OR p_value < 0.05) AND affected_transactions > 50
    joined = joined.with_columns(
        (
            ((pl.col("z_score").abs() > 2.0) | (pl.col("p_value") < 0.05))
            & (pl.col("affected_transactions") > 50)
        ).alias("is_anomaly")
    )

    # Select final columns matching ANOMALY_SCHEMA
    result = joined.select([
        "segment_key",
        "segment_type",
        "baseline_rate",
        "current_rate",
        "rate_change",
        "z_score",
        "p_value",
        "is_anomaly",
        pl.col("affected_transactions").cast(pl.Int64),
        "estimated_revenue_impact_usd",
    ])

    return result


def save_anomalies(anomalies: pl.DataFrame, path: str = ANOMALIES_OUTPUT_PATH) -> None:
    """Save anomalies DataFrame to parquet."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    anomalies.write_parquet(path)
    print(f"[anomaly_detection] Saved {len(anomalies)} anomaly records to '{path}'")


def run() -> pl.DataFrame:
    """Full anomaly detection pipeline. Returns anomalies DataFrame."""
    segments, is_mock = load_segments()
    anomalies = detect_anomalies(segments)
    save_anomalies(anomalies)

    n_anomalies = anomalies.filter(pl.col("is_anomaly")).height
    print(f"[anomaly_detection] Detected {n_anomalies} anomalies out of {len(anomalies)} segments.")
    if is_mock:
        print("[anomaly_detection] NOTE: Running on mock data — real pipeline output not yet available.")
    return anomalies


if __name__ == "__main__":
    run()
