"""
Segmentation engine: compute approval rates across dimension combinations.
"""

import polars as pl

from src.contracts.schemas import SEGMENT_SCHEMA


# ---------------------------------------------------------------------------
# Segment definitions: name -> list of group-by column(s)
# ---------------------------------------------------------------------------
SEGMENT_DEFINITIONS = {
    "time_weekly":        ["week_number"],
    "country":            ["country"],
    "card_brand":         ["card_brand"],
    "card_type":          ["card_type"],
    "issuer":             ["country", "issuer_bank"],
    "amount_bucket":      ["amount_bucket"],
    "hour_bucket":        ["hour_bucket"],
    "country_brand":      ["country", "card_brand"],
    "country_brand_type": ["country", "card_brand", "card_type"],
    "issuer_brand_type":  ["country", "issuer_bank", "card_brand", "card_type"],
}


def _segment_key(dims: list[str]) -> pl.Expr:
    """Build a pipe-separated human-readable key from dimension columns."""
    if len(dims) == 1:
        return pl.col(dims[0]).cast(pl.Utf8)
    parts = [pl.col(d).cast(pl.Utf8) for d in dims]
    expr = parts[0]
    for p in parts[1:]:
        expr = pl.concat_str([expr, pl.lit("|"), p])
    return expr


def _dim_expr(dims: list[str], idx: int) -> pl.Expr:
    """Return dimension value or null if this segment has fewer than idx+1 dims."""
    if idx < len(dims):
        return pl.col(dims[idx]).cast(pl.Utf8).alias(f"dimension_{idx + 1}")
    return pl.lit(None).cast(pl.Utf8).alias(f"dimension_{idx + 1}")


def _compute_agg(group_df: pl.DataFrame, group_cols: list[str], period_value: str, segment_type: str) -> pl.DataFrame:
    """Aggregate a dataframe by group_cols and return rows matching SEGMENT_SCHEMA."""
    agg = (
        group_df
        .group_by(group_cols)
        .agg([
            pl.len().alias("total_transactions"),
            (pl.col("status") == "approved").sum().cast(pl.Int64).alias("approved_transactions"),
            pl.col("amount_usd").sum().alias("total_amount_usd"),
            pl.when(pl.col("status") == "approved")
              .then(pl.col("amount_usd"))
              .otherwise(0.0)
              .sum()
              .alias("approved_amount_usd"),
        ])
        .with_columns([
            pl.lit(segment_type).alias("segment_type"),
            _segment_key(group_cols).alias("segment_key"),
            _dim_expr(group_cols, 0),
            _dim_expr(group_cols, 1),
            _dim_expr(group_cols, 2),
            _dim_expr(group_cols, 3),
            pl.lit(period_value).alias("period"),
            (pl.col("total_transactions").cast(pl.Int64) - pl.col("approved_transactions")).alias("declined_transactions"),
            (pl.col("approved_transactions").cast(pl.Float64) / pl.col("total_transactions").cast(pl.Float64))
              .alias("approval_rate"),
        ])
        .cast({"total_transactions": pl.Int64, "declined_transactions": pl.Int64})
        .select(list(SEGMENT_SCHEMA.keys()))
    )
    return agg


def _segments_for_type(df: pl.DataFrame, segment_type: str, dims: list[str]) -> pl.DataFrame:
    """
    For one segment definition, produce:
      - per-week breakdown (period = "week_1", "week_2", ...)
      - half-period breakdown (period = "weeks_1_3" or "weeks_4_6")
      - overall (period = "all")
    """
    frames = []

    # Per-week
    for week in df["week_number"].unique().sort():
        week_df = df.filter(pl.col("week_number") == week)
        period_label = f"week_{week}"
        frames.append(_compute_agg(week_df, dims, period_label, segment_type))

    # Half-period
    for half in df["period_half"].unique().sort():
        half_df = df.filter(pl.col("period_half") == half)
        frames.append(_compute_agg(half_df, dims, half, segment_type))

    # Overall
    frames.append(_compute_agg(df, dims, "all", segment_type))

    return pl.concat(frames)


def build_segments(df: pl.DataFrame) -> pl.DataFrame:
    """
    Run all segment definitions and return a combined DataFrame
    matching SEGMENT_SCHEMA.
    """
    all_segments = []
    for segment_type, dims in SEGMENT_DEFINITIONS.items():
        seg = _segments_for_type(df, segment_type, dims)
        all_segments.append(seg)
    result = pl.concat(all_segments)
    # Cast to exact schema types
    for col, dtype in SEGMENT_SCHEMA.items():
        result = result.with_columns(pl.col(col).cast(dtype))
    return result
