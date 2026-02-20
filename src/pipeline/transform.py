"""
Weekly trend rollups for headline trend chart.
"""

import polars as pl

from src.contracts.schemas import WEEKLY_TREND_SCHEMA


def build_weekly_trends(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aggregate enriched transaction DataFrame to weekly level,
    producing a DataFrame matching WEEKLY_TREND_SCHEMA.
    """
    trends = (
        df
        .group_by("week_number")
        .agg([
            pl.len().alias("total_transactions"),
            (pl.col("status") == "approved").sum().alias("approved_transactions"),
            pl.col("amount_usd").sum().alias("total_amount_usd"),
        ])
        .with_columns([
            (pl.col("approved_transactions").cast(pl.Float64) / pl.col("total_transactions").cast(pl.Float64))
              .alias("approval_rate"),
        ])
        .sort("week_number")
        .cast({
            "week_number": pl.Int32,
            "total_transactions": pl.Int64,
            "approved_transactions": pl.Int64,
            "approval_rate": pl.Float64,
            "total_amount_usd": pl.Float64,
        })
        .select(list(WEEKLY_TREND_SCHEMA.keys()))
    )
    return trends
