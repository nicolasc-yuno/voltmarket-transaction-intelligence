"""
Ingest raw transaction parquet, validate schema, add derived columns.
"""

import random
import string
from pathlib import Path

import polars as pl

from src.contracts.schemas import (
    AMOUNT_BUCKETS,
    CARD_BRANDS,
    CARD_TYPES,
    COUNTRIES,
    DECLINE_REASONS,
    HOUR_BUCKETS,
    ISSUER_BANKS,
    RAW_OUTPUT_PATH,
    RAW_TRANSACTION_SCHEMA,
)


def _generate_mock_data(n: int = 200) -> pl.DataFrame:
    """Generate mock transactions matching RAW_TRANSACTION_SCHEMA for dev use."""
    import datetime

    random.seed(42)
    rows = []
    countries = list(COUNTRIES.keys())
    brands = list(CARD_BRANDS.keys())
    types = list(CARD_TYPES.keys())

    for i in range(n):
        country = random.choices(countries, weights=[0.50, 0.30, 0.20])[0]
        week_number = random.randint(1, 6)
        # Simulate approval rate story: weeks 1-3 ~82%, weeks 4-6 ~64%
        base_rate = 0.82 if week_number <= 3 else 0.64
        approved = random.random() < base_rate
        status = "approved" if approved else "declined"
        amount_local = round(random.uniform(10, 500), 2)
        fx = COUNTRIES[country]["fx_rate"]
        amount_usd = round(amount_local * fx, 4)
        hour = random.randint(0, 23)
        ts = datetime.datetime(2024, 1, 1) + datetime.timedelta(
            weeks=week_number - 1, hours=hour, minutes=random.randint(0, 59)
        )
        banks = ISSUER_BANKS[country]
        rows.append({
            "transaction_id": "tx_" + "".join(random.choices(string.ascii_lowercase, k=8)),
            "timestamp": ts,
            "week_number": week_number,
            "country": country,
            "currency": COUNTRIES[country]["currency"],
            "amount": amount_local,
            "amount_usd": amount_usd,
            "card_brand": random.choices(brands, weights=[0.50, 0.35, 0.15])[0],
            "card_type": random.choices(types, weights=[0.60, 0.35, 0.05])[0],
            "issuer_bank": random.choice(banks),
            "status": status,
            "decline_reason": random.choice(DECLINE_REASONS) if not approved else None,
            "merchant_id": "m_" + str(random.randint(1, 20)),
            "customer_id": "c_" + str(random.randint(1, 100)),
            "is_recurring": random.random() < 0.15,
            "hour_of_day": hour,
        })

    schema = {k: v for k, v in RAW_TRANSACTION_SCHEMA.items()}
    return pl.DataFrame(rows, schema=schema)


def _validate_schema(df: pl.DataFrame) -> None:
    """Raise if df is missing required columns or has wrong types."""
    for col, dtype in RAW_TRANSACTION_SCHEMA.items():
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")
        actual = df[col].dtype
        # For Datetime, compare only time_unit — ignore optional timezone
        if isinstance(dtype, pl.Datetime):
            if not isinstance(actual, pl.Datetime) or actual.time_unit != dtype.time_unit:
                raise TypeError(f"Column '{col}': expected {dtype}, got {actual}")
        elif actual != dtype:
            raise TypeError(f"Column '{col}': expected {dtype}, got {actual}")


def _add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Add amount_bucket, hour_bucket, period_half using native Polars expressions."""
    # AMOUNT_BUCKETS = ["sub_$10", "$10-50", "$50-100", "$100-200", "$200-350", "$350-500"]
    amount_bucket = (
        pl.when(pl.col("amount_usd") < 10).then(pl.lit(AMOUNT_BUCKETS[0]))
          .when(pl.col("amount_usd") < 50).then(pl.lit(AMOUNT_BUCKETS[1]))
          .when(pl.col("amount_usd") < 100).then(pl.lit(AMOUNT_BUCKETS[2]))
          .when(pl.col("amount_usd") < 200).then(pl.lit(AMOUNT_BUCKETS[3]))
          .when(pl.col("amount_usd") < 350).then(pl.lit(AMOUNT_BUCKETS[4]))
          .otherwise(pl.lit(AMOUNT_BUCKETS[5]))
          .alias("amount_bucket")
    )
    # HOUR_BUCKETS = ["morning_6_12", "afternoon_12_17", "evening_17_20", "night_20_24", "late_night_0_6"]
    hour_bucket = (
        pl.when((pl.col("hour_of_day") >= 6) & (pl.col("hour_of_day") < 12)).then(pl.lit(HOUR_BUCKETS[0]))
          .when((pl.col("hour_of_day") >= 12) & (pl.col("hour_of_day") < 17)).then(pl.lit(HOUR_BUCKETS[1]))
          .when((pl.col("hour_of_day") >= 17) & (pl.col("hour_of_day") < 20)).then(pl.lit(HOUR_BUCKETS[2]))
          .when((pl.col("hour_of_day") >= 20) & (pl.col("hour_of_day") < 24)).then(pl.lit(HOUR_BUCKETS[3]))
          .otherwise(pl.lit(HOUR_BUCKETS[4]))
          .alias("hour_bucket")
    )
    period_half = (
        pl.when(pl.col("week_number") <= 3)
          .then(pl.lit("weeks_1_3"))
          .otherwise(pl.lit("weeks_4_6"))
          .alias("period_half")
    )
    return df.with_columns([amount_bucket, hour_bucket, period_half])


def load_transactions() -> pl.DataFrame:
    """
    Load and validate raw transactions. Falls back to mock data if file is absent.
    Returns enriched DataFrame with derived columns.
    """
    raw_path = Path(RAW_OUTPUT_PATH)
    if raw_path.exists():
        df = pl.read_parquet(raw_path)
        _validate_schema(df)
    else:
        print(f"[ingest] {RAW_OUTPUT_PATH} not found — generating mock data (200 rows)")
        df = _generate_mock_data(200)
        _validate_schema(df)  # validate mock data too
    return _add_derived_columns(df)
