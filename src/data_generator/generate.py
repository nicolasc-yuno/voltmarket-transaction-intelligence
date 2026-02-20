"""
Session A: Synthetic Transaction Data Generator

Generates ~8,000 transactions across 6 weeks with three embedded degradation patterns:
  1. BBVA Mexico collapse (primary driver: 85% -> 45%)
  2. High-value transaction degradation (>$200 USD: 80% -> 60%)
  3. Evening underperformance (post-8PM: 78% -> 58%)

Overall approval rate trends from 82% (weeks 1-3) -> 64% (weeks 4-6).

Usage:
    python -m src.data_generator.generate
"""

import uuid
from datetime import datetime, timedelta

import numpy as np
import polars as pl

from src.contracts.schemas import (
    AMOUNT_BUCKETS,
    CARD_BRANDS,
    CARD_TYPES,
    COUNTRIES,
    DECLINE_REASONS,
    ISSUER_BANKS,
    RAW_OUTPUT_PATH,
    RAW_TRANSACTION_SCHEMA,
)

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
RNG = np.random.default_rng(seed=42)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TOTAL_TRANSACTIONS = 8_000
START_DATE = datetime(2026, 1, 5)  # Monday, week 1 start
WEEKS = 6
DAYS_PER_WEEK = 7

# Transactions per week — roughly equal
TXN_PER_WEEK = TOTAL_TRANSACTIONS // WEEKS  # 1333 each; last week gets remainder

# ---------------------------------------------------------------------------
# Approval rate helpers
# ---------------------------------------------------------------------------


def _is_early(week: int) -> bool:
    return week <= 3


def _decide_approved(p: float) -> bool:
    return bool(RNG.random() < p)


def _pick_decline_reason(weights: dict[str, float]) -> str:
    """Pick a decline reason from weighted dict."""
    reasons = list(weights.keys())
    probs = np.array(list(weights.values()), dtype=float)
    probs /= probs.sum()
    return str(RNG.choice(reasons, p=probs))


# Weighted decline reasons per pattern
BASELINE_DECLINE_WEIGHTS = {
    "insufficient_funds": 0.35,
    "do_not_honor": 0.25,
    "expired_card": 0.10,
    "invalid_transaction": 0.10,
    "restricted_card": 0.08,
    "lost_card": 0.05,
    "pickup_card": 0.04,
    "fraud_suspected": 0.03,
}

BBVA_DECLINE_WEIGHTS = {
    "do_not_honor": 0.40,
    "fraud_suspected": 0.35,
    "insufficient_funds": 0.10,
    "restricted_card": 0.07,
    "expired_card": 0.05,
    "invalid_transaction": 0.02,
    "lost_card": 0.01,
    "pickup_card": 0.00,
}

HIGH_VALUE_DECLINE_WEIGHTS = {
    "restricted_card": 0.40,
    "do_not_honor": 0.35,
    "insufficient_funds": 0.10,
    "fraud_suspected": 0.08,
    "expired_card": 0.05,
    "invalid_transaction": 0.01,
    "lost_card": 0.01,
    "pickup_card": 0.00,
}

EVENING_DECLINE_WEIGHTS = {
    "fraud_suspected": 0.50,
    "do_not_honor": 0.25,
    "insufficient_funds": 0.10,
    "restricted_card": 0.08,
    "expired_card": 0.05,
    "invalid_transaction": 0.01,
    "lost_card": 0.01,
    "pickup_card": 0.00,
}


# ---------------------------------------------------------------------------
# Approval rate table
# ---------------------------------------------------------------------------

def approval_rate(
    week: int,
    country: str,
    issuer_bank: str,
    amount_usd: float,
    hour: int,
) -> float:
    """
    Return the approval probability for a transaction given its attributes.
    Patterns compound when multiple apply.
    """
    early = _is_early(week)

    is_bbva_mx = (country == "MX" and issuer_bank == "BBVA")
    is_high_value = amount_usd > 200
    is_evening = hour >= 20

    # Start with baseline
    rate = 0.85 if early else 0.75

    # Apply pattern penalties (multiplicative, in order of priority)
    if is_bbva_mx:
        # Pattern 1: BBVA Mexico collapse — primary driver
        rate = 0.85 if early else 0.45
        # High-value worsens it further in late weeks
        if not early and is_high_value:
            rate = 0.38
    else:
        # Pattern 2: high-value degradation
        if is_high_value:
            base_hv = 0.80 if early else 0.60
            # MX is more pronounced
            if not early and country == "MX":
                base_hv = 0.55
            rate = min(rate, base_hv)

        # Pattern 3: evening underperformance (applied on top)
        if is_evening:
            evening_rate = 0.78 if early else 0.58
            rate = min(rate, evening_rate)

    return rate


def pick_decline_reason(
    country: str,
    issuer_bank: str,
    amount_usd: float,
    hour: int,
    week: int,
) -> str:
    is_bbva_mx = (country == "MX" and issuer_bank == "BBVA")
    is_high_value = amount_usd > 200
    is_evening = hour >= 20
    early = _is_early(week)

    # Priority order for which pattern's decline weights to use
    if is_bbva_mx and not early:
        return _pick_decline_reason(BBVA_DECLINE_WEIGHTS)
    if is_high_value:
        return _pick_decline_reason(HIGH_VALUE_DECLINE_WEIGHTS)
    if is_evening:
        return _pick_decline_reason(EVENING_DECLINE_WEIGHTS)
    return _pick_decline_reason(BASELINE_DECLINE_WEIGHTS)


# ---------------------------------------------------------------------------
# Generators for individual fields
# ---------------------------------------------------------------------------

def _random_country() -> str:
    countries = list(COUNTRIES.keys())
    weights = np.array([COUNTRIES[c]["weight"] for c in countries])
    return str(RNG.choice(countries, p=weights))


def _random_card_brand() -> str:
    brands = list(CARD_BRANDS.keys())
    weights = np.array(list(CARD_BRANDS.values()))
    return str(RNG.choice(brands, p=weights))


def _random_card_type() -> str:
    types = list(CARD_TYPES.keys())
    weights = np.array(list(CARD_TYPES.values()))
    return str(RNG.choice(types, p=weights))


def _random_issuer(country: str) -> str:
    banks = ISSUER_BANKS[country]
    return str(RNG.choice(banks))


def _random_amount(country: str) -> tuple[float, float]:
    """Return (local_amount, amount_usd)."""
    # Target USD range 10-500, then convert to local
    amount_usd = float(RNG.uniform(10, 500))
    fx = COUNTRIES[country]["fx_rate"]
    # local = usd / fx
    local_amount = round(amount_usd / fx, 2)
    return round(local_amount, 2), round(amount_usd, 2)


def _random_timestamp(week: int) -> datetime:
    """Return a random timestamp within the given week (1-indexed)."""
    week_start = START_DATE + timedelta(weeks=week - 1)
    day_offset = int(RNG.integers(0, DAYS_PER_WEEK))
    # Simulate realistic hourly traffic: peak 10-22h
    # Use a mixture: 30% late night (0-6), 10% early morning (6-10), 60% daytime (10-22)
    r = RNG.random()
    if r < 0.10:
        hour = int(RNG.integers(0, 6))
    elif r < 0.20:
        hour = int(RNG.integers(6, 10))
    else:
        hour = int(RNG.integers(10, 24))
    minute = int(RNG.integers(0, 60))
    second = int(RNG.integers(0, 60))
    return week_start + timedelta(days=day_offset, hours=hour, minutes=minute, seconds=second)


def _merchant_id() -> str:
    # 200 distinct merchants
    return f"MERCH_{RNG.integers(1, 201):04d}"


def _customer_id() -> str:
    # 2000 distinct customers
    return f"CUST_{RNG.integers(1, 2001):06d}"


# ---------------------------------------------------------------------------
# Main generation loop
# ---------------------------------------------------------------------------

def generate_transactions() -> pl.DataFrame:
    records = []

    # Distribute rows across 6 weeks
    weekly_counts = [TXN_PER_WEEK] * WEEKS
    remainder = TOTAL_TRANSACTIONS - sum(weekly_counts)
    weekly_counts[-1] += remainder

    for week in range(1, WEEKS + 1):
        n = weekly_counts[week - 1]
        for _ in range(n):
            country = _random_country()
            card_brand = _random_card_brand()
            card_type = _random_card_type()
            issuer_bank = _random_issuer(country)
            local_amount, amount_usd = _random_amount(country)
            ts = _random_timestamp(week)
            hour = ts.hour

            rate = approval_rate(week, country, issuer_bank, amount_usd, hour)
            approved = _decide_approved(rate)

            status = "approved" if approved else "declined"
            decline_reason = (
                ""
                if approved
                else pick_decline_reason(country, issuer_bank, amount_usd, hour, week)
            )

            records.append({
                "transaction_id": str(uuid.uuid4()),
                "timestamp": ts,
                "week_number": week,
                "country": country,
                "currency": COUNTRIES[country]["currency"],
                "amount": local_amount,
                "amount_usd": amount_usd,
                "card_brand": card_brand,
                "card_type": card_type,
                "issuer_bank": issuer_bank,
                "status": status,
                "decline_reason": decline_reason,
                "merchant_id": _merchant_id(),
                "customer_id": _customer_id(),
                "is_recurring": bool(RNG.random() < 0.15),
                "hour_of_day": hour,
            })

    df = pl.DataFrame(records).cast(RAW_TRANSACTION_SCHEMA)  # type: ignore[arg-type]
    return df


# ---------------------------------------------------------------------------
# Validation / summary statistics
# ---------------------------------------------------------------------------

def print_summary(df: pl.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("DATA GENERATOR SUMMARY")
    print("=" * 60)
    print(f"Total transactions: {len(df):,}")

    # Weekly totals and approval rates
    print("\n--- Weekly approval rates (overall) ---")
    weekly = (
        df.group_by("week_number")
        .agg([
            pl.len().alias("total"),
            (pl.col("status") == "approved").sum().alias("approved"),
        ])
        .sort("week_number")
        .with_columns(
            (pl.col("approved") / pl.col("total")).alias("approval_rate")
        )
    )
    for row in weekly.iter_rows(named=True):
        print(
            f"  Week {row['week_number']}: {row['total']:,} txns | "
            f"approval rate: {row['approval_rate']:.1%}"
        )

    # BBVA Mexico per week
    print("\n--- BBVA Mexico approval rate per week ---")
    bbva = (
        df.filter((pl.col("country") == "MX") & (pl.col("issuer_bank") == "BBVA"))
        .group_by("week_number")
        .agg([
            pl.len().alias("total"),
            (pl.col("status") == "approved").sum().alias("approved"),
        ])
        .sort("week_number")
        .with_columns(
            (pl.col("approved") / pl.col("total")).alias("approval_rate")
        )
    )
    for row in bbva.iter_rows(named=True):
        print(
            f"  Week {row['week_number']}: {row['total']:,} txns | "
            f"approval rate: {row['approval_rate']:.1%}"
        )

    # High-value (>$200 USD) per week
    print("\n--- High-value (>$200 USD) approval rate per week ---")
    hv = (
        df.filter(pl.col("amount_usd") > 200)
        .group_by("week_number")
        .agg([
            pl.len().alias("total"),
            (pl.col("status") == "approved").sum().alias("approved"),
        ])
        .sort("week_number")
        .with_columns(
            (pl.col("approved") / pl.col("total")).alias("approval_rate")
        )
    )
    for row in hv.iter_rows(named=True):
        print(
            f"  Week {row['week_number']}: {row['total']:,} txns | "
            f"approval rate: {row['approval_rate']:.1%}"
        )

    # Post-8PM per week
    print("\n--- Post-8PM (hour >= 20) approval rate per week ---")
    eve = (
        df.filter(pl.col("hour_of_day") >= 20)
        .group_by("week_number")
        .agg([
            pl.len().alias("total"),
            (pl.col("status") == "approved").sum().alias("approved"),
        ])
        .sort("week_number")
        .with_columns(
            (pl.col("approved") / pl.col("total")).alias("approval_rate")
        )
    )
    for row in eve.iter_rows(named=True):
        print(
            f"  Week {row['week_number']}: {row['total']:,} txns | "
            f"approval rate: {row['approval_rate']:.1%}"
        )

    # Half-period summary
    early = df.filter(pl.col("week_number") <= 3)
    late = df.filter(pl.col("week_number") > 3)
    early_rate = (early["status"] == "approved").mean()
    late_rate = (late["status"] == "approved").mean()
    print(f"\n--- Half-period summary ---")
    print(f"  Weeks 1-3 overall: {early_rate:.1%}")
    print(f"  Weeks 4-6 overall: {late_rate:.1%}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    import os

    print("Generating synthetic transactions...")
    df = generate_transactions()

    # Validate schema columns
    expected_cols = set(RAW_TRANSACTION_SCHEMA.keys())
    actual_cols = set(df.columns)
    assert actual_cols == expected_cols, f"Schema mismatch: {actual_cols ^ expected_cols}"

    print_summary(df)

    # Save parquet
    os.makedirs("data/raw", exist_ok=True)
    df.write_parquet(RAW_OUTPUT_PATH)
    print(f"\nSaved {len(df):,} rows -> {RAW_OUTPUT_PATH}")

    # Save CSV sample
    sample_path = "data/raw/sample.csv"
    df.head(100).write_csv(sample_path)
    print(f"Saved 100-row sample -> {sample_path}")


if __name__ == "__main__":
    main()
