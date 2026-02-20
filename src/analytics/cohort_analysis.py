"""
Cohort Analysis for VoltMarket Transaction Intelligence Engine.

Analyses first-time vs returning customer behaviour, recurring vs one-time
transaction patterns, and acquisition cohort retention across the 6-week window.

Outputs: data/analytics/cohort_analysis.json
"""

import json
import os

import polars as pl

from src.contracts.schemas import RAW_OUTPUT_PATH

COHORT_OUTPUT_PATH = "data/analytics/cohort_analysis.json"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_raw_transactions(path: str = RAW_OUTPUT_PATH) -> pl.DataFrame:
    """Load raw transactions and validate the file exists."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Raw transactions not found at '{path}'. "
            "Run the data generator first."
        )
    return pl.read_parquet(path)


# ---------------------------------------------------------------------------
# Customer classification
# ---------------------------------------------------------------------------

def tag_first_time_vs_returning(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rank each customer's transactions by timestamp and label the earliest
    one 'first_time'; all subsequent ones 'returning'.

    Returns the input DataFrame with an added 'customer_type' column.
    """
    ranked = df.with_columns(
        pl.col("timestamp")
        .rank(method="ordinal")
        .over("customer_id")
        .alias("_txn_rank")
    )
    return ranked.with_columns(
        pl.when(pl.col("_txn_rank") == 1)
        .then(pl.lit("first_time"))
        .otherwise(pl.lit("returning"))
        .alias("customer_type")
    ).drop("_txn_rank")


def derive_cohort_week(df: pl.DataFrame) -> pl.DataFrame:
    """
    Assign each customer a cohort_week: the week_number of their very first
    transaction (earliest timestamp).  The result is joined back onto df.
    """
    first_week = (
        df.sort("timestamp")
        .group_by("customer_id")
        .agg(pl.col("week_number").first().alias("cohort_week"))
    )
    return df.join(first_week, on="customer_id", how="left")


# ---------------------------------------------------------------------------
# Metric builders
# ---------------------------------------------------------------------------

def _approval_rate(approved: pl.Expr, total: pl.Expr) -> pl.Expr:
    """Safe approval rate expression: 0.0 when total is zero."""
    return pl.when(total > 0).then(approved / total).otherwise(0.0)


def build_first_time_vs_returning(df: pl.DataFrame) -> list[dict]:
    """
    Per week: approval rate for first-time customers vs returning customers.
    Returns a list of dicts sorted by week_number.
    """
    is_approved = (pl.col("status") == "approved").cast(pl.Int64)
    agg = (
        df.group_by(["week_number", "customer_type"])
        .agg(
            is_approved.sum().alias("approved"),
            pl.len().alias("total"),
        )
        .sort(["week_number", "customer_type"])
    )

    pivoted = agg.pivot(
        on="customer_type",
        index="week_number",
        values=["approved", "total"],
        aggregate_function="sum",
    )

    records = []
    for row in pivoted.sort("week_number").iter_rows(named=True):
        week = row["week_number"]
        ft_app = row.get("approved_first_time", 0) or 0
        ft_tot = row.get("total_first_time", 0) or 0
        re_app = row.get("approved_returning", 0) or 0
        re_tot = row.get("total_returning", 0) or 0
        records.append({
            "week": week,
            "first_time_rate": round(ft_app / ft_tot, 4) if ft_tot else 0.0,
            "first_time_n": ft_tot,
            "returning_rate": round(re_app / re_tot, 4) if re_tot else 0.0,
            "returning_n": re_tot,
        })
    return records


def build_recurring_vs_onetime(df: pl.DataFrame) -> list[dict]:
    """
    Per week: approval rate for recurring transactions vs one-time transactions.
    Returns a list of dicts sorted by week_number.
    """
    is_approved = (pl.col("status") == "approved").cast(pl.Int64)
    txn_kind = pl.when(pl.col("is_recurring")).then(pl.lit("recurring")).otherwise(pl.lit("onetime"))

    agg = (
        df.with_columns(txn_kind.alias("txn_kind"))
        .group_by(["week_number", "txn_kind"])
        .agg(
            is_approved.sum().alias("approved"),
            pl.len().alias("total"),
        )
        .sort(["week_number", "txn_kind"])
    )

    pivoted = agg.pivot(
        on="txn_kind",
        index="week_number",
        values=["approved", "total"],
        aggregate_function="sum",
    )

    records = []
    for row in pivoted.sort("week_number").iter_rows(named=True):
        week = row["week_number"]
        rec_app = row.get("approved_recurring", 0) or 0
        rec_tot = row.get("total_recurring", 0) or 0
        one_app = row.get("approved_onetime", 0) or 0
        one_tot = row.get("total_onetime", 0) or 0
        records.append({
            "week": week,
            "recurring_rate": round(rec_app / rec_tot, 4) if rec_tot else 0.0,
            "recurring_n": rec_tot,
            "onetime_rate": round(one_app / one_tot, 4) if one_tot else 0.0,
            "onetime_n": one_tot,
        })
    return records


def build_acquisition_cohorts(df: pl.DataFrame) -> list[dict]:
    """
    Group customers by their acquisition week (cohort_week).  For each
    (cohort_week, transaction_week) pair track how those customers performed.

    Returns a list of dicts sorted by (cohort_week, transaction_week).
    """
    is_approved = (pl.col("status") == "approved").cast(pl.Int64)

    agg = (
        df.group_by(["cohort_week", "week_number"])
        .agg(
            is_approved.sum().alias("approved"),
            pl.len().alias("total"),
            pl.col("customer_id").n_unique().alias("n_customers"),
        )
        .sort(["cohort_week", "week_number"])
    )

    records = []
    for row in agg.iter_rows(named=True):
        total = row["total"]
        records.append({
            "cohort_week": row["cohort_week"],
            "transaction_week": row["week_number"],
            "approval_rate": round(row["approved"] / total, 4) if total else 0.0,
            "n_customers": row["n_customers"],
            "n_transactions": total,
        })
    records.sort(key=lambda r: (r["cohort_week"], r["transaction_week"]))
    return records


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def save_cohort_output(payload: dict, path: str = COHORT_OUTPUT_PATH) -> None:
    """Serialise the cohort analysis results to JSON."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(payload, fh, indent=2)
    print(f"[cohort_analysis] Saved results to '{path}'")


# ---------------------------------------------------------------------------
# Console summary
# ---------------------------------------------------------------------------

def print_summary(
    first_vs_ret: list[dict],
    rec_vs_one: list[dict],
    cohorts: list[dict],
) -> None:
    """Print a concise summary of cohort findings to stdout."""
    print("\n=== COHORT ANALYSIS SUMMARY ===\n")

    print("First-time vs Returning (approval rates by week):")
    print(f"  {'Week':<6} {'First-time':>12} {'N':>6}  {'Returning':>12} {'N':>6}")
    for r in first_vs_ret:
        print(
            f"  {r['week']:<6} {r['first_time_rate']:>11.1%} {r['first_time_n']:>6}"
            f"  {r['returning_rate']:>11.1%} {r['returning_n']:>6}"
        )

    print("\nRecurring vs One-time (approval rates by week):")
    print(f"  {'Week':<6} {'Recurring':>12} {'N':>6}  {'One-time':>12} {'N':>6}")
    for r in rec_vs_one:
        print(
            f"  {r['week']:<6} {r['recurring_rate']:>11.1%} {r['recurring_n']:>6}"
            f"  {r['onetime_rate']:>11.1%} {r['onetime_n']:>6}"
        )

    print("\nAcquisition Cohorts (cohort_week -> approval rate in transaction_week):")
    cohort_weeks = sorted({r["cohort_week"] for r in cohorts})
    txn_weeks = sorted({r["transaction_week"] for r in cohorts})
    header = f"  {'Cohort':<8}" + "".join(f"  W{w:>2}" for w in txn_weeks)
    print(header)
    lookup = {(r["cohort_week"], r["transaction_week"]): r["approval_rate"] for r in cohorts}
    for cw in cohort_weeks:
        row_str = f"  W{cw:<7}"
        for tw in txn_weeks:
            rate = lookup.get((cw, tw))
            row_str += f"  {rate:.0%}" if rate is not None else "     -"
        print(row_str)
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run() -> dict:
    """
    Full cohort analysis pipeline.

    Loads raw transactions, computes all three cohort views, saves JSON output,
    and prints a console summary.  Returns the output payload dict.
    """
    df = load_raw_transactions()
    df = tag_first_time_vs_returning(df)
    df = derive_cohort_week(df)

    first_vs_ret = build_first_time_vs_returning(df)
    rec_vs_one = build_recurring_vs_onetime(df)
    cohorts = build_acquisition_cohorts(df)

    payload = {
        "first_time_vs_returning": first_vs_ret,
        "recurring_vs_onetime": rec_vs_one,
        "acquisition_cohorts": cohorts,
    }

    save_cohort_output(payload)
    print_summary(first_vs_ret, rec_vs_one, cohorts)

    return payload


if __name__ == "__main__":
    run()
