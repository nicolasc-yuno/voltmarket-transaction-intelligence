"""
CLI entrypoint: python -m src.pipeline
Runs the full ingestion + segmentation + trend pipeline.
"""

from pathlib import Path

from src.contracts.schemas import SEGMENT_OUTPUT_PATH, WEEKLY_TREND_OUTPUT_PATH
from src.pipeline.ingest import load_transactions
from src.pipeline.segment import build_segments
from src.pipeline.transform import build_weekly_trends


def main() -> None:
    print("[pipeline] Starting VoltMarket Transaction Intelligence Pipeline")

    # Step 1: Ingest
    print("[pipeline] Step 1/3 — Ingesting transactions...")
    df = load_transactions()
    total = len(df)
    approved = (df["status"] == "approved").sum()
    overall_rate = approved / total if total > 0 else 0.0
    print(f"  Loaded {total:,} transactions | Overall approval rate: {overall_rate:.1%}")

    # Step 2: Segments
    print("[pipeline] Step 2/3 — Building segments...")
    segments = build_segments(df)
    n_segments = len(segments)
    print(f"  Generated {n_segments:,} segment rows")

    # Step 3: Weekly trends
    print("[pipeline] Step 3/3 — Computing weekly trends...")
    trends = build_weekly_trends(df)
    print(f"  {len(trends)} weekly trend rows")

    # Write outputs
    Path(SEGMENT_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    Path(WEEKLY_TREND_OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
    segments.write_parquet(SEGMENT_OUTPUT_PATH)
    trends.write_parquet(WEEKLY_TREND_OUTPUT_PATH)

    print(f"\n[pipeline] Done.")
    print(f"  segments  -> {SEGMENT_OUTPUT_PATH}")
    print(f"  trends    -> {WEEKLY_TREND_OUTPUT_PATH}")
    print(f"\nSummary:")
    print(f"  Transactions : {total:,}")
    print(f"  Approval rate: {overall_rate:.1%}")
    print(f"  Segment rows : {n_segments:,}")

    # Print per-week approval rates for a quick sanity check
    print("\nWeekly breakdown:")
    for row in trends.iter_rows(named=True):
        print(f"  Week {row['week_number']}: {row['approved_transactions']:,}/{row['total_transactions']:,}"
              f" ({row['approval_rate']:.1%})")


if __name__ == "__main__":
    main()
