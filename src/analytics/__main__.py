"""
CLI entrypoint for Session C Analytics.

Usage:
    python -m src.analytics
"""

from rich.console import Console
from rich.table import Table
from rich import box

from src.analytics.anomaly_detection import run as run_anomaly_detection
from src.analytics.insights import run as run_insights

console = Console()


def main() -> None:
    console.rule("[bold blue]VoltMarket Analytics — Anomaly Detection & Insight Generation")

    # Step 1: Anomaly detection
    console.print("\n[bold cyan]Step 1:[/] Detecting anomalies across segments...")
    anomalies = run_anomaly_detection()

    # Step 2: Insight generation
    console.print("\n[bold cyan]Step 2:[/] Ranking insights by revenue impact...")
    insights = run_insights(anomalies=anomalies)

    # Step 3: Print top insights
    console.rule("[bold green]Top Insights")

    table = Table(
        box=box.ROUNDED,
        show_header=True,
        header_style="bold magenta",
        expand=True,
    )
    table.add_column("#", style="bold", width=3, justify="right")
    table.add_column("Severity", width=10)
    table.add_column("Title", style="bold")
    table.add_column("Rate Change", justify="right", width=12)
    table.add_column("Impact (Monthly)", justify="right", width=18)
    table.add_column("Segment", width=32)

    severity_colors = {
        "critical": "red",
        "high": "orange3",
        "medium": "yellow",
        "low": "green",
    }

    for row in insights.iter_rows(named=True):
        sev = row["severity"]
        color = severity_colors.get(sev, "white")
        rate_change_pct = row["rate_change"] * 100
        table.add_row(
            str(row["rank"]),
            f"[{color}]{sev.upper()}[/{color}]",
            row["title"],
            f"[{'red' if rate_change_pct < 0 else 'green'}]{rate_change_pct:+.1f}pp[/]",
            f"${row['estimated_revenue_impact_usd']:>12,.0f}",
            row["segment_key"],
        )

    console.print(table)

    # Print descriptions
    console.rule("[bold green]Insight Summaries")
    for row in insights.iter_rows(named=True):
        sev = row["severity"]
        color = severity_colors.get(sev, "white")
        console.print(f"\n[bold][{color}]#{row['rank']} — {row['title']}[/{color}][/bold]")
        console.print(f"  {row['description']}")

    console.rule()
    console.print(f"[dim]Outputs: data/analytics/insights.json, data/analytics/anomalies.parquet, data/analytics/summary.json[/dim]")


if __name__ == "__main__":
    main()
