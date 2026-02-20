"""
VoltMarket Transaction Intelligence Engine — CLI Entrypoint.

Usage:
    python -m src.main generate    # Generate synthetic transaction data
    python -m src.main pipeline    # Run segmentation pipeline
    python -m src.main analyze     # Run anomaly detection + insights
    python -m src.main visualize   # Generate dashboard + static charts
    python -m src.main run-all     # Full end-to-end pipeline
"""

import click
from rich.console import Console

console = Console()


@click.group()
def cli():
    """VoltMarket Transaction Intelligence Engine."""
    pass


@cli.command()
def generate():
    """Generate synthetic transaction data."""
    console.rule("[bold]Step 1: Data Generation[/bold]")
    from src.data_generator.generate import main
    main()
    console.print("[green]Data generation complete.[/green]\n")


@cli.command()
def pipeline():
    """Run segmentation pipeline."""
    console.rule("[bold]Step 2: Pipeline[/bold]")
    from src.pipeline.__main__ import main as pipeline_main
    pipeline_main()
    console.print("[green]Pipeline complete.[/green]\n")


@cli.command()
def analyze():
    """Run anomaly detection and insight generation."""
    console.rule("[bold]Step 3: Analytics[/bold]")
    from src.analytics.anomaly_detection import run as run_anomalies
    from src.analytics.insights import run as run_insights

    anomalies = run_anomalies()
    run_insights(anomalies)
    console.print("[green]Analytics complete.[/green]\n")


@cli.command()
def visualize():
    """Generate static charts and HTML dashboard."""
    console.rule("[bold]Step 4: Visualization[/bold]")
    from src.visualization.charts import export_all_png, export_standalone_html
    export_all_png()
    export_standalone_html()
    console.print("[green]Visualization complete.[/green]")
    console.print("Launch interactive dashboard: [bold]streamlit run src/visualization/dashboard.py[/bold]\n")


@cli.command(name="run-all")
def run_all():
    """Run the full pipeline end-to-end."""
    console.rule("[bold cyan]VoltMarket Transaction Intelligence Engine[/bold cyan]")
    console.print("Running full end-to-end pipeline...\n")

    generate.callback()
    pipeline.callback()
    analyze.callback()
    visualize.callback()

    console.rule("[bold green]Pipeline Complete[/bold green]")
    console.print("\nOutputs:")
    console.print("  Data:      data/raw/transactions.parquet")
    console.print("  Segments:  data/processed/segments.parquet")
    console.print("  Insights:  data/analytics/insights.json")
    console.print("  Charts:    output/*.png")
    console.print("  Dashboard: streamlit run src/visualization/dashboard.py")


if __name__ == "__main__":
    cli()
