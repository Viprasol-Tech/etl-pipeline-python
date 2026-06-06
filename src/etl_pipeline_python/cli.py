"""Command-line interface for ETL Pipeline Python.

``etl-pipeline-python demo`` runs a small extract -> transform -> load pipeline
over an embedded CSV string — fully offline, no files or network.

Part of ETL Pipeline Python by Viprasol Tech Private Limited (https://viprasol.com).
"""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from etl_pipeline_python import __version__
from etl_pipeline_python.extractors import CSVStringExtractor
from etl_pipeline_python.loaders import ListLoader
from etl_pipeline_python.pipeline import Pipeline
from etl_pipeline_python.transforms import cast, derive, filter_by, rename, select

app = typer.Typer(add_completion=False, help="ETL Pipeline Python — by Viprasol Tech.")
console = Console()

_SAMPLE_CSV = """id,first,last,age,country
1,Ada,Lovelace,36,UK
2,Alan,Turing,41,UK
3,Grace,Hopper,85,US
4,Tim,Junior,12,US
"""


@app.command()
def version() -> None:
    """Print the installed version."""
    console.print(f"etl-pipeline-python [bold cyan]{__version__}[/] - by Viprasol Tech")


@app.command()
def demo() -> None:
    """Run an offline extract -> transform -> load pipeline on sample data."""
    extractor = CSVStringExtractor(_SAMPLE_CSV)
    loader = ListLoader()

    pipeline = (
        Pipeline(extractor=extractor, loader=loader)
        .transform(cast({"age": int}))
        .transform(filter_by(lambda r: r["age"] >= 18))
        .transform(derive("name", lambda r: f"{r['first']} {r['last']}"))
        .transform(rename({"country": "region"}))
        .transform(select("id", "name", "age", "region"))
    )

    result = pipeline.run()

    table = Table(title="Loaded records")
    for column in ("id", "name", "age", "region"):
        table.add_column(column, style="cyan")
    for row in loader.rows:
        table.add_row(str(row["id"]), str(row["name"]), str(row["age"]), str(row["region"]))
    console.print(table)

    console.print(
        f"Extracted: [bold]{result.extracted}[/]  "
        f"Dropped: [bold yellow]{result.dropped}[/]  "
        f"Loaded: [bold green]{result.loaded}[/]"
    )


if __name__ == "__main__":
    app()
