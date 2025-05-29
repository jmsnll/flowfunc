from pathlib import Path

import click

from flowfunc.console import console
from flowfunc.workflow import loader
from flowfunc.workflow import pipeline


@click.command(name="graph", help="Graphs a workflow using matplotlib.")
@click.argument(
    "workflow_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
def graph(workflow_path: str) -> None:
    console.status("[bold green]Loading workflow...", spinner="dots")
    workflow_model = loader.from_path(workflow_path.absolute())
    workflow_pipeline = pipeline.from_model(workflow_model)
    workflow_pipeline.visualize_matplotlib()
