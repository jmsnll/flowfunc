from pathlib import Path

import click

from flowfunc.console import console
from flowfunc.workflow import loader
from flowfunc.workflow import pipeline


@click.command("docs", help="Displays the documentation for a workflow.")
@click.argument(
    "workflow_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "-v", "--verbose", is_flag=True, help="Print extra info about the workflow file."
)
def docs(workflow_path: Path, verbose: bool) -> None:
    try:
        workflow_model = loader.from_path(workflow_path.absolute())
        workflow_pipeline = pipeline.from_model(workflow_model)
        workflow_pipeline.print_documentation()
    except Exception as e:
        console.print(f"[red]Failed to generate documentation:[/] {e}")
        raise click.Abort()
