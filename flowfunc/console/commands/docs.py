from pathlib import Path

import click

from flowfunc import workflow
from flowfunc.console import console
from flowfunc.workflow.context import RunContext


@click.command("docs", help="Displays the documentation for a workflow.")
@click.argument(
    "workflow_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "-v", "--verbose", is_flag=True, help="Print extra info about the workflow file."
)
def docs(workflow_path: Path, verbose: bool) -> None:
    ctx = RunContext()
    try:
        workflow.load(workflow_path, ctx.workflow)
        ctx.workflow.pipeline.print_documentation()
    except Exception as e:
        console.print(f"[red]Failed to generate documentation:[/] {e}")
        raise click.Abort()
