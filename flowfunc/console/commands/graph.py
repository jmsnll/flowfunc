from pathlib import Path

import click
from rich.console import Console

from flowfunc import workflow
from flowfunc.workflow.context import RunContext

console = Console()


@click.command(name="graph", help="Graphs a workflow using matplotlib.")
@click.argument("workflow_path", type=click.Path(exists=True, dir_okay=False))
def graph(workflow_path: str) -> None:
    ctx = RunContext()

    try:
        console.status("[bold green]Loading workflow...", spinner="dots")
        workflow.load(Path(workflow_path), ctx.workflow)

        console.log(
            f"[green]✅ Loaded workflow:[/green] {ctx.workflow.model.metadata.name}"
        )
        console.log("[cyan]🧠 Visualizing pipeline with matplotlib...[/cyan]")

        ctx.workflow.pipeline.visualize_matplotlib()

        console.log("[bold green]✅ Graph visualization complete.[/bold green]")

    except Exception as e:
        console.log(f"[bold red]❌ Error while graphing: {e}[/bold red]")
        raise click.Abort()
