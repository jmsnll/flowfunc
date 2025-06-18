import sys
from pathlib import Path

import click
from rich.panel import Panel

from flowfunc.console import console

from flowfunc.console.reporter import ConsoleReporter
from flowfunc.pipeline.builder import PipelineBuilder
from flowfunc.pipeline.builder import PipelineBuildError

from flowfunc.workflow_definition.loader import WorkflowDefinitionLoader
from flowfunc.workflow_definition.loader import WorkflowDefinitionLoaderError


@click.command(name="graph", help="Graphs a workflow using matplotlib (if available).")
@click.argument(
    "workflow_path",
    type=click.Path(
        exists=True, dir_okay=False, path_type=Path
    ),
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable verbose output for loading/building steps.",
)
def graph(workflow_path: Path, verbose: bool) -> None:
    """
    Loads a workflow, builds the pipeline, and then attempts to visualize
    it using `pipefunc.Pipeline.visualize_matplotlib()`.
    """
    reporter = ConsoleReporter(rich_console=console, verbose=verbose)
    definition_loader = WorkflowDefinitionLoader()
    pipeline_builder = PipelineBuilder()

    try:
        workflow_model = None
        with reporter.status("[cyan]Loading workflow definition...[/cyan]"):
            abs_workflow_path = workflow_path.absolute()
            workflow_model = definition_loader.from_path(abs_workflow_path)

        if verbose and workflow_model:
            reporter.log_info(
                f"Workflow '{workflow_model.metadata.name}' loaded successfully."
            )

        with reporter.status(
            f"[cyan]Building pipeline for '{workflow_model.metadata.name}'...[/cyan]"
        ):
            workflow_pipeline = pipeline_builder.build(workflow_model)

        if verbose:
            reporter.log_info("Pipeline built. Attempting to generate graph...")

        # Note: visualize_matplotlib() might require a GUI backend or specific setup.
        # It typically blocks until the plot window is closed.
        console.print(
            "\n[bold green]Generating workflow graph using matplotlib...[/bold green]"
        )
        console.print("[italic]Close the matplotlib window to continue.[/italic]")
        workflow_pipeline.visualize_matplotlib()
        console.print("Graph window closed.")

    except (WorkflowDefinitionLoaderError, PipelineBuildError) as e:
        error_title = (
            "Pipeline Build Error"
            if isinstance(e, PipelineBuildError)
            else "Workflow Loading Error"
        )
        console.print(
            Panel(f"[bold red]{error_title}:[/]\n{e}", border_style="red", expand=False)
        )
        sys.exit(1)
    except ImportError as e:
        if "matplotlib" in str(e).lower():
            console.print(
                Panel(
                    "[bold red]Graphing Error:[/] Matplotlib is required for graphing but not found.\n"
                    "Please install it: `pip install matplotlib` (or `flowfunc[graph]`)",
                    border_style="red",
                    expand=False,
                )
            )
        else:
            console.print(
                Panel(
                    f"[bold red]Import Error:[/] {e}", border_style="red", expand=False
                )
            )
        sys.exit(1)
    except Exception as e:
        console.print(
            Panel(
                f"[bold red]An unexpected error occurred during graphing:[/] {e}",
                border_style="red",
                expand=False,
            )
        )
        sys.exit(1)
