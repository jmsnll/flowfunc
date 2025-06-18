import sys
from pathlib import Path

import click
from rich.panel import Panel

from flowfunc.console import console
from flowfunc.pipeline.builder import PipelineBuilder
from flowfunc.pipeline.builder import PipelineBuildError

# Import the refactored classes
from flowfunc.workflow_definition.loader import WorkflowDefinitionLoader
from flowfunc.workflow_definition.loader import WorkflowDefinitionLoaderError


@click.command("docs", help="Displays the documentation for a workflow.")
@click.argument(
    "workflow_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable verbose output (currently affects logging levels).",
)
def docs(workflow_path: Path, verbose: bool) -> None:
    """
    Displays the generated documentation for a given workflow definition file.
    This involves parsing the workflow and constructing the pipeline to access
    its documentation string.
    """
    definition_loader = WorkflowDefinitionLoader()
    pipeline_builder = PipelineBuilder()

    try:
        console.print(
            f"Loading workflow definition from: [cyan]{workflow_path.absolute()}[/cyan]"
        )
        workflow_model = definition_loader.from_path(workflow_path.absolute())

        if verbose:
            console.print("\n[bold green]Workflow Model Snippet:[/bold green]")
            console.print(workflow_model.model_dump(mode='json', exclude_none=True))

        console.print(
            f"Building pipeline for: [cyan]{workflow_model.metadata.name}[/cyan]"
        )
        workflow_pipeline = pipeline_builder.build(workflow_model)

        console.print("\n[bold green]Workflow Documentation:[/bold green]")
        # pipefunc.Pipeline.print_documentation() usually prints to stdout.
        # If flowfunc.console.console has redirected stdout, this should be fine.
        workflow_pipeline.print_documentation()

    except (WorkflowDefinitionLoaderError, PipelineBuildError) as e:
        error_title = (
            "Pipeline Build Error"
            if isinstance(e, PipelineBuildError)
            else "Workflow Loading Error"
        )
        console.print(
            Panel(f"[bold red]{error_title}:[/]\n{e}", border_style="red", expand=False)
        )
        sys.exit(1)  # Use sys.exit for non-zero exit code
    except Exception as e:
        console.print(
            Panel(
                f"[bold red]An unexpected error occurred:[/] {e}",
                border_style="red",
                expand=False,
            )
        )
        sys.exit(1)
