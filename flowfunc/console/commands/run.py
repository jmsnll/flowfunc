import logging
import sys
from pathlib import Path

import click
from rich.panel import Panel  # Keep for error display if needed

from flowfunc.console import console  # Your rich console instance
from flowfunc.console.reporter import ConsoleReporter

# Assuming these are now the primary ways to interact with the run logic
from flowfunc.run import Status as RunStatus
from flowfunc.run import WorkflowRunCoordinator
from flowfunc.run.coordinator import WorkflowRunError  # Custom error from coordinator

logger = logging.getLogger(__name__)

# This was the original display_outputs_table, now part of ConsoleReporter
# def display_outputs_table(persisted_outputs: dict[str, str]) -> None:
#     # ... logic moved to ConsoleReporter.display_outputs_table ...


@click.command()
@click.argument(
    "workflow_path",
    type=click.Path(exists=True, path_type=Path, dir_okay=False),
)
@click.option(
    "--input-file",
    "-i",
    "input_file_path",
    type=click.Path(exists=True, path_type=Path, dir_okay=False),
    help="Path to the JSON input file for the workflow.",
)
@click.option(
    "--inputs-json",
    "-j",
    "inputs_json_string",
    type=str,
    help="JSON string containing inputs for the workflow.",
)
@click.option(
    "--name",
    "-n",
    "custom_run_name",
    type=str,
    help="A custom name prefix for the run ID.",
)
@click.option(
    "--run-id",
    "custom_run_id",
    type=str,
    help="A specific run ID to use (e.g., for reruns or specific tracking).",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output for status messages.",
)
def run(
    workflow_path: Path,
    input_file_path: Path | None,
    inputs_json_string: str | None,
    custom_run_name: str | None,
    custom_run_id: str | None,
    verbose: bool,
) -> None:
    """
    Executes a FlowFunc workflow with the specified inputs.

    WORKFLOW_PATH: Path to the workflow definition file (e.g., workflow.yaml).
    """
    if input_file_path and inputs_json_string:
        console.print(
            "[bold red]Error:[/] Please provide inputs via --input-file OR --inputs-json, not both."
        )
        sys.exit(1)

    reporter = ConsoleReporter(rich_console=console, verbose=verbose)
    coordinator = WorkflowRunCoordinator(reporter=reporter)

    input_data_dict: dict | None = None
    if inputs_json_string:
        import json

        try:
            # The InputProvider in the coordinator will handle actual parsing and validation
            # This is just a quick check to avoid passing malformed JSON string if possible
            input_data_dict = json.loads(inputs_json_string)
            if not isinstance(input_data_dict, dict):
                console.print(
                    "[bold red]Error:[/] --inputs-json must be a valid JSON object (dictionary)."
                )
                sys.exit(1)
        except json.JSONDecodeError as e:
            console.print(f"[bold red]Error parsing --inputs-json:[/] {e}")
            sys.exit(1)

    try:
        summary = coordinator.execute_workflow(
            workflow_file_path=workflow_path,
            input_file_path=input_file_path,
            input_data=input_data_dict,
            custom_run_name=custom_run_name,
            custom_run_id=custom_run_id,
        )

        if summary.status == RunStatus.SUCCESS:
            console.print(
                f"\n✅ Workflow '{summary.workflow_name}' (Run ID: {summary.run_id}) executed successfully."
            )
            sys.exit(0)
        else:
            console.print(
                f"\n❌ Workflow '{summary.workflow_name}' (Run ID: {summary.run_id}) failed."
                f"{(' Error: ' + summary.error_message) if summary.error_message else ''}"
            )
            sys.exit(1)

    except WorkflowRunError as e:
        console.print(
            Panel(
                f"[bold red]Workflow Execution Error:[/] {e}",
                border_style="red",
                expand=False,
            )
        )
        logger.error(f"Workflow execution failed: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        console.print(
            Panel(
                f"[bold red]An unexpected error occurred:[/] {e}",
                border_style="red",
                expand=False,
            )
        )
        logger.critical(
            f"Unexpected error during workflow execution: {e}", exc_info=True
        )
        sys.exit(2)
