from __future__ import annotations

import logging
from pathlib import Path

import click

from flowfunc.workflow import runner
from flowfunc.workflow.context import RunContext
from flowfunc.workflow.context import Status
from flowfunc.workflow.utils import generate_unique_id


@click.command(
    name="run",
    help="Runs a workflow, managing run history, outputs, and run-specific caching.",
)
@click.option(
    "--input-file",
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
    help="Path to a JSON file containing input data for the workflow.",
    required=False,
)
@click.option(
    "--name",
    type=str,
    help="A custom name for this run (will be part of the run ID / directory).",
    required=False,
)
@click.option(
    "-v", "--verbose", is_flag=True, default=False, help="Enable verbose output."
)
@click.argument(
    "workflow_path",
    nargs=1,
    type=click.Path(exists=True, dir_okay=False, readable=True, path_type=Path),
)
def run(
    input_file: click.Path | None, name: str | None, workflow_path: Path, verbose: bool
) -> None:
    if not verbose:
        logging.getLogger().setLevel(logging.INFO)

    ctx = RunContext()
    ctx.metadata.run_id = generate_unique_id(name)
    runner.execute_pipeline(ctx, workflow_path, input_file)

    raise SystemExit(0 if ctx.metadata.status == Status.SUCCESS else 1)
