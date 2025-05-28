from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.text import Text
from rich.traceback import install

from flowfunc import workflow
from flowfunc.pyproject.toml import load_flowfunc_toml
from flowfunc.workflow import inputs
from flowfunc.workflow import outputs
from flowfunc.workflow import runs
from flowfunc.workflow.context import PathsContext
from flowfunc.workflow.context import RunContext
from flowfunc.workflow.context import Status

# Setup Rich
install(show_locals=True, width=200)
console = Console()
logging.basicConfig(
    level="DEBUG",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
logger = logging.getLogger("flowfunc")


@click.command(
    name="run",
    help="Runs a workflow, managing run history, outputs, and run-specific caching.",
)
@click.option(
    "--input-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
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
    type=click.Path(exists=True, dir_okay=False, readable=True),
)
def run(
    input_file: click.Path | None, name: str | None, workflow_path: Path, verbose: bool
) -> None:
    if not verbose:
        logging.getLogger().setLevel(logging.INFO)

    ctx = RunContext()
    with console.status("[bold cyan]Loading workflow...", spinner="dots"):
        workflow.load(ctx.workflow, Path(workflow_path))

    ctx.metadata.run_id = runs.generate_unique_id() if not name else name
    ctx.metadata.start_time = datetime.now()
    ctx.metadata.status = Status.FAILED
    ctx.paths = PathsContext.build(
        ctx.workflow.model.metadata.name,
        ctx.metadata.run_id,
        load_flowfunc_toml(),
    )
    ctx.metadata.start()

    console.print(
        Rule(
            title=f"🛠️ Running: [green]{ctx.workflow.model.metadata.name}[/]",
            style="bold green",
        )
    )

    try:
        logger.info(f"[bold]Run ID:[/] {ctx.metadata.run_id}")
        logger.info(f"📂 Output directory: {ctx.paths.output_dir}")

        if input_file:
            with console.status("📥 Loading input file...", spinner="point"):
                ctx.inputs.user_inputs = inputs.from_file(input_file)
                logger.debug(f"Loaded user input: {ctx.inputs.user_inputs}")

        with console.status("🧩 Resolving inputs...", spinner="bouncingBar"):
            ctx.inputs.resolved_inputs = inputs.resolve(
                ctx.inputs.user_inputs,
                ctx.workflow.model.spec.global_inputs,
                ctx.workflow.pipeline.info().get("inputs", tuple()),
                ctx.workflow.pipeline.info().get("required_inputs", []),
            )

        with console.status("⚙️ Executing pipeline...", spinner="earth"):
            ctx.outputs.results = ctx.workflow.pipeline.map(ctx.inputs.resolved_inputs)
            ctx.metadata.status = Status.SUCCESS.value

        with console.status("💾 Persisting outputs...", spinner="dots2"):
            ctx.outputs.persisted_outputs = outputs.persist_workflow_outputs(
                ctx.outputs.results,
                ctx.workflow.model.spec.pipeline_outputs,
                ctx.paths.output_dir,
            )

        # Summary table
        table = Table(
            title="✅ Workflow Completed", show_header=True, header_style="bold magenta"
        )
        table.add_column("Output Key")
        table.add_column("Path", overflow="fold")
        for k, v in ctx.outputs.persisted_outputs.items():
            table.add_row(k, str(v))
        console.print(table)

        console.print(
            Panel.fit(
                Text(
                    f"Run ID: {ctx.metadata.run_id}\nStarted: {ctx.metadata.start_time}\nOutput: {ctx.paths.output_dir}",
                    style="green",
                ),
                title="📝 Summary",
                border_style="green",
            )
        )

    except Exception:
        console.print_exception(show_locals=True)
        logger.error(
            f"[red]An error occurred during workflow run {ctx.metadata.run_id}[/]"
        )
    finally:
        ctx.save_summary()

    exit_code = 0 if ctx.metadata.status == Status.SUCCESS else 1
    raise SystemExit(exit_code)
