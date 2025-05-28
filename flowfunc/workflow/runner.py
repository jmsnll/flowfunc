import logging
from datetime import datetime
from pathlib import Path

from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from flowfunc import locations
from flowfunc import workflow
from flowfunc.console import console
from flowfunc.console import status
from flowfunc.pyproject.toml import load_flowfunc_toml
from flowfunc.workflow import inputs
from flowfunc.workflow import outputs
from flowfunc.workflow.context import PathsContext
from flowfunc.workflow.context import RunContext
from flowfunc.workflow.context import Status
from flowfunc.workflow.utils import generate_unique_id

logger = logging.getLogger(__name__)


@status("[bold cyan]Loading workflow...")
def load_workflow(ctx: RunContext, workflow_path: Path):
    workflow.load(ctx.workflow, locations.project_root() / workflow_path)


@status("📥 Loading input file...")
def load_inputs(ctx: RunContext, input_file: Path):
    ctx.inputs.user_inputs = inputs.from_file(input_file)


@status("🧩 Resolving inputs...")
def resolve_inputs(ctx: RunContext):
    pipeline_info = ctx.workflow.pipeline.info()
    ctx.inputs.resolved_inputs = inputs.resolve(
        ctx.inputs.user_inputs,
        ctx.workflow.model.spec.global_inputs,
        pipeline_info.get("inputs", ()),
        pipeline_info.get("required_inputs", []),
    )


@status("⚙️ Executing pipeline...")
def execute_pipeline_core(ctx: RunContext):
    ctx.outputs.results = ctx.workflow.pipeline.map(ctx.inputs.resolved_inputs)


@status("💾 Persisting outputs...")
def persist_outputs(ctx: RunContext):
    ctx.outputs.persisted_outputs = outputs.persist_workflow_outputs(
        ctx.outputs.results,
        ctx.workflow.model.spec.outputs,
        ctx.paths.output_dir,
    )


@status("📝 Saving run summary...")
def save_summary(ctx: RunContext):
    ctx.save_summary()


def execute_pipeline(
    ctx: RunContext,
    workflow_path: Path,
    input_file: Path,
) -> RunContext:
    """
    Runs the full pipeline: load workflow, inputs, resolve, execute, persist outputs.
    """
    load_workflow(ctx, workflow_path)

    ctx.metadata.run_id = ctx.metadata.run_id or generate_unique_id()
    ctx.metadata.start_time = datetime.now()
    ctx.metadata.status = Status.FAILED

    ctx.paths = PathsContext.build(
        ctx.workflow.model.metadata.name,
        ctx.metadata.run_id,
        load_flowfunc_toml(),
    )

    if hasattr(ctx.metadata, "start") and callable(ctx.metadata.start):
        ctx.metadata.start()

    logger.info(f"[bold]Run ID:[/] {ctx.metadata.run_id}")
    logger.info(f"📂 Output directory: {ctx.paths.output_dir}")

    try:
        if input_file:
            load_inputs(ctx, input_file)
        resolve_inputs(ctx)
        execute_pipeline_core(ctx)

        ctx.metadata.status = Status.SUCCESS.value
        logger.info("Run status set to SUCCESS")

        persist_outputs(ctx)

    except Exception:
        logger.exception(f"Error during run {ctx.metadata.run_id}")
        raise

    finally:
        save_summary(ctx)

        # Summary table
        table = Table(title="Outputs", show_header=True, header_style="bold magenta")
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
    return ctx
