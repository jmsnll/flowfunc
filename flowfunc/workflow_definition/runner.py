import logging
from pathlib import Path
from typing import Any

import pipefunc

from flowfunc.console import status
from flowfunc.locations import workflow_run_dir
from flowfunc.pyproject.toml import load_flowfunc_toml
from flowfunc.workflow import inputs
from flowfunc.workflow import loader
from flowfunc.workflow import outputs
from flowfunc.workflow import pipeline
from flowfunc.workflow import schema
from flowfunc.workflow.summary import Summary
from flowfunc.workflow.utils import generate_unique_id

logger = logging.getLogger(__name__)


@status("[bold cyan]Loading workflow...")
def initialise_workflow(
    workflow_path: Path,
) -> tuple[schema.WorkflowDefinition, pipefunc.Pipeline]:
    workflow_model = loader.from_path(workflow_path.absolute())
    workflow_pipeline = pipeline.from_model(workflow_model)
    return workflow_model, workflow_pipeline


@status("📥 Loading input file...")
def load_inputs(input_file: Path) -> dict[str, Any]:
    return inputs.from_file(input_file)


@status("🧩 Resolving inputs...")
def resolve_inputs(
    workflow_model: schema.WorkflowDefinition,
    pipeline_info: dict[str, Any],
    user_inputs: dict[str, str],
) -> dict[str, Any]:
    return inputs.resolve(
        user_inputs,
        workflow_model.spec.inputs,
        pipeline_info.get("inputs", ()),
        pipeline_info.get("required_inputs", []),
        workflow_model.spec.options.scope,
    )


@status("⚙️ Executing pipeline...")
def execute_pipeline_core(
    workflow_pipeline: pipefunc.Pipeline, resolved_inputs: dict[str, str]
) -> dict[str, Any]:
    return workflow_pipeline.map(resolved_inputs)


@status("💾 Persisting outputs...")
def persist_outputs(
    results: dict[str, Any], output_definitions: dict[str, str], output_directory: Path
) -> dict[str, str]:
    return outputs.persist(
        results,
        output_definitions,
        output_directory,
    )


@status("📝 Saving run summary...")
def save_summary(summary: Summary, run_directory: Path) -> None:
    summary_file = run_directory / "summary.json"
    summary_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.write_text(summary.model_dump_json(indent=2))


def execute_pipeline(
    workflow_path: Path,
    input_file: Path,
    *,
    run_id: str,
) -> Summary:
    """Runs the full pipeline: load workflow, inputs, resolve, execute, persist outputs."""
    workflow_model, workflow_pipeline = initialise_workflow(workflow_path)
    run_id = run_id or generate_unique_id()
    logger.info(f"[bold]Run ID:[/] {run_id}")
    pyproject_config = load_flowfunc_toml()

    summary = Summary(
        run_id=run_id,
        run_dir=workflow_run_dir(
            pyproject_config, workflow_model.metadata.name, run_id
        ),
        workflow_file=input_file,
        workflow_name=workflow_model.metadata.name,
    )

    logger.info(f"📂    Run directory: {summary.run_dir}")
    logger.info(f"📂 Output directory: {summary.output_dir}")

    try:
        summary.user_inputs = load_inputs(input_file) if input_file else {}
        summary.resolved_inputs = resolve_inputs(
            workflow_model, workflow_pipeline.info(), summary.user_inputs
        )

        results = execute_pipeline_core(workflow_pipeline, summary.resolved_inputs)

        summary.persisted_outputs = persist_outputs(
            results, workflow_model.spec.outputs, summary.output_dir
        )
        summary.finished()
    except Exception:
        logger.exception(f"Error during {summary.run_id}")
        raise

    finally:
        summary.save()

    return summary
