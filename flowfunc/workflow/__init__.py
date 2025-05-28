from pathlib import Path

from flowfunc.workflow import loader
from flowfunc.workflow import pipeline
from flowfunc.workflow.context import WorkflowContext


def load(ctx: WorkflowContext, workflow_path: Path) -> None:
    if not workflow_path.exists():
        raise FileNotFoundError(f"Workflow file not found: {workflow_path}")

    ctx.file_path = workflow_path
    ctx.model = loader.load_from_path(workflow_path)
    ctx.pipeline = pipeline.from_model(ctx.model)
