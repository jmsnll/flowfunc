from datetime import datetime
from pathlib import Path

import pytest

from flowfunc.workflow import pipeline
from flowfunc.workflow.context import InputsContext
from flowfunc.workflow.context import MetadataContext
from flowfunc.workflow.context import OutputsContext
from flowfunc.workflow.context import PathsContext
from flowfunc.workflow.context import RunContext
from flowfunc.workflow.context import Status
from flowfunc.workflow.context import WorkflowContext


@pytest.fixture
def metadata_context(request):
    params = getattr(request, "param", {})
    return MetadataContext(
        run_id=params.get("run_id", "test-run"),
        start_time=params.get("start_time", datetime(2025, 1, 1, 0, 0, 0)),
        end_time=params.get("end_time", datetime(2025, 1, 1, 1, 0, 0)),
        duration_seconds=params.get("duration_seconds", 3600.0),
        status=params.get("status", Status.SUCCESS),
    )


@pytest.fixture
def workflow_context(request, load_example_workflow):
    params = getattr(request, "param", {})
    with_example = params.get("with_example")
    assert with_example, (
        "Expected 'with_example' to be provided in the test parametrize block for workflow_context fixture."
    )
    workflow = load_example_workflow(with_example)
    return WorkflowContext(
        pipeline=params.get("pipeline", pipeline.from_model(workflow)),
        model=params.get("model", workflow),
        file_path=params.get("file_path", Path("/path/to/workflow.yaml")),
    )


@pytest.fixture
def inputs_context(request):
    params = getattr(request, "param", {})
    return InputsContext(
        user_inputs=params.get("user_inputs", {}),
        resolved_inputs=params.get("resolved_inputs", {}),
    )


@pytest.fixture
def outputs_context(request):
    params = getattr(request, "param", {})
    return OutputsContext(
        results=params.get("results", {}),
        persisted_outputs=params.get("persisted_outputs", {}),
    )


@pytest.fixture
def paths_context(request):
    params = getattr(request, "param", {})
    run_dir = params.get("run_dir", Path("/tmp/test-run"))
    return PathsContext(
        run_dir=run_dir,
        output_dir=params.get("output_dir", run_dir / "outputs"),
    )


@pytest.fixture
def run_context(
    metadata_context, workflow_context, inputs_context, outputs_context, paths_context
):
    return RunContext(
        metadata=metadata_context,
        workflow=workflow_context,
        inputs=inputs_context,
        outputs=outputs_context,
        paths=paths_context,
    )
