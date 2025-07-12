from typing import Any

import pytest

from flowfunc.composition.step import resolve_defaults
from flowfunc.composition.step import resolve_inputs
from flowfunc.composition.step import resolve_resources
from flowfunc.composition.step import resolve_scope
from flowfunc.exceptions import PipelineBuildError
from flowfunc.workflow_definition import Resources
from flowfunc.workflow_definition import StepDefinition
from flowfunc.workflow_definition import StepOptions
from flowfunc.workflow_definition import WorkflowSpec
from flowfunc.workflow_definition import WorkflowSpecOptions
from flowfunc.workflow_definition.schema import InputItem
from flowfunc.workflow_definition.schema import KindEnum
from flowfunc.workflow_definition.schema import Metadata
from flowfunc.workflow_definition.schema import WorkflowDefinition


@pytest.fixture
def empty_workflow() -> WorkflowDefinition:
    """Provides a basic, empty WorkflowDefinition."""
    return WorkflowDefinition(
        apiVersion="flowfunc.dev/v1beta1",
        kind=KindEnum.PIPELINE,
        metadata=Metadata(name="test-workflow"),
        spec=WorkflowSpec(steps=[]),
    )


@pytest.fixture
def initial_options() -> StepOptions:
    """Provides a default StepOptions object."""
    return StepOptions()


@pytest.fixture
def rendering_context() -> dict[str, Any]:
    """Provides a mock rendering context for Jinja2."""
    return {
        "globals": {"run_id": "run-xyz-123", "data_dir": "/data"},
        "steps": {
            "step_one": {
                "outputs": {
                    "raw_data": "step_one_raw_data_output",
                    "processed_data": "step_one_processed_data_output",
                }
            }
        },
    }


def test_resolve_inputs_no_inputs(
    initial_options: StepOptions, rendering_context: dict[str, Any]
) -> None:
    step = StepDefinition(name="test_step", consumes=None)
    options = resolve_inputs(initial_options, step, rendering_context)
    assert options.renames == {}
    assert options.defaults == {}


def test_resolve_inputs_handles_literals_as_defaults(
    initial_options: StepOptions, rendering_context: dict[str, Any]
) -> None:
    step = StepDefinition(
        name="test_step",
        consumes={
            "path": "/absolute/path/file.txt",
            "retries": 5,
            "is_active": True,
        },
    )
    options = resolve_inputs(initial_options, step, rendering_context)

    assert options.renames == {}
    assert options.defaults == {
        "path": "/absolute/path/file.txt",
        "retries": 5,
        "is_active": True,
    }


def test_resolve_inputs_as_rename(
    initial_options: StepOptions, rendering_context: dict[str, Any]
) -> None:
    step = StepDefinition(
        name="test_step",
        consumes={
            "input_data": InputItem(value="{{ steps.step_one.outputs.raw_data }}")
        },
    )
    options = resolve_inputs(initial_options, step, rendering_context)
    assert options.renames == {"input_data": "step_one_raw_data_output"}
    assert options.defaults == {}


def test_resolve_inputs_as_default(
    initial_options: StepOptions, rendering_context: dict[str, Any]
) -> None:
    step = StepDefinition(
        name="test_step",
        consumes={"output_path": "path/{{ globals.run_id }}/data.csv"},
    )
    options = resolve_inputs(initial_options, step, rendering_context)
    assert options.defaults == {"output_path": "path/{{ globals.run_id }}/data.csv"}
    assert options.renames == {"output_path": "path/run-xyz-123/data.csv"}


def test_resolve_inputs_merges_with_existing_options(
    initial_options: StepOptions, rendering_context: dict[str, Any]
) -> None:
    initial_options.renames = {"existing_rename": "source_one"}
    initial_options.defaults = {"existing_default": "value1"}

    step = StepDefinition(
        name="test_step",
        consumes={
            "new_input": "{{ steps.step_one.outputs.processed_data }}",
            "new_default": "config-{{ globals.run_id }}.json",
        },
    )
    options = resolve_inputs(initial_options, step, rendering_context)

    assert options.renames == {
        "existing_rename": "source_one",
        "new_input": "step_one_processed_data_output",
        "new_default": "config-run-xyz-123.json",
    }
    assert options.defaults == {
        "existing_default": "value1",
        "new_default": "config-{{ globals.run_id }}.json",
    }


def test_resolve_inputs_raises_for_invalid_reference(
    initial_options: StepOptions, rendering_context: dict[str, Any]
) -> None:
    step = StepDefinition(
        name="test_step",
        consumes={"bad_input": "{{ steps.non_existent.outputs.data }}"},
    )

    with pytest.raises(PipelineBuildError) as excinfo:
        resolve_inputs(initial_options, step, rendering_context)

    assert "In step 'test_step', input 'bad_input' has an invalid reference" in str(
        excinfo.value
    )
    assert "non_existent" in str(excinfo.value)


def test_resolve_inputs_handles_mixed_types(
    initial_options: StepOptions, rendering_context: dict[str, Any]
) -> None:
    step = StepDefinition(
        name="mixed_step",
        consumes={
            "direct_ref": "{{ steps.step_one.outputs.raw_data }}",
            "interpolated_path": "{{ globals.data_dir }}/{{ globals.run_id }}",
            "literal_string": "just-a-string",
            "literal_int": 123,
        },
    )
    options = resolve_inputs(initial_options, step, rendering_context)

    assert options.renames == {
        "direct_ref": "step_one_raw_data_output",
        "interpolated_path": "/data/run-xyz-123",
    }
    assert options.defaults == {
        "interpolated_path": "{{ globals.data_dir }}/{{ globals.run_id }}",
        "literal_string": "just-a-string",
        "literal_int": 123,
    }


def test_resolve_defaults_with_parameters(initial_options) -> None:
    step = StepDefinition(name="test_step", params={"param1": 100, "param2": "value"})
    options = resolve_defaults(initial_options, step)
    assert options.defaults == {"param1": 100, "param2": "value"}


def test_resolve_defaults_merges_with_existing(initial_options) -> None:
    step = StepDefinition(name="test_step", params={"new_param": "new_value"})
    initial_options.defaults = {"existing_param": "old_value"}
    options = resolve_defaults(initial_options, step)
    assert options.defaults == {
        "existing_param": "old_value",
        "new_param": "new_value",
    }


def test_resolve_defaults_no_parameters(initial_options) -> None:
    step = StepDefinition(name="test_step")
    options = resolve_defaults(initial_options, step)
    assert not options.defaults


def test_resolve_resources_merges_global_and_step(
    initial_options, empty_workflow
) -> None:
    empty_workflow.spec.options = WorkflowSpecOptions(
        default_resources=Resources(cpus=2, memory="4Gi")
    )
    step = StepDefinition(name="test_step", resources=Resources(memory="8Gi"))
    options = resolve_resources(initial_options, step, workflow=empty_workflow)
    assert options.resources == {"cpus": 2, "memory": "8Gi"}


def test_resolve_resources_only_global(initial_options, empty_workflow) -> None:
    empty_workflow.spec.options = WorkflowSpecOptions(
        default_resources=Resources(cpus=4)
    )
    step = StepDefinition(name="test_step")
    options = resolve_resources(initial_options, step, workflow=empty_workflow)
    assert options.resources == {"cpus": 4}


def test_resolve_resources_only_step(initial_options, empty_workflow) -> None:
    step = StepDefinition(name="test_step", resources=Resources(memory="16Gi"))
    options = resolve_resources(initial_options, step, workflow=empty_workflow)
    assert options.resources == {"memory": "16Gi"}


def test_resolve_resources_none_defined(initial_options, empty_workflow) -> None:
    step = StepDefinition(name="test_step")
    options = resolve_resources(initial_options, step, workflow=empty_workflow)
    assert not options.resources


def test_resolve_scope_from_step_options(initial_options) -> None:
    scope_name = "my_scope"
    step = StepDefinition(name="test_step", options=StepOptions(scope=scope_name))
    options = resolve_scope(initial_options, step)
    assert options.scope == scope_name


def test_resolve_scope_not_defined(initial_options) -> None:
    step = StepDefinition(name="test_step")
    options = resolve_scope(initial_options, step)
    assert options.scope is None


def test_resolve_scope_is_none_in_options(initial_options) -> None:
    step = StepDefinition(name="test_step", options=StepOptions(scope=None))
    options = resolve_scope(initial_options, step)
    assert options.scope is None
