# Fixtures for shared objects
import pytest

from flowfunc.composition.step import resolve_defaults
from flowfunc.composition.step import resolve_renames
from flowfunc.composition.step import resolve_resources
from flowfunc.composition.step import resolve_scope
from flowfunc.workflow_definition import Resources
from flowfunc.workflow_definition import StepDefinition
from flowfunc.workflow_definition import StepOptions
from flowfunc.workflow_definition import WorkflowSpec
from flowfunc.workflow_definition import WorkflowSpecOptions
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


# Tests for resolve_renames
def test_resolve_renames_with_global_inputs(initial_options):
    step = StepDefinition(
        name="test_step",
        inputs={
            "local_name": "$global.global_name",
            "same_name": "$global.same_name",
            "non_global": "another_step.output",
        },
    )
    options = resolve_renames(initial_options, step)
    assert options.renames == {"local_name": "global_name"}


def test_resolve_renames_merges_with_existing(initial_options):
    step = StepDefinition(
        name="test_step",
        inputs={"new_input": "$global.new_global"},
    )
    initial_options.renames = {"existing": "old_global"}
    options = resolve_renames(initial_options, step)
    assert options.renames == {"existing": "old_global", "new_input": "new_global"}


def test_resolve_renames_no_globals(initial_options):
    step = StepDefinition(
        name="test_step",
        inputs={"input1": "step1.output", "input2": "step2.output"},
    )
    options = resolve_renames(initial_options, step)
    assert not options.renames


def test_resolve_renames_no_inputs(initial_options):
    step = StepDefinition(name="test_step")
    options = resolve_renames(initial_options, step)
    assert not options.renames


# Tests for resolve_defaults
def test_resolve_defaults_with_parameters(initial_options):
    step = StepDefinition(
        name="test_step", parameters={"param1": 100, "param2": "value"}
    )
    options = resolve_defaults(initial_options, step)
    assert options.defaults == {"param1": 100, "param2": "value"}


def test_resolve_defaults_merges_with_existing(initial_options):
    step = StepDefinition(name="test_step", parameters={"new_param": "new_value"})
    initial_options.defaults = {"existing_param": "old_value"}
    options = resolve_defaults(initial_options, step)
    assert options.defaults == {
        "existing_param": "old_value",
        "new_param": "new_value",
    }


def test_resolve_defaults_no_parameters(initial_options):
    step = StepDefinition(name="test_step")
    options = resolve_defaults(initial_options, step)
    assert not options.defaults


# Tests for resolve_resources
def test_resolve_resources_merges_global_and_step(initial_options, empty_workflow):
    empty_workflow.spec.options = WorkflowSpecOptions(
        default_resources=Resources(cpus=2, memory="4Gi")
    )
    step = StepDefinition(name="test_step", resources=Resources(memory="8Gi"))
    options = resolve_resources(initial_options, step, workflow=empty_workflow)
    assert options.resources == {"cpus": 2, "memory": "8Gi"}


def test_resolve_resources_only_global(initial_options, empty_workflow):
    empty_workflow.spec.options = WorkflowSpecOptions(
        default_resources=Resources(cpus=4)
    )
    step = StepDefinition(name="test_step")
    options = resolve_resources(initial_options, step, workflow=empty_workflow)
    assert options.resources == {"cpus": 4}


def test_resolve_resources_only_step(initial_options, empty_workflow):
    step = StepDefinition(name="test_step", resources=Resources(memory="16Gi"))
    options = resolve_resources(initial_options, step, workflow=empty_workflow)
    assert options.resources == {"memory": "16Gi"}


def test_resolve_resources_none_defined(initial_options, empty_workflow):
    step = StepDefinition(name="test_step")
    options = resolve_resources(initial_options, step, workflow=empty_workflow)
    assert not options.resources


# Tests for resolve_scope
def test_resolve_scope_from_step_options(initial_options):
    scope_name = "my_scope"
    step = StepDefinition(name="test_step", options=StepOptions(scope=scope_name))
    options = resolve_scope(initial_options, step)
    assert options.scope == scope_name


def test_resolve_scope_not_defined(initial_options):
    step = StepDefinition(name="test_step")
    options = resolve_scope(initial_options, step)
    assert options.scope is None


def test_resolve_scope_is_none_in_options(initial_options):
    step = StepDefinition(name="test_step", options=StepOptions(scope=None))
    options = resolve_scope(initial_options, step)
    assert options.scope is None
