import pytest

from flowfunc.composition.step import resolve_defaults
from flowfunc.composition.step import resolve_renames
from flowfunc.composition.step import resolve_resources
from flowfunc.composition.step import resolve_scope
from flowfunc.composition.step import validate_step_dependencies_exist
from flowfunc.composition.step import validate_step_name_is_unique
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


def test_validate_step_name_is_unique_success(empty_workflow, initial_options):
    empty_workflow.spec.steps = [
        StepDefinition(name="step-one"),
        StepDefinition(name="step-two"),
    ]
    try:
        validate_step_name_is_unique(workflow=empty_workflow, options=initial_options)
    except PipelineBuildError:
        pytest.fail("validate_step_name_is_unique raised an exception unexpectedly.")


def test_validate_step_name_is_unique_failure_duplicate_name(
    empty_workflow, initial_options
):
    empty_workflow.spec.steps = [
        StepDefinition(name="step-one"),
        StepDefinition(name="step-two"),
        StepDefinition(name="step-one"),
    ]
    with pytest.raises(PipelineBuildError) as excinfo:
        validate_step_name_is_unique(workflow=empty_workflow, options=initial_options)

    assert "Duplicate step name 'step-one' found" in str(excinfo.value)


def test_validate_step_name_is_unique_with_no_steps(empty_workflow, initial_options):
    empty_workflow.spec.steps = []
    try:
        validate_step_name_is_unique(workflow=empty_workflow, options=initial_options)
    except PipelineBuildError:
        pytest.fail(
            "validate_step_name_is_unique raised an exception on an empty workflow."
        )


def test_validate_step_dependencies_exist_success(empty_workflow, initial_options):
    empty_workflow.spec.steps = [
        StepDefinition(name="step-one"),
        StepDefinition(
            name="step-two",
            inputs={"data": InputItem(value="step-one.output")},
        ),
    ]
    try:
        validate_step_dependencies_exist(
            workflow=empty_workflow, options=initial_options
        )
    except PipelineBuildError:
        pytest.fail(
            "validate_step_dependencies_exist raised an exception unexpectedly."
        )


def test_validate_step_dependencies_exist_with_global_success(
    empty_workflow, initial_options
):
    empty_workflow.spec.steps = [
        StepDefinition(
            name="step-one",
            inputs={"config": InputItem(value="$global.my_config")},
        ),
    ]
    try:
        validate_step_dependencies_exist(
            workflow=empty_workflow, options=initial_options
        )
    except PipelineBuildError:
        pytest.fail("Validator failed on a valid global dependency.")


def test_validate_step_dependencies_exist_failure_unknown_step(
    empty_workflow, initial_options
):
    empty_workflow.spec.steps = [
        StepDefinition(name="step-one"),
        StepDefinition(
            name="step-two",
            inputs={"data": InputItem(value="unknown_step.output")},
        ),
    ]
    with pytest.raises(PipelineBuildError) as excinfo:
        validate_step_dependencies_exist(
            workflow=empty_workflow, options=initial_options
        )

    assert "undefined dependency" in str(excinfo.value)
    assert "refers to an unknown step 'unknown_step'" in str(excinfo.value)


def test_validate_step_dependencies_exist_no_dependencies(
    empty_workflow, initial_options
):
    empty_workflow.spec.steps = [
        StepDefinition(name="step-one"),
        StepDefinition(name="step-two", parameters={"param": 123}),
    ]
    try:
        validate_step_dependencies_exist(
            workflow=empty_workflow, options=initial_options
        )
    except PipelineBuildError:
        pytest.fail("Validator failed on a workflow with no dependencies.")
