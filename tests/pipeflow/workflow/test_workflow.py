import copy  # For deepcopy

import pipefunc
import pytest

from pipeflow.workflow.exceptions import PipelineBuildError
from pipeflow.workflow.workflow import PipeflowFunc, Workflow


@pytest.fixture
def base_workflow_specification():
    return {
        "apiVersion": "workflows/v1",
        "kind": "Pipeline",
        "metadata": {
            "name": "example-workflow",
            "version": "1.0.1",
            "description": "An example workflow for testing.",
        },
        "spec": {
            "global_inputs": {},
            "steps": [
                {
                    "id": "calculate_md5_hash",
                    "function": "tests.pipeflow.workflow.helpers.md5_hash",
                    "description": "Calculates an MD5 hash of input data.",
                    "inputs": ["text_to_be_hashed"],
                    "pipefunc_options": {
                        "output_name": "hashed_text",
                        "mapspec": "text_to_be_hashed[n] -> hashed_text[n]",
                        # "renames": {
                        #     "credit_card_number": "data",
                        # },
                        "defaults": {
                            "text_to_be_hashed": "123",
                        },
                        "profile": True,
                        "debug": True,
                    },
                },
                {
                    "id": "make_bold",
                    "function": "tests.pipeflow.workflow.helpers.markdown_make_bold",
                    "description": "Wraps the input text in asterix to make it appear as bold in markdown.",
                    "inputs": ["hashed_text"],
                    "pipefunc_options": {
                        "output_name": "bold_string",
                        "mapspec": "hashed_text[n] -> bold_string[n]",
                        "profile": True,
                        "debug": True,
                        "renames": {
                            "text_to_be_made_bold": "hashed_text",
                        },
                    },
                },
            ],
        },
        "outputs": ["bold_string"],
    }


@pytest.fixture
def base_workflow_step_specification(base_workflow_specification):
    """Provides the base, unmodified configuration for a workflow."""
    return base_workflow_specification["spec"]["steps"][0]


@pytest.fixture
def pipe_func(base_workflow_step_specification):
    """
    Returns a PipeflowFunc instance created from the
    standard base_workflow_config.
    """
    return PipeflowFunc.from_dict(base_workflow_step_specification)


@pytest.fixture
def pipe_func_options(base_workflow_step_specification):
    """
    Returns the pipefunc_options from the standard base_workflow_config.
    """
    return base_workflow_step_specification.get("pipefunc_options", {})


PARAMETRIZED_CHECKS = [
    (
        "output_name_check_matches_config_value",  # test_id
        lambda pf: pf.output_name,
        lambda config, opts: opts.get("output_name"),
    ),
    (
        "renames_dict_check_matches_config_value",
        lambda pf: pf.renames,
        lambda config, opts: opts.get("renames"),
    ),
    (
        "defaults_dict_check_matches_config_value",
        lambda pf: pf.defaults,
        lambda config, opts: opts.get("defaults"),
    ),
    (
        "profile_flag_check_matches_config_value",
        lambda pf: pf.profile,
        lambda config, opts: opts.get("profile", False),
    ),
    (
        "debug_flag_check_matches_config_value",
        lambda pf: pf.debug,
        lambda config, opts: opts.get("debug", False),
    ),
    (
        "cache_flag_check_matches_config_value",
        lambda pf: pf.cache,
        lambda config, opts: opts.get("cache", False),
    ),
    (
        "mapspec_input_names_check_match_config_inputs_set",
        lambda pf: set(pf.mapspec.input_names),
        lambda config, opts: set(config.get("inputs", set())),
    ),
    (
        "original_parameters_keys_check_match_config_renames_keys_set",
        lambda pf: set(pf.original_parameters.keys()),
        lambda config, opts: set(opts.get("renames", {}).keys()),
    ),
]


@pytest.mark.parametrize(
    "test_id, get_actual_value, get_expected_value",
    PARAMETRIZED_CHECKS,
    ids=[
        check[0] for check in PARAMETRIZED_CHECKS
    ],  # Uses the descriptive test_id for pytest output
)
def test_workflow_step_creation_with_standard_properties(  # Subject: PipeflowFunc standard properties
    pipe_func,
    base_workflow_step_specification,
    pipe_func_options,
    test_id,
    get_actual_value,
    get_expected_value,  # test_id is now e.g., "output_name_check_matches_config_value"
):
    actual_value = get_actual_value(pipe_func)
    expected_value = get_expected_value(
        base_workflow_step_specification, pipe_func_options
    )
    assert actual_value == expected_value, f"Check failed for property: {test_id}"


def test_workflow_step_creation_with_inconsistent_renames_for_inputs_check_raises_pipeline_build_error(
    base_workflow_step_specification,
):
    modified_config = copy.deepcopy(base_workflow_step_specification)
    if "renames" in modified_config["pipefunc_options"]:
        del modified_config["pipefunc_options"]["renames"]

    with pytest.raises(PipelineBuildError):
        PipeflowFunc.from_dict(modified_config)


def test_workflow_step_creation_with_scope_in_options_check_renames_values_are_prefixed(
    base_workflow_step_specification,
):
    modified_config = copy.deepcopy(base_workflow_step_specification)
    scope_name = "example_scope"
    modified_config["pipefunc_options"]["scope"] = scope_name

    function = PipeflowFunc.from_dict(modified_config)

    assert function.renames is not None, "Instance 'renames' attribute should exist."
    assert (
        len(function.renames) > 0
    ), "Instance 'renames' attribute should not be empty for this check."

    all_renamed_values_are_scoped = True
    failed_renames = {}
    for original_key, renamed_value_target in function.renames.items():
        expected_prefix = scope_name + "."
        if not renamed_value_target.startswith(expected_prefix):
            all_renamed_values_are_scoped = False
            failed_renames[original_key] = renamed_value_target

    assert all_renamed_values_are_scoped, (
        f"Not all 'renames' values are correctly prefixed with '{scope_name}.'. "
        f"Incorrect renames: {failed_renames}. All renames: {function.renames}"
    )


def test_workflow_step_creation_check_is_valid_pipeline(
    base_workflow_step_specification,
):
    function = PipeflowFunc.from_dict(base_workflow_step_specification)
    pipefunc.Pipeline([function]).validate()


def test_workflow_creation_with_base_specification_check_initializes_all_steps_correctly(
    base_workflow_specification,
):
    workflow_instance = Workflow.from_dict(base_workflow_specification)

    expected_steps_configs = base_workflow_specification.get("spec", {}).get(
        "steps", []
    )
    assert len(workflow_instance.functions) == len(expected_steps_configs)
    assert len(workflow_instance.functions) == 2
    assert len(workflow_instance.graph.edges) == 2
    assert len(workflow_instance.graph.nodes) == 3

    input_array = {"text_to_be_hashed": [1, 2, 3]}
    assert workflow_instance.map(input_array)


# Define test cases for various spec/steps structural issues
SPEC_STRUCTURE_EDGE_CASES = [
    pytest.param(
        lambda data: data["spec"].__setitem__("steps", []),
        "spec_with_empty_steps_list",
        id="spec_with_empty_steps_list",
    ),
    pytest.param(
        lambda data: data["spec"].pop("steps", None),
        "spec_with_missing_steps_key",
        id="spec_with_missing_steps_key",
    ),
    pytest.param(
        lambda data: data.pop("spec", None),
        "data_with_missing_spec_key",
        id="data_with_missing_spec_key",
    ),
    pytest.param(
        lambda data: data.clear(),  # Entirely empty input data
        "data_is_empty_dict",
        id="data_is_empty_dict",
    ),
]


@pytest.mark.parametrize(
    "config_modifier_func, case_description_id", SPEC_STRUCTURE_EDGE_CASES
)
def test_workflow_creation_on_spec_edge_cases_check_initializes_empty_workflow(
    base_workflow_specification, config_modifier_func, case_description_id
):
    modified_config = copy.deepcopy(base_workflow_specification)

    # Apply the modification to simulate the edge case
    # Some modifiers might completely clear the dict, so no error should occur if "spec" is already gone
    try:
        config_modifier_func(modified_config)
    except KeyError as e:
        # This might happen if a modifier expects a key that was removed by a previous modifier
        # in a more complex setup, but deepcopy should isolate.
        # For these specific modifiers, this is unlikely unless base_workflow_specification is malformed.
        pytest.fail(
            f"Modifier function for '{case_description_id}' caused KeyError: {e} on config: {modified_config}"
        )

    workflow_instance = Workflow.from_dict(modified_config)

    assert isinstance(
        workflow_instance, Workflow
    ), f"Instance type check failed for case: {case_description_id}."
    assert len(workflow_instance.functions) == 0, (
        f"Workflow functions should be empty for case: '{case_description_id}'. "
        f"Found {len(workflow_instance.functions)} functions. Config was: {modified_config}"
    )
