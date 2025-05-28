import pipefunc
import pytest

from flowfunc.workflow import function
from flowfunc.workflow import pipeline
from flowfunc.workflow.exceptions import PipelineBuildError
from flowfunc.workflow.exceptions import WorkflowSchemaValidationError
from flowfunc.workflow.schema import Pipeline
from flowfunc.workflow.schema import Step
from flowfunc.workflow.schema import Workflow


@pytest.fixture
def valid_workflow_dict() -> dict:
    """A valid workflow definition as a dictionary."""
    return {
        "apiVersion": "flowfunc.dev/v1beta1",
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
                    "name": "calculate_md5_hash",
                    "function": "tests.flowfunc.workflow.helpers.md5_hash",
                    "description": "Calculates an MD5 hash of input data.",
                    "inputs": {"text_to_be_hashed": "hello, world!"},
                    "options": {
                        "output_name": "hashed_text",
                        "mapspec": "text_to_be_hashed[n] -> hashed_text[n]",
                        "defaults": {
                            "text_to_be_hashed": "default input if not wired",
                        },
                        "profile": True,
                        "debug": True,
                    },
                },
                {
                    "name": "make_bold",
                    "function": "tests.flowfunc.workflow.helpers.markdown_make_bold",
                    "description": "Wraps the input text to make it bold.",
                    "options": {
                        "output_name": "bold_string",
                        "mapspec": "hashed_text[n] -> bold_string[n]",
                        "profile": True,
                        "debug": True,
                        "renames": {"text_to_be_made_bold": "hashed_text"},
                    },
                },
            ],
            "pipeline_outputs": ["make_bold.bold_string"],
        },
    }


@pytest.fixture
def base_pipeline_model(valid_workflow_dict: dict) -> Workflow:
    """Provides a validated FlowFuncPipelineModel instance."""
    try:
        return Workflow.model_validate(valid_workflow_dict)
    except WorkflowSchemaValidationError as e:
        pytest.fail(f"Test fixture setup failed: Pydantic validation error: {e}")
        raise


@pytest.fixture
def first_step_model(base_pipeline_model: Workflow) -> Step:
    """Provides the first StepModel from the base_pipeline_model."""
    return base_pipeline_model.spec.steps[0]


@pytest.fixture
def pipefunc_function_from_first_step(first_step_model: Step) -> pipefunc.PipeFunc:
    """Provides a pipefunc.Function created from the first_step_model."""
    return function.from_model(first_step_model)


def test_step_with_only_parameters(valid_workflow_dict) -> None:
    """Test a step that gets its input from parameters, not wiring."""
    step_dict = {
        "name": "param_step",
        "function": "tests.flowfunc.workflow.helpers.md5_hash",
        "parameters": {"text_to_be_hashed": "direct_value_from_params"},
        "options": {"output_name": "hashed_param_value"},
    }
    step_model = Step.model_validate(step_dict)
    pf_func = function.from_model(step_model)

    assert pf_func.defaults["text_to_be_hashed"] == "direct_value_from_params"
    assert pf_func.output_name == "hashed_param_value"


def test_step_with_various_options(valid_workflow_dict) -> None:
    """Test a step with more PipefuncOptionsModel fields."""
    step_dict = {
        "name": "options_galore_step",
        "function": "tests.flowfunc.workflow.helpers.md5_hash",
        "inputs": {"text_to_be_hashed": "$global.some_text"},
        "options": {
            "output_name": "optioned_output",
            "cache": True,
            "internal_shape": "?",
            # "post_execution_hook": "some.hook.function", # Would require mocking/setup
            "resources": {
                "cpus": 2,
                "memory": "4GB",
                "parallelization_mode": "internal",  # Literal["internal"]
            },
            "variant": "gpu_optimized",
        },
    }
    step_model = Step.model_validate(step_dict)
    pf_func = function.from_model(step_model)

    assert pf_func.cache is True
    assert pf_func.internal_shape == "?"
    assert pf_func.resources is not None
    assert pf_func.resources.cpus == 2
    assert pf_func.resources.memory == "4GB"
    assert pf_func.resources.parallelization_mode == "internal"
    assert pf_func.variant[None] == "gpu_optimized"


def test_step_creation_with_problematic_config_raises_error(
    first_step_model: Step,
) -> None:
    modified_step_model = first_step_model.model_copy(deep=True)
    modified_step_model.options.renames = {"non_existent_arg": "some_source"}

    with pytest.raises(PipelineBuildError):  # Or ValueError, TypeError, etc.
        function.from_model(modified_step_model)


def test_step_creation_with_scope_prefixes_renamed_outputs(  # Or inputs, depending on what 'scope' does
    first_step_model: Step,
) -> None:
    modified_step_model = first_step_model.model_copy(deep=True)
    scope_name = "example_scope"
    modified_step_model.options.scope = scope_name

    pf_function = function.from_model(modified_step_model)

    assert pf_function.renames
    for internal_arg, pipeline_name in pf_function.renames.items():
        assert pipeline_name.startswith(scope_name + "."), (
            f"Rename '{pipeline_name}' for arg '{internal_arg}' not prefixed by scope '{scope_name}'"
        )


def test_single_step_function_forms_valid_pipeline(
    pipefunc_function_from_first_step: pipefunc.PipeFunc,
) -> None:
    pipeline_instance = pipefunc.Pipeline([pipefunc_function_from_first_step])
    pipeline_instance.validate()


def test_pipeline_creation_from_model_initializes_correctly(
    base_pipeline_model: Workflow,
    valid_workflow_dict: dict,
) -> None:
    pipeline_instance = pipeline.from_model(base_pipeline_model.spec)

    expected_num_steps = len(valid_workflow_dict["spec"]["steps"])
    assert len(pipeline_instance.functions) == expected_num_steps
    assert len(pipeline_instance.graph.nodes) == 3
    assert len(pipeline_instance.graph.edges) == 2

    try:
        results = pipeline_instance.map({"text_to_be_hashed": ["test1", "test2"]})
        assert len(results.get("bold_string").output) == 2
    except Exception as e:
        pytest.fail(f"Pipeline execution failed in test: {e}")


def test_pipeline_with_new_import_path(tmp_path, monkeypatch) -> None:
    """Test pipeline using default_module for function resolution."""
    monkeypatch.syspath_prepend(str(tmp_path))
    helpers_dir = tmp_path / "custom_helpers"
    helpers_dir.mkdir()
    with open(helpers_dir / "__init__.py", "w") as f:
        f.write("")
    with open(helpers_dir / "my_funcs.py", "w") as f:
        f.write("def simple_func(x): return x * 2\n")

    pipeline_dict = {
        "apiVersion": "flowfunc.dev/v1",
        "kind": "Pipeline",
        "metadata": {"name": "test-default-module"},
        "spec": {
            "steps": [
                {
                    "name": "doubler",
                    "function": "custom_helpers.my_funcs.simple_func",
                    "options": {
                        "output_name": "doubled_num",
                        "mapspec": "x[n] -> doubled_num[n]",
                    },
                }
            ],
            "pipeline_outputs": ["doubled_num"],
        },
    }
    pipeline_model = Workflow.model_validate(pipeline_dict)
    pf_pipeline = pipeline.from_model(pipeline_model.spec)

    assert len(pf_pipeline.functions) == 1
    results = pf_pipeline.map({"x": [1, 2, 3]})
    assert list(results["doubled_num"].output) == [2, 4, 6]


def test_pipeline_with_various_pipeline_configs() -> None:
    """Test various pipeline_config settings."""
    spec_dict = {
        "pipeline_config": {
            "validate_type_annotations": True,
            "cache_type": "disk",
            "cache_kwargs": {"cache_dir": "/tmp/cache"},
            "lazy": True,
            "debug": True,
        },
        "steps": [
            {
                "name": "s1",
                "function": "tests.flowfunc.workflow.helpers.md5_hash",
                "parameters": {"text_to_be_hashed": "abc"},
                "options": {"output_name": "o1"},
            }
        ],
    }
    spec_model = Pipeline.model_validate(spec_dict)
    pf_pipeline = pipeline.from_model(spec_model)

    assert pf_pipeline.validate_type_annotations is True
    assert pf_pipeline._cache_type == "disk"
    assert pf_pipeline._cache_kwargs["cache_dir"] == "/tmp/cache"
    assert pf_pipeline.lazy is True
    assert pf_pipeline.debug is True
