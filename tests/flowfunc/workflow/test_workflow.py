from pathlib import Path

import pytest

from flowfunc.exceptions import PipelineBuildError
from flowfunc.pipeline import PipelineBuilder
from flowfunc.workflow_definition.schema import WorkflowDefinition


@pytest.fixture
def valid_workflow_dict() -> dict:
    """Provides a valid workflow definition as a dictionary."""
    return {
        "apiVersion": "flowfunc.dev/v1beta1",
        "kind": "Pipeline",
        "metadata": {
            "name": "example-workflow",
            "version": "1.0.1",
            "description": "An example workflow for testing purposes.",
        },
        "spec": {
            "steps": [
                {
                    "name": "calculate_md5_hash",
                    "func": "tests.flowfunc.workflow.helpers.md5_hash",
                    "description": "Calculates an MD5 hash of the input data.",
                    "inputs": {"text_to_be_hashed": "hello, world!"},
                    "outputs": "hashed_text",
                    "options": {
                        "mapspec": "text_to_be_hashed[n] -> hashed_text[n]",
                        "profile": True,
                        "debug": True,
                    },
                },
                {
                    "name": "make_bold",
                    "func": "tests.flowfunc.workflow.helpers.markdown_make_bold",
                    "description": "Wraps text to make it bold in Markdown.",
                    "inputs": {"text_to_be_made_bold": "calculate_md5_hash.hashed_text"},
                    "outputs": "bold_string",
                    "options": {
                        "mapspec": "text_to_be_made_bold[n] -> bold_string[n]",
                        "profile": True,
                        "debug": True,
                    },
                },
            ],
            "outputs": {
                "make_bold.bold_string": "bold_string.txt"
            },
        },
    }


@pytest.fixture
def base_workflow_definition(valid_workflow_dict: dict) -> WorkflowDefinition:
    """Provides a validated WorkflowDefinition instance."""
    return WorkflowDefinition.model_validate(valid_workflow_dict)


def test_step_with_parameters_as_defaults(valid_workflow_dict: dict) -> None:
    """Ensures that `parameters` are correctly treated as defaults."""
    step_dict = {
        "name": "param_step",
        "func": "tests.flowfunc.workflow.helpers.md5_hash",
        "parameters": {"text_to_be_hashed": "direct_value_from_params"},
        "options": {"output": "hashed_param_value"},
    }
    valid_workflow_dict["spec"]["steps"] = [step_dict]
    workflow = WorkflowDefinition.model_validate(valid_workflow_dict)
    pf_func = workflow.pipeline.functions[0]

    assert pf_func.defaults["text_to_be_hashed"] == "direct_value_from_params"
    assert pf_func.output == "hashed_param_value"


def test_step_with_various_options(valid_workflow_dict: dict) -> None:
    """Validates that various `PipefuncOptionsModel` fields are correctly applied."""
    step_dict = {
        "name": "options_galore_step",
        "func": "tests.flowfunc.workflow.helpers.md5_hash",
        "inputs": {"text_to_be_hashed": "$global.some_text"},
        "options": {
            "output": "optioned_output",
            "cache": True,
            "internal_shape": "?",
            "resources": {
                "cpus": 2,
                "memory": "4GB",
                "parallelization_mode": "internal",
            },
            "variant": "gpu_optimized",
        },
    }
    valid_workflow_dict["spec"]["steps"] = [step_dict]
    workflow = WorkflowDefinition.model_validate(valid_workflow_dict)
    pf_func = workflow.pipeline.functions[0]

    assert pf_func.cache is True
    assert pf_func.internal_shape == "?"
    assert pf_func.resources is not None
    assert pf_func.resources.cpus == 2
    assert pf_func.resources.memory == "4GB"
    assert pf_func.resources.parallelization_mode == "internal"
    assert pf_func.variant[None] == "gpu_optimized"


def test_step_with_invalid_rename_raises_error(valid_workflow_dict: dict) -> None:
    """Tests that a `PipelineBuildError` is raised for a problematic configuration."""
    valid_workflow_dict["spec"]["steps"][0]["options"]["renames"] = {
        "non_existent_arg": "some_source"
    }
    definition = WorkflowDefinition.model_validate(valid_workflow_dict)

    with pytest.raises(PipelineBuildError, match="cannot be renamed"):
        definition


def test_single_step_can_form_valid_pipeline(valid_workflow_dict: dict) -> None:
    """Ensures a single valid step can form a complete and valid pipeline."""
    valid_workflow_dict["spec"]["steps"] = [valid_workflow_dict["spec"]["steps"][0]]
    definition = WorkflowDefinition.model_validate(valid_workflow_dict)
    workflow = definition

    # The validation is implicitly done during `pipefunc.Pipeline` instantiation.
    # We can explicitly call it to be sure.
    workflow.pipeline.validate()
    assert len(workflow.pipeline.functions) == 1


def test_pipeline_creation_from_definition(
        base_workflow_definition: WorkflowDefinition,
) -> None:
    """Verifies that a `WorkflowDefinition` correctly initializes a `pipefunc.Pipeline`."""
    pipeline = PipelineBuilder().build(base_workflow_definition)

    assert len(pipeline.functions) == 2
    assert len(pipeline.graph.nodes) == 4
    assert len(pipeline.graph.edges) == 2

    try:
        results = pipeline.map({"text_to_be_hashed": ["test1", "test2"]})
        assert "bold_string" in results
        assert len(results["bold_string"].output) == 2
    except Exception as e:
        pytest.fail(f"Pipeline execution failed during test: {e}")


def test_pipeline_with_custom_import_path(tmp_path: Path, monkeypatch) -> None:
    """Tests function resolution using a temporary module path."""
    monkeypatch.syspath_prepend(str(tmp_path))
    helpers_dir = tmp_path / "custom_helpers"
    helpers_dir.mkdir()
    (helpers_dir / "__init__.py").touch()
    (helpers_dir / "my_funcs.py").write_text("def simple_func(input_numbers): return input_numbers * 2")

    workflow_dict = {
        "apiVersion": "flowfunc.dev/v1",
        "kind": "Pipeline",
        "metadata": {"name": "test-custom-import"},
        "spec": {
            "steps": [
                {
                    "name": "doubler",
                    "func": "custom_helpers.my_funcs.simple_func",
                    "inputs": {"input_numbers": "$global.input_numbers"},
                    "outputs": "doubled_num",
                    "options": {
                        "mapspec": "input_numbers[n] -> doubled_num[n]",
                    },
                }
            ],
            "outputs": {
                "doubler.doubled_num": "doubled_num.json"
            },
        },
    }
    workflow = WorkflowDefinition.model_validate(workflow_dict)
    pipeline = PipelineBuilder().build(workflow)
    assert len(pipeline.functions) == 1
    results = pipeline.map({"input_numbers": [1, 2, 3]})
    assert list(results["doubled_num"].output) == [2, 4, 6]


def test_pipeline_config_settings() -> None:
    """Verifies that `pipeline_config` settings are passed to `pipefunc.Pipeline`."""
    workflow_dict = {
        "apiVersion": "flowfunc.dev/v1beta1",
        "kind": "Pipeline",
        "metadata": {"name": "config-test"},
        "spec": {
            "options": {
                "validate_type_annotations": True,
                "lazy": True,
                "debug": True,
            },
            "steps": [
                {
                    "name": "s1",
                    "func": "tests.flowfunc.workflow.helpers.md5_hash",
                    "parameters": {"text_to_be_hashed": "abc"},
                    "outputs": "o1",
                }
            ],
        },
    }
    workflow = WorkflowDefinition.model_validate(workflow_dict)
    assert workflow.spec.options.validate_type_annotations is True
    assert workflow.spec.options.lazy is True
    assert workflow.spec.options.debug is True
