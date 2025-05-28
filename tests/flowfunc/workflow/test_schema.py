from pathlib import Path

import pytest
import yaml

from flowfunc.workflow import loader
from flowfunc.workflow.exceptions import WorkflowLoadError
from flowfunc.workflow.schema import Workflow


@pytest.fixture
def valid_yaml_content() -> dict:
    return {
        "apiVersion": "flowfunc.dev/v1alpha1",
        "kind": "Pipeline",
        "metadata": {"name": "test-pipeline", "version": "1.0"},
        "spec": {"steps": [{"name": "step1", "function": "my.func"}]},
    }


@pytest.fixture
def minimal_valid_yaml_content() -> dict:
    return {
        "apiVersion": "flowfunc.dev/v1alpha1",
        "kind": "Pipeline",
        "metadata": {"name": "minimal-pipeline"},
        "spec": {"steps": [{"name": "minimal_step"}]},
    }


@pytest.fixture
def invalid_schema_yaml_content_missing_required_spec() -> dict:
    return {
        "apiVersion": "flowfunc.dev/v1alpha1",
        "kind": "Pipeline",
        "metadata": {"name": "invalid-pipeline"},
        # 'spec' is missing, which is required by FlowFuncPipelineModel
    }


@pytest.fixture
def invalid_schema_yaml_content_wrong_type() -> dict:
    return {
        "apiVersion": "flowfunc.dev/v1alpha1",
        "kind": "Pipeline",
        "metadata": {"name": 123},  # Name should be string
        "spec": {"steps": [{"name": "step1"}]},
    }


def test_workflow_yaml_init_file_not_found(tmp_path: Path) -> None:
    non_existent_file = tmp_path / "does_not_exist.yaml"
    with pytest.raises(WorkflowLoadError, match=str(non_existent_file)):
        loader.load_from_path(non_existent_file)


def test_workflow_yaml_check_model_valid(load_example_path: pytest.fixture) -> None:
    file_path = load_example_path("image_processing")
    _ = loader.load_from_path(file_path)


def test_model_loads_valid_yaml(
    tmp_path: Path, minimal_valid_yaml_content: dict
) -> None:
    file_path = tmp_path / "valid_workflow.yaml"
    with open(file_path, "w") as f:
        yaml.dump(minimal_valid_yaml_content, f)

    model = loader.load_from_path(file_path)

    assert isinstance(model, Workflow)
    assert model.metadata.name == "minimal-pipeline"
    assert len(model.spec.steps) == 1
    assert model.spec.steps[0].name == "minimal_step"


def test_model_caching(tmp_path: Path, minimal_valid_yaml_content: dict) -> None:
    file_path = tmp_path / "cache_test.yaml"
    with open(file_path, "w") as f:
        yaml.dump(minimal_valid_yaml_content, f)

    model1 = model2 = loader.load_from_path(file_path)

    assert model1 is model2


def test_model_empty_file_raises_workflow_load_error(tmp_path: Path) -> None:
    file_path = tmp_path / "empty.yaml"
    file_path.write_text("")

    with pytest.raises(
        WorkflowLoadError, match="empty or contains no parsable content"
    ):
        _ = loader.load_from_path(file_path)


def test_model_malformed_yaml_raises_workflow_load_error(tmp_path: Path) -> None:
    file_path = tmp_path / "malformed.yaml"
    file_path.write_text("apiVersion: v1\nkind: Pipeline\n  bad-indent: true")

    with pytest.raises(WorkflowLoadError, match="is not a valid YAML file"):
        _ = loader.load_from_path(file_path)


def test_model_not_dict_top_level_raises_workflow_load_error(tmp_path: Path) -> None:
    file_path = tmp_path / "list_top_level.yaml"
    yaml_list_content = [{"item": "value"}]
    with open(file_path, "w") as f:
        yaml.dump(yaml_list_content, f)

    with pytest.raises(WorkflowLoadError, match="Top level must be a mapping"):
        _ = loader.load_from_path(file_path)


def test_model_schema_validation_error_missing_required(
    tmp_path: Path, invalid_schema_yaml_content_missing_required_spec: dict
) -> None:
    file_path = tmp_path / "invalid_schema_missing.yaml"
    with open(file_path, "w") as f:
        yaml.dump(invalid_schema_yaml_content_missing_required_spec, f)

    with pytest.raises(
        WorkflowLoadError, match="Workflow YAML schema validation failed"
    ):
        _ = loader.load_from_path(file_path)


def test_model_schema_validation_error_wrong_type(
    tmp_path: Path, invalid_schema_yaml_content_wrong_type: dict
) -> None:
    file_path = tmp_path / "invalid_schema_type.yaml"
    with open(file_path, "w") as f:
        yaml.dump(invalid_schema_yaml_content_wrong_type, f)

    with pytest.raises(
        WorkflowLoadError, match="Workflow YAML schema validation failed"
    ):
        _ = loader.load_from_path(file_path)
