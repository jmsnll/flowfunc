from pathlib import Path

import pytest
import yaml

from flowfunc import locations
from flowfunc.workflow import loader
from flowfunc.workflow.schema import WorkflowDefinition


@pytest.fixture
def write_yaml(temp_path):
    """
    Write a dict as YAML to a file in the temp_path.

    Usage:
        path = write_yaml("workflow.yaml", {"foo": "bar"})
    """

    def _writer(name: str, data: dict):
        encoded = yaml.dump(data).encode("utf-8")
        temp_path.write(name, encoded)
        return temp_path.getpath(name)

    return _writer


@pytest.fixture
def read_yaml(temp_path):
    """
    Read a YAML file into a dict.

    If `path` is a relative path (str or Path), reads it from the temp_path.
    If `path` is an absolute path, reads it directly.
    """

    def _reader(path: str | Path):
        if isinstance(path, str) or (isinstance(path, Path) and not path.is_absolute()):
            # Resolve relative paths inside tempdir
            full_path = Path(temp_path.path) / path
        else:
            full_path = Path(path)
        with full_path.open("rb") as f:
            return yaml.safe_load(f)

    return _reader


@pytest.fixture
def load_example_workflow():
    """
    Load examples/{example_name}/workflow.yaml as a dict.

    Usage:
        config = load_example_workflow("my_example")
    """

    def _loader(example_name: str) -> WorkflowDefinition:
        path = (
            locations.project_root() / Path("examples") / example_name / "workflow.yaml"
        )
        if not path.exists():
            raise FileNotFoundError(f"Workflow not found at {path}")
        with path.open("rb"):
            return loader.from_path(path)
            # return yaml.safe_load(f)

    return _loader
