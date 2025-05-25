from pathlib import Path

import pytest


@pytest.fixture
def load_example_path(examples_dir: Path):
    """
    Returns a function to access examples/{name}/workflow.yaml as a dict.

    Usage:
        path = example_path("minimal")
    """

    def _get_path(name: str) -> Path:
        path = examples_dir / name / "workflow.yaml"
        if not path.exists():
            raise FileNotFoundError(f"No workflow.yaml at {path}")
        return path

    return _get_path


@pytest.fixture
def load_example(load_example_path: Path, load_yaml):
    """
    Returns a function to access examples/{name}/workflow.yaml as a dict.

    Usage:
        path = example_path("minimal")
    """

    def _load_path(name: str) -> dict:
        return load_yaml(load_example_path)

    return _load_path
