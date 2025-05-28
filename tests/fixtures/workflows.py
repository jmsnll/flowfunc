from pathlib import Path

import pytest
import yaml

from flowfunc import locations


@pytest.fixture
def load_example():
    """
    Load examples/{example_name}/workflow.yaml as a dict.

    Usage:
        config = load_example_workflow("my_example")
    """

    def _loader(example_name: str) -> dict:
        path = (
            locations.project_root() / Path("examples") / example_name / "workflow.yaml"
        )
        if not path.exists():
            raise FileNotFoundError(f"Workflow not found at {path}")
        with path.open("rb") as f:
            return yaml.safe_load(f)

    return _loader
