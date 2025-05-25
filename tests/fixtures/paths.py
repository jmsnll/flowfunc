import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Returns the path to the root of the repo."""
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def examples_dir(project_root: Path) -> Path:
    """Returns the path to the examples/ directory."""
    return project_root / "examples"


@pytest.fixture
def sys_path_insert():
    def _inner(path: Path):
        sys.path.insert(0, str(path))
        yield
        # sys.path.pop(0)

    return _inner
