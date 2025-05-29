from __future__ import annotations

from pathlib import Path

from platformdirs import user_cache_path
from platformdirs import user_config_path
from platformdirs import user_data_path

from flowfunc.__version__ import APP_NAME
from flowfunc.env import get_prefix_env
from flowfunc.utils import helpers

DEFAULT_CACHE_DIR = user_cache_path(APP_NAME, appauthor=False)
CONFIG_DIR = Path(
    get_prefix_env("CONFIG_DIR")
    or user_config_path(APP_NAME, appauthor=False, roaming=True)
)


def data_dir() -> Path:
    if application_home := get_prefix_env("HOME"):
        return Path(application_home).expanduser()

    return user_data_path(APP_NAME, appauthor=False, roaming=True)


def project_root(markers=None) -> Path:
    """
    Walks up from the current working directory to find the project root,
    identified by the presence of a marker file or directory like pyproject.toml or .git.

    Returns:
        Path to the project root. If no marker is found, returns Path.cwd().
    """
    if markers is None:
        markers = {"pyproject.toml", ".git"}

    current = Path.cwd()

    for parent in [current, *current.parents]:
        if any((parent / marker).exists() for marker in markers):
            return parent

    return current


def workflow_run_dir(toml_config, workflow_name, run_id) -> Path:
    runs_directory_relative = toml_config.get("runs_directory", "runs")
    runs_directory_absolute = project_root() / runs_directory_relative
    sanitized_workflow_name = helpers.sanitize_string(workflow_name)
    return runs_directory_absolute / sanitized_workflow_name / run_id


def ensure(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
