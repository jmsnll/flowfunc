from __future__ import annotations

from pathlib import Path

from platformdirs import user_cache_path, user_config_path, user_data_path

from pipeflow.__version__ import APP_NAME
from pipeflow.env import get_prefix_env

DEFAULT_CACHE_DIR = user_cache_path(APP_NAME, appauthor=False)
CONFIG_DIR = Path(
    get_prefix_env("CONFIG_DIR")
    or user_config_path(APP_NAME, appauthor=False, roaming=True)
)


def data_dir() -> Path:
    if application_home := get_prefix_env("HOME"):
        return Path(application_home).expanduser()

    return user_data_path(APP_NAME, appauthor=False, roaming=True)
