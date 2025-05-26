import os

from flowfunc.__version__ import APP_NAME

KEY_PREFIX = APP_NAME.upper()


def get_prefix_env(key, default=None):
    return os.getenv(f"{KEY_PREFIX}_{key}", default=default)
