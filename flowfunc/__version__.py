from __future__ import annotations

from importlib import metadata

APP_NAME = "flowfunc"
try:
    __version__ = metadata.version(APP_NAME)
except metadata.PackageNotFoundError:
    __version__ = "dev"
