# flowfunc/config/__init__.py

"""
FlowFunc Configuration Loading Utilities.

This module provides functions to load FlowFunc-specific configurations,
typically from a `pyproject.toml` file.
"""

from flowfunc.config.loader import ConfigLoaderError
from flowfunc.config.loader import load_flowfunc_toml

__all__ = [
    "ConfigLoaderError",
    "load_flowfunc_toml",
]
