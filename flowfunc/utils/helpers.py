from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@contextmanager
def directory(path: Path):
    cwd = Path.cwd()
    try:
        os.chdir(path)
        yield path
    finally:
        os.chdir(cwd)


def merge_dicts(d1: dict[str, Any], d2: dict[str, Any]) -> None:
    for k in d2:
        if k in d1 and isinstance(d1[k], dict) and isinstance(d2[k], Mapping):
            merge_dicts(d1[k], d2[k])
        else:
            d1[k] = d2[k]


def ensure_path(path: str | Path, is_directory: bool = False) -> Path:
    if isinstance(path, str):
        path = Path(path)

    if path.exists() and path.is_dir() == is_directory:
        return path

    raise ValueError(
        f"Specified path '{path}' is not a valid {'directory' if is_directory else 'file'}."
    )


def pluralize(count: int, word: str = "") -> str:
    if count == 1:
        return word
    return word + "s"
