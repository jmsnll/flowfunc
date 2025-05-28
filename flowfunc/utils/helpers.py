from __future__ import annotations

import logging
import os
import re
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

def sanitize_string(data: str) -> str:
    sanitized = "".join(c if c.isalnum() or c in ["-", "_"] else "_" for c in data)
    remove_duplicate_underscores = re.sub(r"_+", "_", sanitized)
    return remove_duplicate_underscores.strip("_")
