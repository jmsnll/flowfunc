from __future__ import annotations

import logging
import sys
import textwrap
from pathlib import Path
from typing import TYPE_CHECKING
from typing import ClassVar

# MODIFIED: Import PIPEFUNC_FILTER
from flowfunc.console.logging.filters import FLOWFUNC_FILTER
from flowfunc.console.logging.filters import PIPEFUNC_FILTER

if TYPE_CHECKING:
    from logging import LogRecord


class IOFormatter(logging.Formatter):
    _colors: ClassVar[dict[str, str]] = {
        "error": "fg=red",
        "warning": "fg=yellow",
        "debug": "debug",  # cleo handles 'debug' tag specifically for verbosity levels
        "info": "fg=blue",
        "critical": "fg=red;options=bold",  # MODIFIED: Added critical
    }

    # MODIFIED: Added __init__ to set a default format with timestamp and level
    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: str = "%",
        validate: bool = True,
    ) -> None:
        if fmt is None:
            fmt = "[%(asctime)s][%(levelname)s] %(message)s"
        # In Python 3.8+ style and validate were added.
        # For broader compatibility, especially if targeting <3.8,
        # you might need to adjust the super().__init__ call.
        # Assuming Python 3.8+ for keyword arguments:
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)

    def format(self, record: LogRecord) -> str:
        # MODIFIED: Color the original message part (record.msg)
        # This will be used by super().format() if %(message)s is in the fmt string.
        original_msg = record.msg  # Keep a copy
        level = record.levelname.lower()

        if level in self._colors:
            # Apply color tags to the message itself
            record.msg = f"<{self._colors[level]}>{original_msg}</>"

        # Let the base class formatter handle the full formatting
        # based on the `fmt` string (which now includes timestamp and level by default).
        # This will also handle exception information if present.
        formatted = super().format(record)

        # Restore original message on the record in case other handlers need it
        record.msg = original_msg

        # MODIFIED: Prefixing logic
        # The `formatted` string now contains the timestamp, level, and (colored) message.
        # The entire line will be indented with the prefix.

        # Determine prefix based on logger name
        if FLOWFUNC_FILTER.filter(record):
            # "flowfunc" logs - styled to stand out (e.g., yellow prefix)
            # Comment corrected: Highlights logs from 'flowfunc'
            formatted = textwrap.indent(
                formatted, f"[<fg=yellow>{_log_prefix(record)}</>]", lambda line: True
            )
        elif PIPEFUNC_FILTER.filter(record):  # MODIFIED: Added PIPEFUNC handling
            # "pipefunc" logs - styled distinctly (e.g., cyan prefix)
            formatted = textwrap.indent(
                formatted, f"[<fg=cyan>{_log_prefix(record)}</>]", lambda line: True
            )
        else:
            # Logs from other sources (e.g., dependencies) - standard prefix
            # Comment corrected: Standard prefix for other logs
            formatted = textwrap.indent(
                formatted, f"[{_log_prefix(record)}] ", lambda line: True
            )

        return formatted


def _log_prefix(record: LogRecord) -> str:
    """Return a logging prefix based on file path and logger name."""
    prefix = _path_to_package(Path(record.pathname))
    if prefix == "flowfunc":
        return record.name
    if record.name != "root":
        prefix = f"{prefix}:{record.name}"
    return prefix


def _path_to_package(path: Path) -> str | None:
    """Return main package name from the LogRecord.pathname."""
    prefix: Path | None = None
    for syspath_str in sys.path:
        # Ensure syspath_str is a valid path and handle potential errors
        try:
            syspath = Path(syspath_str)
            if not syspath.is_dir():  # Optimization: only check directories
                continue
        except (
            TypeError
        ):  # Handle cases where sys.path might contain non-path-like objects
            continue

        # Check if syspath is a parent of the record's path
        # and if it's a more specific match than a previously found prefix.
        if (
            path.is_absolute() and syspath.is_absolute()
        ):  # Ensure both are absolute for correct comparison
            try:
                if syspath in path.parents:
                    if prefix is None or syspath in prefix.parents:
                        prefix = syspath
            except RuntimeError:  # Can happen with deeply nested paths on some OS
                continue
        elif not path.is_absolute() and not syspath.is_absolute():
            # Relative path handling can be complex; for now, assume absolute or simplify
            # This part of logic might need refinement based on how paths are expected
            pass  # Or implement relative path comparison if necessary

    if not prefix:
        return None

    try:
        relative_path = path.relative_to(prefix)
        return relative_path.parts[0]  # main package name
    except (
        ValueError
    ):  # path is not under prefix, should ideally not happen if logic above is correct
        return None
