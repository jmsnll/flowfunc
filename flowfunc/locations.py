from __future__ import annotations

from pathlib import Path


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


def ensure(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
