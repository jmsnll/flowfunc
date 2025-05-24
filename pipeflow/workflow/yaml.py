from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class WorkflowYAML:
    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(self._path.as_posix())
        self._data: dict[str, Any] | None = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def data(self) -> dict[str, Any]:
        if self._data is None:
            if not self.path.exists():
                self._data = {}
            else:
                try:
                    with self.path.open("rb") as f:
                        self._data = yaml.load(f, Loader=yaml.SafeLoader)
                except yaml.YAMLError as e:
                    from pipeflow.workflow.exceptions import WorkflowLoadError

                    msg = (
                        f"{self._path.as_posix()} is not a valid YAML file.\n"
                        f"{e.__class__.__name__}: {e}"
                    )

                    if str(e).startswith("Cannot overwrite a value"):
                        msg += "\nThis is often caused by a duplicate entry."

                    raise WorkflowLoadError(msg) from e

        return self._data

    @property
    def metadata(self) -> dict[str, Any]:
        return self._load_key("metadata")

    @property
    def spec(self) -> dict[str, Any]:
        return self._load_key("spec")

    def _load_key(self, key: str) -> dict[str, Any]:
        try:
            return self.data[key]
        except KeyError as e:
            from pipeflow.workflow.exceptions import WorkflowLoadError

            raise WorkflowLoadError(
                f"Key '{key}' not found in {self._path.as_posix()}"
            ) from e
