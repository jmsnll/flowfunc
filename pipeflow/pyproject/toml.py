from __future__ import annotations

import tomllib
from contextlib import suppress
from typing import TYPE_CHECKING
from typing import Any

from tomlkit.toml_document import TOMLDocument

from pipeflow.toml import TOMLFile

if TYPE_CHECKING:
    from pathlib import Path


class BasePyProjectTOML:
    def __init__(self, path: Path) -> None:
        self._path = path
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
                        self._data = tomllib.load(f)
                except tomllib.TOMLDecodeError as e:
                    from pipeflow.pyproject.exceptions import PyProjectError

                    msg = (
                        f"{self._path.as_posix()} is not a valid TOML file.\n"
                        f"{e.__class__.__name__}: {e}"
                    )

                    if str(e).startswith("Cannot overwrite a value"):
                        msg += "\nThis is often caused by a duplicate entry."

                    raise PyProjectError(msg) from e

        return self._data

    @property
    def pipeflow_config(self) -> dict[str, Any]:
        try:
            tool = self.data["tool"]
            assert isinstance(tool, dict)
            config = tool["pipeflow"]
            assert isinstance(config, dict)
            return config
        except KeyError as e:
            from pipeflow.pyproject.exceptions import PyProjectError

            raise PyProjectError(
                f"[tool.pipeflow] section not found in {self._path.as_posix()}"
            ) from e

    def is_pipeflow_project(self) -> bool:
        from pipeflow.pyproject.exceptions import PyProjectError

        if self.path.exists():
            with suppress(PyProjectError):
                _ = self.pipeflow_config
                return True

            # Even if there is no [tool.pipeflow] section, a project can still be a
            # valid Pipeflow project if there is a name and a version in [project]
            # and there are no dynamic fields.
            with suppress(KeyError):
                project = self.data["project"]
                if (
                    project["name"]
                    and project["version"]
                    and not project.get("dynamic")
                ):
                    return True

        return False


class PyProjectTOML(BasePyProjectTOML):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self._toml_file = TOMLFile(path=path)
        self._toml_document: TOMLDocument | None = None

    @property
    def file(self) -> TOMLFile:
        return self._toml_file

    @property
    def data(self) -> TOMLDocument:
        if self._toml_document is None:
            if not self.file.exists():
                self._toml_document = TOMLDocument()
            else:
                self._toml_document = self.file.read()

        return self._toml_document

    def save(self) -> None:
        self.file.write(data=self.data)

    def reload(self) -> None:
        self._toml_document = None
