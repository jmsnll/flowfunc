from __future__ import annotations

import tomllib
from contextlib import suppress
from typing import TYPE_CHECKING
from typing import Any

from tomlkit.toml_document import TOMLDocument
from tomlkit.toml_file import TOMLFile

from flowfunc import locations
from flowfunc.toml import TOMLFile as internaltomlfile

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
                    from flowfunc.pyproject.exceptions import PyProjectError

                    msg = (
                        f"{self._path.as_posix()} is not a valid TOML file.\n"
                        f"{e.__class__.__name__}: {e}"
                    )

                    if str(e).startswith("Cannot overwrite a value"):
                        msg += "\nThis is often caused by a duplicate entry."

                    raise PyProjectError(msg) from e

        return self._data

    @property
    def flowfunc_config(self) -> dict[str, Any]:
        try:
            tool = self.data["tool"]
            assert isinstance(tool, dict)
            config = tool["flowfunc"]
            assert isinstance(config, dict)
            return config
        except KeyError as e:
            from flowfunc.pyproject.exceptions import PyProjectError

            raise PyProjectError(
                f"[tool.flowfunc] section not found in {self._path.as_posix()}"
            ) from e

    def is_flowfunc_project(self) -> bool:
        from flowfunc.pyproject.exceptions import PyProjectError

        if self.path.exists():
            with suppress(PyProjectError):
                _ = self.flowfunc_config
                return True

            # Even if there is no [tool.flowfunc] section, a project can still be a
            # valid FlowFunc project if there is a name and a version in [project]
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
        self._toml_file = internaltomlfile(path=path)
        self._toml_document: TOMLDocument | None = None

    @property
    def file(self) -> internaltomlfile:
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


def load_pyproject(path: Path = None) -> TOMLFile:
    path = path or locations.project_root() / "pyproject.toml"
    if not path.exists() or not path.is_file():
        raise FileNotFoundError("Failed to load pyproject.toml")
    return TOMLFile(path)


def load_flowfunc_toml(path: Path = None) -> TOMLDocument:
    pyproject_file = load_pyproject(path)
    pyproject_document = pyproject_file.read()
    return pyproject_document.get("tools", {}).get("flowfunc", {})
