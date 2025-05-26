from __future__ import annotations

from pathlib import Path

import tomlkit

from flowfunc.console.commands.command import Command
from flowfunc.pyproject.toml import PyProjectTOML


class InitCommand(Command):
    """Initializes a FlowFunc project by configuring pyproject.toml and creating standard directories."""

    name = "init"
    description = (
        "Initializes a FlowFunc project: adds [tool.flowfunc] config to pyproject.toml, "
        "and creates src/, workflows/, and runs/ directories."
    )

    DEFAULTS = {
        "source_directory": "src",
        "workflows_directory": "workflows",
        "runs_directory": "runs",
    }

    def handle(self) -> int:
        self.line("<info>Initializing FlowFunc project...</info>")

        project_dir = Path.cwd()
        pyproject_path = project_dir / "pyproject.toml"

        pyproject = self._load_pyproject(pyproject_path)
        if pyproject is None:
            return 1

        config = self._update_config(pyproject)
        directories = self._resolve_directories(project_dir, config)

        self._create_directories(directories, project_dir)
        self._summarize(directories, project_dir)

        return 0

    def _load_pyproject(self, path: Path) -> PyProjectTOML | None:
        if not path.exists():
            self.line_error(f"<error>No pyproject.toml found at {path}</error>")
            self.line(
                "<comment>Run 'poetry init' or 'uv init' to create one first.</comment>"
            )
            return None
        try:
            return PyProjectTOML(path)
        except Exception as e:
            self.line_error(f"<error>Failed to parse pyproject.toml: {e}</error>")
            return None

    def _update_config(self, pyproject: PyProjectTOML) -> dict[str, str]:
        tool_table = pyproject.data.get("tool") or tomlkit.table()
        flowfunc_table = tool_table.get("flowfunc") or tomlkit.table()

        updated = False
        for key, default in self.DEFAULTS.items():
            if not flowfunc_table.get(key):
                flowfunc_table[key] = default
                updated = True

        if updated:
            tool_table["flowfunc"] = flowfunc_table
            pyproject.data["tool"] = tool_table
            try:
                pyproject.save()
                self.line(
                    "<info>Updated pyproject.toml with [tool.flowfunc] defaults.</info>"
                )
            except Exception as e:
                self.line_error(f"<error>Failed to save pyproject.toml: {e}</error>")
        else:
            self.line(
                "<comment>[tool.flowfunc] already contains required config. Skipping update.</comment>"
            )

        return {key: flowfunc_table[key] for key in self.DEFAULTS}

    def _resolve_directories(
        self, project_dir: Path, config: dict[str, str]
    ) -> dict[str, Path]:
        return {
            "Source": project_dir / config["source_directory"],
            "Workflows": project_dir / config["workflows_directory"],
            "Runs": project_dir / config["runs_directory"],
        }

    def _create_directories(
        self, directories: dict[str, Path], project_dir: Path
    ) -> None:
        for label, path in directories.items():
            try:
                if not path.exists():
                    path.mkdir(parents=True, exist_ok=True)
                    self.line(
                        f"<info>Created {label} directory: {path.relative_to(project_dir)}/</info>"
                    )
                elif not path.is_dir():
                    self.line_error(
                        f"<error>Path '{path.relative_to(project_dir)}' exists but is not a directory.</error>"
                    )
            except Exception as e:
                self.line_error(
                    f"<error>Failed to create {label} directory at '{path.relative_to(project_dir)}': {e}</error>"
                )

    def _summarize(self, directories: dict[str, Path], project_dir: Path) -> None:
        self.line("\n<success>FlowFunc project initialized successfully!</success>")
        for label, path in directories.items():
            self.line(
                f"  - {label} directory: <comment>{path.relative_to(project_dir)}/</comment>"
            )
        self.line(
            "  - Create a workflow with: <fg=cyan>flowfunc new <your_bundle_name></>"
        )
