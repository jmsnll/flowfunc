from __future__ import annotations

from pathlib import Path

import tomlkit

from pipeflow.console.commands.command import Command
from pipeflow.pyproject.toml import PyProjectTOML


class InitCommand(Command):
    """Initializes a new Pipeflow project with a recommended directory structure and configuration."""

    name = "init"
    description = "Initializes a new Pipeflow project (pyproject.toml, src, workflows)."

    def handle(self) -> int:
        self.line("<info>Initializing Pipeflow project...</info>")

        project_dir = Path.cwd()
        pyproject_path = project_dir / "pyproject.toml"
        workflows_dir = project_dir / "workflows"
        src_dir = project_dir / "src"

        if not pyproject_path.exists():
            self.line_error(
                "<error>No pyproject.toml found in directory. "
                "Please initialize it with your preferred package manager first.</error>"
            )
            return 1

        try:
            project_toml = PyProjectTOML(pyproject_path)
            tool_table = project_toml.data.get("tool") or tomlkit.table()
            pipeflow_table = tool_table.get("pipeflow") or tomlkit.table()

            updated = False
            if "source_directory" not in pipeflow_table:
                pipeflow_table["source_directory"] = src_dir.name
                updated = True
            if "workflows_directory" not in pipeflow_table:
                pipeflow_table["workflows_directory"] = workflows_dir.name
                updated = True

            if updated:
                tool_table["pipeflow"] = pipeflow_table
                project_toml.data["tool"] = tool_table
                project_toml.save()
                self.line("<info>Updated pyproject.toml with [tool.pipeflow] settings.</info>")
            else:
                self.line("<comment>[tool.pipeflow] section already exists with defaults. Skipping update.</comment>")

        except Exception as e:
            self.line_error(f"<error>Failed to update pyproject.toml: {e}</error>")
            return 1

        for directory in [workflows_dir, src_dir]:
            if not directory.exists():
                directory.mkdir(parents=True)
                self.line(f"<info>Created directory: {directory.name}/</info>")

        self.line("<info>Pipeflow project initialized successfully!</info>")
        self.line(f"  - Manage your Python functions in the <comment>{src_dir.name}</comment> directory.")
        self.line(f"  - Define your workflows in the <comment>{workflows_dir.name}</comment> directory.")
        return 0