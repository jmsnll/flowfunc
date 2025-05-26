from __future__ import annotations

import textwrap
from pathlib import Path
from typing import Any

from cleo.helpers import argument
from cleo.helpers import option

from flowfunc.console.commands.command import Command


class NewCommand(Command):
    """Creates a new workflow bundle (directory + workflow.yaml file)."""

    name = "new"
    description = "Creates a new workflow bundle (directory + workflow.yaml file)."

    arguments = [
        argument(
            "workflow_bundle_name",
            "Name of the new workflow bundle.",
        )
    ]

    options = [
        option("overwrite", "o", "Overwrite workflow.yaml if it exists.", flag=True),
        option("force-dir", None, "Allow existing bundle directory.", flag=True),
    ]

    def get_project_config_value(self, key: str, default: Any = None) -> Any:
        try:
            return (
                self.pyproject.data.get("tool", {})
                .get("flowfunc", {})
                .get(key, default)
            )
        except Exception:
            return default

    def handle(self) -> int:
        name = self.argument("workflow_bundle_name")

        if not name or not name.replace("_", "").replace("-", "").isalnum():
            self.line_error(
                f"<error>Invalid bundle name '{name}'. Use alphanumerics, -, _.</error>"
            )
            return 1

        # Resolve base directories
        root = Path.cwd()
        workflows_base = root / self.get_project_config_value(
            "workflows_directory", "workflows"
        )
        bundle_path = workflows_base / name
        workflow_path = bundle_path / "workflow.yaml"

        # Ensure workflows directory exists
        if not workflows_base.exists():
            if workflows_base.name == "workflows":
                try:
                    workflows_base.mkdir(parents=True)
                    self.line(
                        f"<comment>Created workflows directory: {workflows_base.relative_to(root)}</comment>"
                    )
                except Exception as e:
                    self.line_error(
                        f"<error>Could not create workflows directory: {e}</error>"
                    )
                    return 1
            else:
                self.line_error(
                    f"<error>Workflows directory '{workflows_base}' does not exist.</error>"
                )
                return 1

        if workflows_base.is_file():
            self.line_error(
                f"<error>{workflows_base} exists but is not a directory.</error>"
            )
            return 1

        # Handle bundle directory
        if bundle_path.exists():
            if not bundle_path.is_dir():
                self.line_error(
                    f"<error>{bundle_path.relative_to(root)} exists but is not a directory.</error>"
                )
                return 1
            if not self.option("force-dir"):
                self.line(
                    "<comment>Directory exists. Use --force-dir to continue.</comment>"
                )
                return 1
            self.line(
                f"<warning>Using existing directory: {bundle_path.relative_to(root)}</warning>"
            )
        else:
            try:
                bundle_path.mkdir(parents=True)
                self.line(
                    f"<info>Created bundle directory: {bundle_path.relative_to(root)}</info>"
                )
            except Exception as e:
                self.line_error(
                    f"<error>Could not create bundle directory: {e}</error>"
                )
                return 1

        # Handle workflow.yaml
        if workflow_path.exists() and not self.option("overwrite"):
            self.line_error(
                f"<error>{workflow_path.relative_to(root)} already exists. Use --overwrite to replace.</error>"
            )
            return 1

        self.line(f"<info>Creating: {workflow_path.relative_to(root)}</info>")

        # Gather metadata
        default_meta_name = name.replace("_", "-")
        meta_name = self.ask(
            f"Workflow name (<comment>{default_meta_name}</comment>):",
            default=default_meta_name,
        )
        meta_version = self.ask(
            "Workflow version (<comment>0.1.0</comment>):", default="0.1.0"
        )
        meta_description = self.ask("Workflow description (optional):", default="")

        default_src = self.get_project_config_value("source_directory", "src")
        default_module = f"{default_src.replace('/', '.')}.{name}_functions"
        spec_module = self.ask(
            f"Default Python module (optional, e.g. <comment>{default_module}</comment>):",
            default=None,
        )

        # Build workflow.yaml content
        step = {
            "name": "example_step",
            "description": "A placeholder step. Implement its function and adjust inputs/outputs.",
            "inputs": {"input_arg1": "$global.example_global_input"},
            "options": {"output_name": "example_output"},
        }

        if not spec_module:
            step["function"] = f"your_custom_module.{name}_example_step_function"

        workflow_content = {
            "apiVersion": "flowfunc.dev/v1beta1",
            "kind": "Pipeline",
            "metadata": {
                "name": meta_name,
                "version": meta_version,
            },
            "spec": {
                "global_inputs": {
                    "example_global_input": {
                        "description": "An example global input for this workflow.",
                        "type": "string",
                        "default": "hello from bundle",
                    }
                },
                "steps": [step],
                "pipeline_outputs": ["example_step.example_output"],
            },
        }

        if meta_description:
            workflow_content["metadata"]["description"] = meta_description
        if spec_module:
            workflow_content["spec"]["default_module"] = spec_module

        import yaml

        with workflow_path.open("w", encoding="utf-8") as f:
            yaml.dump(workflow_content, f, sort_keys=False)

        # Optional README.md
        readme_path = bundle_path / "README.md"
        readme_text = textwrap.dedent(f"""\
            # Workflow: {meta_name}

            Version: {meta_version}

            {meta_description or "This workflow bundle contains a flowfunc pipeline."}

            ## How to Run

            ```bash
            uvx flowfunc run {workflows_base.name}/{name} --inputs '{{"example_global_input": "custom_value"}}'
            ```
        """)
        readme_path.write_text(readme_text.strip(), encoding="utf-8")

        self.line(f"<info>Created: {workflow_path.relative_to(root)}</info>")
        self.line(f"<info>Created: {readme_path.relative_to(root)}</info>")
        self.line("<success>Workflow bundle initialized successfully.</success>")
        return 0
