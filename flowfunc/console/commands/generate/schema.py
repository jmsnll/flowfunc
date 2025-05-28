from __future__ import annotations

import json
from pathlib import Path
from typing import ClassVar

from cleo.helpers import option
from cleo.io.inputs.option import Option

from flowfunc.console.commands.command import Command
from flowfunc.workflow.schema import Workflow


class GenerateSchemaCommand(Command):
    """Generates the JSON schema for flowfunc workflow.yaml files."""

    name = "generate schema"
    description = "Generates and prints the JSON schema for workflow.yaml files."

    options: ClassVar[list[Option]] = [  # type: ignore[assignment]
        option(
            "output",
            "o",
            description="Path to save the JSON schema file. If not provided, prints to stdout.",
            flag=False,
            value_required=False,
        ),
        option(
            "indent",
            "i",
            description="JSON indentation level for the output. Defaults to 2.",
            flag=False,
            value_required=False,
        ),
    ]

    def handle(self) -> int:
        self.line("<info>Generating JSON schema for FlowFuncPipelineModel...</info>")

        try:
            indent_str = self.option("indent")
            if indent_str is not None:
                try:
                    indent = int(indent_str)
                    if indent < 0:
                        self.line_error(
                            "<error>Indent value must be a non-negative integer.</error>"
                        )
                        return 1
                except ValueError:
                    self.line_error(
                        "<error>Invalid indent value. Must be an integer.</error>"
                    )
                    return 1
            else:
                indent = 2  # Default indent

            schema_dict = Workflow.model_json_schema()
            schema_json_str = json.dumps(schema_dict, indent=indent)

            output_path_str = self.option("output")

            if output_path_str:
                output_path = Path(output_path_str)
                try:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(schema_json_str + "\n")
                    self.line(
                        f"<info>JSON schema successfully saved to: {output_path}</info>"
                    )
                except Exception as e:
                    self.line_error(
                        f"<error>Failed to write schema to file '{output_path}': {e}</error>"
                    )
                    return 1
            else:
                self.line(schema_json_str)

            return 0

        except Exception as e:
            self.line_error(
                f"<error>An unexpected error occurred while generating the schema: {e}</error>"
            )
            if self.io.is_debug() or self.io.is_very_verbose():
                import traceback

                self.io.write_error_line(traceback.format_exc())
            return 1
