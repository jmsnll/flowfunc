from __future__ import annotations

import json

from pipeflow.console.commands.command import WorkflowCommand
from pipeflow.workflow.exceptions import PipelineBuildError  # From existing files
from pipeflow.workflow.exceptions import WorkflowLoadError


class ValidateCommand(WorkflowCommand):
    name = "validate"
    description = "Validates a workflow definition, including schema and pipefunc pipeline integrity."

    def handle(self) -> int:
        self.line(f"<info>Validating workflow: {self.argument('workflow')}</info>")

        try:
            if self.workflow:
                self.line("<comment>Schema and basic structure: OK</comment>")

            self.line(
                "<info>Performing detailed pipefunc pipeline validation...</info>"
            )
            self.workflow.validate()
            self.line("<comment>Pipefunc pipeline integrity: OK</comment>")
            self.line("<info>Validation successful. The workflow is valid.</info>")
            return 0
        except WorkflowLoadError as e:
            self.line_error(f"<error>Workflow Loading Error: {e}</error>")
            if hasattr(e, "errors") and callable(e.errors):
                try:
                    self.line_error(json.dumps(e.errors(), indent=2))
                except TypeError:
                    self.line_error(str(e.errors()))
            return 1
        except PipelineBuildError as e:
            self.line_error(f"<error>Pipeline Build Error: {e}</error>")
            return 1
        except Exception as e:
            self.line_error(
                f"<error>An unexpected error occurred during validation: {e}</error>"
            )
            self.line_error(f"<error>Type: {type(e).__name__}</error>")
            import traceback

            self.line_error(f"<error>Traceback: {traceback.format_exc()}</error>")
            return 1
