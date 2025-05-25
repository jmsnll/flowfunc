from __future__ import annotations

from typing import TYPE_CHECKING
from typing import ClassVar

from cleo.helpers import argument

from pipeflow.console.commands.command import Command
from pipeflow.workflow.workflow import Workflow
from pipeflow.workflow.yaml import WorkflowYAML

if TYPE_CHECKING:
    from cleo.io.inputs.argument import Argument


class DocsCommand(Command):
    name = "docs"
    description = "Displays the documentation for a workflow."

    arguments: ClassVar[list[Argument]] = [
        argument(
            "file",
            "The file to graph.",
        )
    ]

    def handle(self) -> int:
        spec = WorkflowYAML(self.argument("file"))
        workflow = Workflow.from_dict(spec.data)
        workflow.print_documentation()
        return 0
