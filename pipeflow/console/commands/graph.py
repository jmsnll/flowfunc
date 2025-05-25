from __future__ import annotations

from typing import TYPE_CHECKING
from typing import ClassVar

from cleo.helpers import argument

from pipeflow.console.commands.command import Command
from pipeflow.workflow import pipeline
from pipeflow.workflow.yaml import WorkflowYAML

if TYPE_CHECKING:
    from cleo.io.inputs.argument import Argument


class GraphCommand(Command):
    name = "graph"
    description = "Graphs a workflow."

    arguments: ClassVar[list[Argument]] = [
        argument(
            "file",
            "The file to graph.",
        )
    ]

    def handle(self) -> int:
        spec = WorkflowYAML(self.argument("file"))
        workflow = pipeline.new_from_yaml(spec.data)
        workflow.visualize_matplotlib()
        return 0
