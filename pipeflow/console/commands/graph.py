from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from cleo.helpers import argument

from pipeflow.console.commands.command import Command
from pipeflow.workflow.workflow import Workflow
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
        workflow = Workflow.from_dict(spec.data)
        # workflow.visualize(backend="matplotlib")
        workflow.print_documentation()
        workflow.info(print_table=True)
        # workflow.run(output_name="aggregate_results.summary", kwargs={"full_output": True})

        return 0
