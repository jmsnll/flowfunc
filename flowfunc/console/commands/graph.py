from __future__ import annotations

from typing import ClassVar

from cleo.io.inputs.option import Option

from flowfunc.console.commands.command import WorkflowCommand


class GraphCommand(WorkflowCommand):
    name: str = "graph"
    description: str = "Graphs a workflow."

    arguments: ClassVar[list[Option]] = [
        *WorkflowCommand._group_arguments(),
    ]

    def handle(self) -> int:
        self.load_workflow()
        self.context.workflow.pipeline.visualize_matplotlib()
        return 0
