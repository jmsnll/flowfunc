from __future__ import annotations

from flowfunc.console.commands.command import WorkflowCommand


class GraphCommand(WorkflowCommand):
    name: str = "graph"
    description: str = "Graphs a workflow."

    def handle(self) -> int:
        self.context.workflow.pipeline.visualize_matplotlib()
        return 0
