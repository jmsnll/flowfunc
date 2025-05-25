from __future__ import annotations

from pipeflow.console.commands.command import WorkflowCommand


class GraphCommand(WorkflowCommand):
    name: str = "graph"
    description: str = "Graphs a workflow."


    def handle(self) -> int:
        self.workflow.visualize_matplotlib()
        return 0
