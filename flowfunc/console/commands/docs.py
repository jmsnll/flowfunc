from __future__ import annotations

from flowfunc.console.commands.command import WorkflowCommand


class DocsCommand(WorkflowCommand):
    name = "docs"
    description = "Displays the documentation for a workflow."

    def handle(self) -> int:
        self.context.workflow.pipeline.print_documentation()
        return 0
