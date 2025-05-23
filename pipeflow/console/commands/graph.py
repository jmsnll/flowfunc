from __future__ import annotations

import textwrap

from pipeflow.console.commands.command import Command


class GraphCommand(Command):
    name = "graph"
    description = "Graphs a workflow."

    def handle(self) -> int:
        return 0
