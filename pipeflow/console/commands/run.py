from __future__ import annotations

from pipeflow.console.commands.command import Command


class RunCommand(Command):
    name = "run"
    description = "Runs a workflow."

    def handle(self) -> int:
        return 0
