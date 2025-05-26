from __future__ import annotations

from flowfunc.console.commands.command import Command


class StepCommand(Command):
    name = "step"
    description = "Runs an individual step in a workflow."

    def handle(self) -> int:
        return 0
