from __future__ import annotations

from pipeflow.console.commands.command import Command


class ValidateCommand(Command):
    name = "validate"
    description = "Validates a workflow."

    def handle(self) -> int:
        return 0
