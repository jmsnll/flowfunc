from __future__ import annotations

import textwrap

from pipeflow.console.commands.command import Command


class DescribeCommand(Command):
    name = "describe"
    description = "Describes a workflow."

    def handle(self) -> int:
        return 0
