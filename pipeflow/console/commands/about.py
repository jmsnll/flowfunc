from __future__ import annotations

import textwrap

from pipeflow.console.commands.command import Command


class AboutCommand(Command):
    name = "about"
    description = "Shows information about Pipeflow."

    def handle(self) -> int:
        from importlib import metadata

        self.line(
            textwrap.dedent(
                f"""\
            <info>Pipeflow - A wrapper around pipefunc for managing workflows.
            
            Version: {metadata.version("pipeflow")}
            </info>
            """
            )
        )

        return 0
