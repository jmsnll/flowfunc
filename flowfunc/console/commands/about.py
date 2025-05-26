from __future__ import annotations

import textwrap

from flowfunc.console.commands.command import Command


class AboutCommand(Command):
    name = "about"
    description = "Shows information about FlowFunc."

    def handle(self) -> int:
        from importlib import metadata

        self.line(
            textwrap.dedent(
                f"""\
            <info>FlowFunc - A wrapper around pipefunc for managing workflows.

            Version: {metadata.version("flowfunc")}
            </info>
            """
            )
        )

        return 0
