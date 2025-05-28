import textwrap

import click
from rich.panel import Panel

from flowfunc.__version__ import __version__
from flowfunc.console import console


@click.command(name="about", help="Shows information about FlowFunc.")
def about() -> None:
    info_text = textwrap.dedent(f"""
        FlowFunc – A wrapper around pipefunc for managing workflows.

        Version: {__version__}
    """)

    console.print(
        Panel.fit(info_text.strip(), title="About FlowFunc", border_style="cyan")
    )
