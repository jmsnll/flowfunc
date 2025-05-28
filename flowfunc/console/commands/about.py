import textwrap
from importlib import metadata

import click
from rich.console import Console
from rich.panel import Panel

console = Console()


@click.command(name="about", help="Shows information about FlowFunc.")
def about():
    try:
        version = metadata.version("flowfunc")
    except metadata.PackageNotFoundError:
        version = "unknown"

    info_text = textwrap.dedent(f"""
        FlowFunc – A wrapper around pipefunc for managing workflows.

        Version: {version}
    """)

    console.print(
        Panel.fit(info_text.strip(), title="About FlowFunc", border_style="cyan")
    )
