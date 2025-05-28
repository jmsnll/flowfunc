import click
from rich.console import Console

from flowfunc.console.commands import about
from flowfunc.console.commands import docs
from flowfunc.console.commands import graph
from flowfunc.console.commands import init
from flowfunc.console.commands import new
from flowfunc.console.commands import run

console = Console()


@click.group()
def cli() -> None:
    """FlowFunc CLI - Manage and explore your workflows."""
    pass


# Register subcommands
cli.add_command(about)
cli.add_command(docs)
cli.add_command(graph)
cli.add_command(init)
cli.add_command(new)
cli.add_command(run)

if __name__ == "__main__":
    cli()
