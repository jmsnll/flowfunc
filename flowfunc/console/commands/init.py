from __future__ import annotations

import click
import tomlkit
from rich.panel import Panel
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.table import Table
from rich.text import Text

from flowfunc import locations
from flowfunc.console import console

# from flowfunc.pyproject.toml import load_pyproject

DEFAULTS = {
    "source_directory": "src",
    "workflows_directory": "workflows",
    "runs_directory": "runs",
}


@click.command(
    name="init",
    help="Initializes a FlowFunc project by configuring pyproject.toml and creating standard directories.",
)
def init() -> None:
    with console.status("[bold green]Loading pyproject.toml..."):
        pyproject = load_pyproject()
        config = pyproject.read()

    tool_table = config.get("tool", tomlkit.table())
    flowfunc_table = tool_table.get("flowfunc", tomlkit.table())
    updated = False

    for key, default in DEFAULTS.items():
        if key not in flowfunc_table:
            flowfunc_table[key] = default
            updated = True

    if updated:
        tool_table["flowfunc"] = flowfunc_table
        config["tool"] = tool_table
        try:
            pyproject.write(config)
            console.log(
                "[bold green]✅ Updated pyproject.toml with [tool.flowfunc] defaults.[/bold green]"
            )
        except Exception as e:
            console.log(f"[bold red]❌ Failed to save pyproject.toml: {e}[/bold red]")
            raise click.Abort()
    else:
        console.log(
            "[yellow]⚠️ [tool.flowfunc] already contains required config. Skipping update.[/yellow]"
        )

    project_dir = locations.project_root()
    directories = {
        "Source": project_dir / flowfunc_table["source_directory"],
        "Workflows": project_dir / flowfunc_table["workflows_directory"],
        "Runs": project_dir / flowfunc_table["runs_directory"],
    }

    created = []
    failed = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
        console=console,
    ) as progress:
        task = progress.add_task("Creating project directories...", total=None)
        for label, path in directories.items():
            try:
                if not path.exists():
                    path.mkdir(parents=True, exist_ok=True)
                    created.append((label, path))
                elif not path.is_dir():
                    console.log(
                        f"[bold red]❌ Path '{path.relative_to(project_dir)}' exists but is not a directory.[/bold red]"
                    )
                    failed.append((label, path, "Not a directory"))
            except Exception as e:
                failed.append((label, path, str(e)))
        progress.remove_task(task)

    if created:
        console.log("[bold green]📁 Created directories:[/bold green]")
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Label", style="cyan")
        table.add_column("Path", style="magenta")
        for label, path in created:
            table.add_row(label, str(path.relative_to(project_dir)))
        console.print(table)

    if failed:
        console.log("[bold red]Some directories could not be created:[/bold red]")
        for label, path, error in failed:
            console.log(f"[red]- {label} ({path}): {error}[/red]")

    console.print(
        Panel.fit(
            Text.from_markup(
                "\n[bold green]🎉 FlowFunc project initialized successfully![/bold green]\n"
                "\n[cyan]Next step:[/cyan] [bold]flowfunc new <your_bundle_name>[/bold]"
            ),
            title="✅ Done",
            border_style="green",
        )
    )
