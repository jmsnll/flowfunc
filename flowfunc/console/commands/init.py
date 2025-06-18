from __future__ import annotations

import click
from rich.panel import Panel
from rich.text import Text
from pathlib import Path

from flowfunc.console import console

CONVENTIONAL_DIRS = {
    "Source": "src",
    "Workflows": "workflows",
}

MAIN_PY_TEMPLATE = """
def create_greeting(name: str) -> str:
    \"\"\"Creates a greeting message.\"\"\"
    print(f"Creating greeting for {name}...")
    return f"Hello, {name}!"

def save_message(message: str, output_path: str) -> str:
    \"\"\"Saves the message to a text file.\"\"\"
    print(f"Saving message to {output_path}...")
    with open(output_path, "w") as f:
        f.write(message)
    return output_path
"""

WORKFLOW_YAML_TEMPLATE = """
apiVersion: flowfunc.dev/v1alpha1
kind: Workflow
metadata:
  name: hello-world-workflow
  description: "A simple workflow to greet a user and save the message."

spec:
  # Assumes 'src' is in the Python path.
  # Users will run `flowfunc` from the project root.
  module: main

  inputs:
    - name: user_name
      description: "The name of the person to greet."
      default: "World"
    - name: output_file
      default: "greeting.txt"

  steps:
    - name: make_greeting
      function: create_greeting
      args:
        name: "{{ inputs.user_name }}"

    - name: write_to_file
      function: save_message
      args:
        message: "{{ steps.make_greeting.outputs.return_value }}"
        output_path: "{{ inputs.output_file }}"
"""


@click.command(
    name="init",
    help="Initializes a new FlowFunc project with a conventional layout and example workflow.",
)
@click.argument("directory", type=click.Path(file_okay=False, dir_okay=True, writable=True, resolve_path=True), default=".")
def init(directory: str) -> None:
    """Initializes a FlowFunc project by creating a conventional directory
    structure and a runnable example workflow.
    """
    project_dir = Path(directory)
    console.log(f"[bold green]Initializing FlowFunc project in [cyan]{project_dir}[/cyan]...[/bold green]")

    if directory != ".":
        project_dir.mkdir(parents=True, exist_ok=True)

    console.log("[bold green]📁 Creating conventional directories...[/bold green]")
    source_dir = project_dir / CONVENTIONAL_DIRS["Source"]
    workflows_dir = project_dir / CONVENTIONAL_DIRS["Workflows"]

    source_dir.mkdir(exist_ok=True)
    workflows_dir.mkdir(exist_ok=True)
    console.log(f"  - Source:     [magenta]{CONVENTIONAL_DIRS['Source']}/[/magenta]")
    console.log(f"  - Workflows:  [magenta]{CONVENTIONAL_DIRS['Workflows']}/[/magenta]")

    console.log("[bold green]📝 Creating example workflow files...[/bold green]")
    source_file = source_dir / "main.py"
    workflow_file = workflows_dir / "workflow.yaml"

    if not source_file.exists():
        source_file.write_text(MAIN_PY_TEMPLATE)
        console.log(f"  - Created: [magenta]{source_file.relative_to(project_dir)}[/magenta]")
    else:
        console.log(f"  - Skipped: [yellow]{source_file.relative_to(project_dir)} already exists.[/yellow]")


    if not workflow_file.exists():
        workflow_file.write_text(WORKFLOW_YAML_TEMPLATE)
        console.log(f"  - Created: [magenta]{workflow_file.relative_to(project_dir)}[/magenta]")
    else:
        console.log(f"  - Skipped: [yellow]{workflow_file.relative_to(project_dir)} already exists.[/yellow]")


    run_command = f"flowfunc run {workflow_file.relative_to(project_dir)}"
    final_message = (
        "\n[bold green]🎉 FlowFunc project initialized successfully![/bold green]\n\n"
        "[cyan]Next steps:[/cyan]\n"
    )
    if directory != ".":
        final_message += f"1. [bold]cd {project_dir.name}[/bold]\n"
    final_message += "2. Add the source directory to your path: [bold]export PYTHONPATH=$PYTHONPATH:src[/bold]\n"
    final_message += f"3. [bold]{run_command}[/bold]"

    console.log(
        Panel.fit(
            Text.from_markup(final_message),
            title="✅ Done",
            border_style="green",
        )
    )