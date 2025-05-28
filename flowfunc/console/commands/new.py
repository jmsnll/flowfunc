import textwrap
from pathlib import Path

import click
import yaml
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt

console = Console()


@click.command()
@click.argument("workflow_bundle_name")
@click.option(
    "--overwrite", "-o", is_flag=True, help="Overwrite workflow.yaml if it exists."
)
@click.option("--force-dir", is_flag=True, help="Allow existing bundle directory.")
def new(workflow_bundle_name: str, overwrite: bool, force_dir: bool):
    """Creates a new workflow bundle (directory + workflow.yaml file)."""
    name = workflow_bundle_name
    root = Path.cwd()

    if not name or not name.replace("_", "").replace("-", "").isalnum():
        console.print(
            f"[red]Invalid bundle name '{name}'. Use alphanumerics, -, _.[/red]"
        )
        raise click.Abort()

    workflows_base = root / get_project_config_value("workflows_directory", "workflows")
    bundle_path = workflows_base / name
    workflow_path = bundle_path / "workflow.yaml"

    if not workflows_base.exists():
        if workflows_base.name == "workflows":
            try:
                workflows_base.mkdir(parents=True)
                console.print(
                    f"[yellow]Created workflows directory: {workflows_base.relative_to(root)}[/yellow]"
                )
            except Exception as e:
                console.print(f"[red]Could not create workflows directory: {e}[/red]")
                raise click.Abort()
        else:
            console.print(
                f"[red]Workflows directory '{workflows_base}' does not exist.[/red]"
            )
            raise click.Abort()

    if workflows_base.is_file():
        console.print(f"[red]{workflows_base} exists but is not a directory.[/red]")
        raise click.Abort()

    if bundle_path.exists():
        if not bundle_path.is_dir():
            console.print(
                f"[red]{bundle_path.relative_to(root)} exists but is not a directory.[/red]"
            )
            raise click.Abort()
        if not force_dir:
            console.print(
                "[yellow]Directory exists. Use --force-dir to continue.[/yellow]"
            )
            raise click.Abort()
        console.print(
            f"[bold yellow]Using existing directory: {bundle_path.relative_to(root)}[/bold yellow]"
        )
    else:
        try:
            bundle_path.mkdir(parents=True)
            console.print(
                f"[green]Created bundle directory: {bundle_path.relative_to(root)}[/green]"
            )
        except Exception as e:
            console.print(f"[red]Could not create bundle directory: {e}[/red]")
            raise click.Abort()

    if workflow_path.exists() and not overwrite:
        console.print(
            f"[red]{workflow_path.relative_to(root)} already exists. Use --overwrite to replace.[/red]"
        )
        raise click.Abort()

    default_meta_name = name.replace("_", "-")
    meta_name = Prompt.ask("Workflow name", default=default_meta_name)
    meta_version = Prompt.ask("Workflow version", default="0.1.0")
    meta_description = Prompt.ask("Workflow description (optional)", default="")

    default_src = get_project_config_value("source_directory", "src")
    default_module = rf"{default_src.replace('/', '.')}\.{name}_functions"
    spec_module = Prompt.ask(
        "Default Python module (optional)", default="", show_default=False
    )

    step = {
        "name": "example_step",
        "description": "A placeholder step. Implement its function and adjust inputs/outputs.",
        "inputs": {"input_arg1": "$global.example_global_input"},
        "options": {"output_name": "example_output"},
    }
    if not spec_module:
        step["function"] = f"your_custom_module.{name}_example_step_function"

    workflow_content = {
        "apiVersion": "flowfunc.dev/v1beta1",
        "kind": "Pipeline",
        "metadata": {
            "name": meta_name,
            "version": meta_version,
        },
        "spec": {
            "global_inputs": {
                "example_global_input": {
                    "description": "An example global input for this workflow.",
                    "type": "string",
                    "default": "hello from bundle",
                }
            },
            "steps": [step],
            "pipeline_outputs": ["example_step.example_output"],
        },
    }
    if meta_description:
        workflow_content["metadata"]["description"] = meta_description
    if spec_module:
        workflow_content["spec"]["default_module"] = spec_module

    with workflow_path.open("w", encoding="utf-8") as f:
        yaml.dump(workflow_content, f, sort_keys=False)

    readme_path = bundle_path / "README.md"
    readme_text = textwrap.dedent(f"""\
        # Workflow: {meta_name}

        Version: {meta_version}

        {meta_description or "This workflow bundle contains a flowfunc pipeline."}

        ## How to Run

        ```bash
        uvx flowfunc run {workflows_base.name}/{name} --inputs '{{"example_global_input": "custom_value"}}'
        ```
    """)
    readme_path.write_text(readme_text.strip(), encoding="utf-8")

    console.print(
        Panel.fit(
            "\n[green]Workflow bundle initialized successfully![/green]",
            box=box.ROUNDED,
        )
    )
    console.print(f"[cyan]Created:[/cyan] {workflow_path.relative_to(root)}")
    console.print(f"[cyan]Created:[/cyan] {readme_path.relative_to(root)}")


if __name__ == "__main__":
    new()
