import json
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

from flowfunc import WorkflowDefinition

console = Console()


@click.command(
    name="generate-schema",
    help="Generates and prints the JSON schema for workflow.yaml files.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(writable=True, dir_okay=False, path_type=Path),
    help="Path to save the JSON schema file. If not provided, prints to stdout.",
)
@click.option(
    "--indent",
    "-i",
    type=int,
    default=2,
    help="JSON indentation level for the output. Defaults to 2.",
)
def generate_schema(output: Path | None, indent: int) -> None:
    console.print("[cyan]Generating JSON schema for FlowFuncPipelineModel...[/cyan]")

    if indent < 0:
        console.print("[red]Indent value must be a non-negative integer.[/red]")
        raise click.Abort()

    try:
        schema_dict = WorkflowDefinition.model_json_schema()
        schema_json_str = json.dumps(schema_dict, indent=indent)

        if output:
            try:
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(schema_json_str + "\n", encoding="utf-8")
                console.print(
                    f"[green]JSON schema successfully saved to:[/green] {output}"
                )
            except Exception as e:
                console.print(
                    f"[red]Failed to write schema to file '{output}': {e}[/red]"
                )
                raise click.Abort()
        else:
            syntax = Syntax(schema_json_str, "json", theme="monokai", word_wrap=True)
            console.print(
                Panel(syntax, title="Workflow JSON Schema", border_style="cyan")
            )

    except Exception as e:
        console.print(
            f"[red]An unexpected error occurred while generating the schema: {e}[/red]"
        )
        raise click.Abort()
