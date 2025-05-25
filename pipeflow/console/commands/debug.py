from __future__ import annotations

from pathlib import Path

from cleo.io.inputs.string_input import StringInput

from pipeflow.console.commands.command import Command
from pipeflow.console.commands.command import WorkflowCommand


class DebugCommand(Command):
    name = "debug"
    description = "Interactively select and run commands with an example workflow."
    hidden = True

    @property
    def project_root(self) -> Path:
        return Path(__file__).resolve().parents[3]

    def _find_example_workflows(self) -> list[Path]:
        """Finds workflow.yaml files in the 'examples' directory."""
        try:
            examples_dir = self.project_root / "examples"
        except IndexError:
            self.line_error("<error>Could not find 'examples' directory.</error>")
            return []

        if not examples_dir.is_dir():
            self.line_error(
                f"<error>'examples' directory not found at: {examples_dir}</error>"
            )
            return []

        self.line(
            f"<comment>Searching for workflow.yaml files in: {examples_dir}</comment>"
        )
        workflow_files = list(examples_dir.rglob("workflow.yaml"))
        workflow_files.extend(list(examples_dir.rglob("*.workflow.yaml")))

        return sorted(set(workflow_files))

    def handle(self) -> int:
        self.line("<info>Pipeflow Debug Mode - Workflow Runner</info>")

        command_names_to_offer = []

        for name in self.application.command_loader.names:
            try:
                command_instance = self.application.find(name)
                if isinstance(command_instance, WorkflowCommand):
                    command_names_to_offer.append(name)
            except Exception:
                # Ignore commands that can't be found or other errors during introspection
                pass

        commands_to_offer = sorted(set(command_names_to_offer))
        if not commands_to_offer:
            self.line_error("<error>No supported commands available to debug.</error>")
            return 1

        chosen_command_name = self.must_choose(
            "Select a command to debug:", commands_to_offer
        )
        self.line(f"<comment>You selected command: {chosen_command_name}</comment>")

        example_workflows = self._find_example_workflows()
        if not example_workflows:
            self.line_error(
                f"<warning>No example workflow.yaml files found. "
                f"The '{chosen_command_name}' command requires a workflow path.</warning>"
            )
            if self.confirm(
                "Do you want to manually provide a workflow file path?", default=True
            ):  # Default to True since it's needed
                chosen_workflow_path_str = self.ask("Enter path to workflow file:")
                if (
                    not chosen_workflow_path_str
                    or not Path(chosen_workflow_path_str).exists()
                ):
                    self.line_error(
                        "<error>Invalid or non-existent workflow file path provided.</error>"
                    )
                    return 1
            else:
                self.line_error("<error>Workflow file is required to proceed.</error>")
                return 1  # Exit if no workflow is provided for a WorkflowCommand
        else:
            workflow_path_choices = [
                str(p.relative_to(self.project_root)) for p in example_workflows
            ]
            # Add an option to specify manually even if examples are found
            workflow_path_choices.append("-- Specify path manually --")

            selected_workflow_choice = self.choice(
                "Select an example workflow file (or specify manually):",
                workflow_path_choices,
            )

            if selected_workflow_choice == "-- Specify path manually --":
                chosen_workflow_path_str = self.ask("Enter path to workflow file:")
                if (
                    not chosen_workflow_path_str
                    or not Path(chosen_workflow_path_str).exists()
                ):
                    self.line_error(
                        "<error>Invalid or non-existent workflow file path provided.</error>"
                    )
                    return 1
            else:
                chosen_workflow_path_str = selected_workflow_choice

        if not chosen_workflow_path_str:  # Should not happen if logic above is correct
            self.line_error("<error>No workflow file was selected or provided.</error>")
            return 1

        self.line(
            f"<comment>You selected workflow: {chosen_workflow_path_str}</comment>"
        )

        command_parts = [chosen_command_name]
        command_parts.append(f'"{chosen_workflow_path_str}"')

        additional_args_str = self.ask(
            f"\nEnter any additional arguments or options for '{chosen_command_name}' "
            f'(e.g., --inputs \'{{"key": "value"}}\' or --start-step my_step).\n'
            f"Current command preview: '{' '.join(command_parts)} ...'\n"
            f"Your input will be appended: ",
            default="",
        ).strip()

        if additional_args_str:
            command_parts.append(additional_args_str)

        full_command_string = " ".join(command_parts)
        self.line(f"<info>Executing: pipeflow {full_command_string}</info>")
        self.line("-" * 30)

        try:
            sub_command_input = StringInput(full_command_string)
            return_code = self.application.run(sub_command_input, self.io.output)
            self.line("-" * 30)
            self.line(
                f"<info>Command '{chosen_command_name}' finished with exit code: {return_code}</info>"
            )
            return return_code
        except Exception as e:
            self.line_error(
                f"<error>An error occurred while trying to run the sub-command: {e}</error>"
            )
            if self.io.is_debug() or self.io.is_very_verbose():
                import traceback

                self.io.write_error_line(traceback.format_exc())
            return 1
