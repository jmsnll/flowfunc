from __future__ import annotations

import json
from typing import Any  # Removed cast
from typing import ClassVar

from cleo.helpers import option
from cleo.io.inputs.option import Option

from pipeflow.console.commands.command import WorkflowCommand


class RunCommand(WorkflowCommand):
    name = "run"
    description = "Runs a workflow with the specified inputs, checking for missing required inputs."

    options: ClassVar[list[Option]] = [  # type: ignore[assignment]
        option(
            "inputs",
            "i",
            description="Input data for the workflow as a JSON string. "
            'E.g., \'{"param1": ["value1"], "param2": "global_value"}\'. '
            "Parameters that are mapped over should be provided as lists.",
            flag=False,
            default="{}",
        ),
        option(
            "inputs-file",
            None,
            description="Path to a JSON file containing input data for the workflow. "
            "Overrides --inputs if both are provided.",
            flag=False,
        ),
    ]

    def _load_user_inputs(self) -> dict[str, Any] | None:
        """Loads inputs provided by the user via CLI options."""
        user_provided_inputs: dict[str, Any] = {}
        inputs_str = self.option("inputs")
        inputs_file = self.option("inputs-file")

        if inputs_file:
            self.line(f"<comment>Loading inputs from file: {inputs_file}</comment>")
            try:
                with open(inputs_file) as f:
                    user_provided_inputs = json.load(f)
            except FileNotFoundError:
                self.line_error(f"<error>Input file not found: {inputs_file}</error>")
                return None
            except json.JSONDecodeError as e:
                self.line_error(
                    f"<error>Invalid JSON in input file {inputs_file}: {e}</error>"
                )
                return None
        elif inputs_str:
            try:
                user_provided_inputs = json.loads(inputs_str)
            except json.JSONDecodeError as e:
                self.line_error(f"<error>Invalid JSON string for inputs: {e}</error>")
                return None

        if not isinstance(user_provided_inputs, dict):
            self.line_error("<error>Inputs must be a JSON object (dictionary).</error>")
            return None

        return user_provided_inputs

    def _apply_pipeflow_global_defaults(
        self, current_inputs: dict[str, Any], all_pipeline_input_names: tuple[str, ...]
    ) -> dict[str, Any]:
        """Applies defaults from spec.global_inputs."""
        resolved_inputs = current_inputs.copy()
        if not self.workflow_model or not self.workflow_model.spec.global_inputs:
            return resolved_inputs

        for name, definition in self.workflow_model.spec.global_inputs.items():
            if name not in resolved_inputs and definition.default is not None:
                if name in all_pipeline_input_names:
                    resolved_inputs[name] = definition.default
                    self.line(
                        f"<comment>Using default value for '{name}' from workflow's global_inputs.</comment>"
                    )
        return resolved_inputs

    def _get_missing_inputs_using_pipeline_info(
        self, effective_inputs: dict[str, Any], pipeline_info: dict[str, Any]
    ) -> list[str]:
        """Checks for missing inputs based on pipeline_info['required_inputs']."""
        missing: list[str] = []
        pipefunc_required_inputs = pipeline_info.get("required_inputs", [])

        for req_input_name in pipefunc_required_inputs:
            if req_input_name not in effective_inputs:
                missing.append(req_input_name)
        return missing

    def _execute_pipeline_with_inputs(self, final_inputs: dict[str, Any]) -> int:
        """Executes the pipeline and prints results or errors."""
        # self.workflow is assumed to be non-None here due to checks in handle()
        try:
            self.line("<info>Executing workflow...</info>")
            results = self.workflow.map(final_inputs)  # type: ignore
            self.line("<info>Workflow execution finished.</info>")
            self.line("<comment>Results:</comment>")

            output_dict = {}
            if results:
                for key, output_obj in results.items():
                    output_dict[key] = getattr(output_obj, "output", output_obj)

            self.line(json.dumps(output_dict, indent=2, default=str))
            return 0
        except Exception as e:
            self.line_error(
                f"<error>An error occurred during workflow execution: {e}</error>"
            )
            if self.io.is_debug() or self.io.is_very_verbose():
                import traceback

                self.io.write_error_line(traceback.format_exc())
            return 1

    def handle(self) -> int:
        self.line(
            f"<info>Attempting to run workflow: {self.argument('workflow')}</info>"
        )

        if self.workflow is None or self.workflow_model is None:
            return 1

        try:
            pipeline_info = self.workflow.info()
            if pipeline_info is None:
                self.line_error(
                    "<error>Could not retrieve pipeline input/output information from pipefunc.</error>"
                )
                return 1
        except Exception as e:
            self.line_error(
                f"<error>Failed to get pipeline info from pipefunc: {e}</error>"
            )
            return 1

        all_pipeline_input_names = pipeline_info.get("inputs", tuple())

        user_inputs = self._load_user_inputs()
        if user_inputs is None:
            return 1

        inputs_with_defaults = self._apply_pipeflow_global_defaults(
            user_inputs,
            all_pipeline_input_names,  # self.workflow_model is used internally
        )

        missing_inputs = self._get_missing_inputs_using_pipeline_info(
            inputs_with_defaults, pipeline_info
        )

        if missing_inputs:
            self.line_error(
                "<error>Execution aborted. The following required inputs are missing:</error>"
            )
            for missing_input in missing_inputs:
                self.line_error(f"  - {missing_input}")
            self.line_error(
                "These inputs are required by the pipeline and were not provided by the user, nor do they have a default in the workflow's global_inputs that applies."
            )
            return 1

        pipefunc_required_inputs = pipeline_info.get("required_inputs", [])
        if (
            not user_inputs
            and not inputs_with_defaults
            and any(
                p_name in pipefunc_required_inputs
                for p_name in all_pipeline_input_names
            )
        ):
            self.line(
                "<warning>No inputs provided by user and no global defaults were applicable or defined for some required inputs. This may lead to errors if functions have no internal defaults (though pipeline.info should account for this).</warning>"
            )
        elif not user_inputs and inputs_with_defaults:
            self.line(
                f"<comment>No explicit inputs provided by user. Using resolved defaults. Effective inputs: {json.dumps(inputs_with_defaults, indent=2, default=str)}</comment>"
            )
        else:
            self.line(
                f"<comment>Final effective inputs: {json.dumps(inputs_with_defaults, indent=2, default=str)}</comment>"
            )

        return self._execute_pipeline_with_inputs(
            inputs_with_defaults
        )  # self.workflow is used internally
