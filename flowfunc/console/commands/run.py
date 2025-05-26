from __future__ import annotations

import json
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import ClassVar

from cleo.helpers import option
from cleo.io.inputs.option import Option

from flowfunc import __version__ as flowfunc_version
from flowfunc import locations
from flowfunc.console.commands.command import WorkflowCommand
from flowfunc.io.serializer import _SERIALIZERS
from flowfunc.workflow.run import generate_id
from flowfunc.workflow.schema import PipefuncCacheConfigUsed
from flowfunc.workflow.schema import RunInfoModel

PIPEFLOW_RUNS_DIR_NAME = "runs"


RunPaths = namedtuple("RunPaths", ["run_dir", "workflow_output_dir", "cache_dir"])


class RunCommand(WorkflowCommand):
    name = "run"
    description = (
        "Runs a workflow, managing run history, outputs, and run-specific caching."
    )

    options: ClassVar[list[Option]] = [  # type: ignore[assignment]
        option(
            "inputs",
            "i",
            description="Input data for the workflow as a JSON string.",
            flag=False,
            default="{}",
        ),
        option(
            "inputs-file",
            None,
            description="Path to a JSON file containing input data for the workflow.",
            flag=False,
        ),
        option(
            "run-name",
            None,
            description="A custom name for this run (will be part of the run ID / directory).",
            flag=False,
        ),
        option(
            "force-run-specific-cache",
            None,
            description="Force pipefunc disk cache into a run-specific directory, even if workflow YAML defines a static cache_dir.",
            flag=True,
        ),
        option(
            "output-root",
            None,
            description=f"Override the default project output root directory (default: ./{PIPEFLOW_RUNS_DIR_NAME}).",
            flag=False,
        ),
    ]

    _run_id: str | None = None
    _run_paths: RunPaths | None = None

    @property
    def user_input_values(self) -> dict[str, Any]:
        inputs_str = self.option("inputs")
        inputs_file = self.option("inputs-file")

        if inputs_file:
            self.line(f"<comment>Loading inputs from file: {inputs_file}</comment>")
            with Path(inputs_file).open() as f:
                return json.load(f)

        if inputs_str:
            result = json.loads(inputs_str)
            self.line(f"<comment>Loaded inputs from parameters: {result}</comment>")
            return result

        return {}

    def apply_global_defaults(
        self, current_inputs: dict[str, Any], all_input_names: tuple[str, ...]
    ) -> dict[str, Any]:
        resolved_inputs = current_inputs.copy()

        if not self.workflow_model.spec.global_inputs:
            return resolved_inputs

        for name, definition in self.workflow_model.spec.global_inputs.items():
            if name not in resolved_inputs and definition.default is not None:
                if name in all_input_names:
                    resolved_inputs[name] = definition.default
                    self.line(
                        f"<comment>Using default value for '{name}' from workflow's global_inputs: {definition.default}</comment>"
                    )

        return resolved_inputs

    def check_for_missing_inputs(self, effective_inputs: dict[str, Any]) -> None:
        required_inputs = self.workflow.info().get("required_inputs", [])
        missing = [key for key in required_inputs if key not in effective_inputs]

        if missing:
            self.line_error(
                "<error>Execution aborted. Missing required inputs:</error>"
            )
            for key in missing:
                self.line_error(f"  - {key}")
            raise ValueError(f"Missing required inputs: {', '.join(missing)}")

    @property
    def run_paths(self) -> RunPaths:
        """Creates run directory structure and returns (run_base_dir, outputs_dir, cache_dir_for_pipefunc)."""
        if self._run_paths is None:
            project_root = locations.project_root()

            if cli_output_root := self.option("output-root"):
                global_runs_base_dir = Path(cli_output_root).resolve()
            else:
                tool = self.pyproject.data.get("tool")
                flowfunc = tool.get("flowfunc", {})
                output_root_dir = flowfunc.get("runs_directory", PIPEFLOW_RUNS_DIR_NAME)
                global_runs_base_dir = project_root / output_root_dir

            # Sanitize workflow name for directory creation
            workflow_name_sanitized = "".join(
                c if c.isalnum() or c in ["-", "_"] else "_"
                for c in self.workflow_model.metadata.name
            ).strip("_")

            run_base_dir = global_runs_base_dir / workflow_name_sanitized / self.run_id
            # Determine specific output path for this workflow's explicitly saved artifacts
            # This considers workflow_model.spec.output_config
            workflow_outputs_base = run_base_dir / "outputs"

            if (
                self.workflow_model.spec.output_config
                and self.workflow_model.spec.output_config.base_path
            ):
                # Assuming output_config.base_path is relative within the run's "outputs" dir for now
                workflow_outputs_base = (
                    workflow_outputs_base
                    / self.workflow_model.spec.output_config.base_path
                )

            pipefunc_run_specific_cache_dir = run_base_dir / ".pipefunc_cache"

            try:
                workflow_outputs_base.mkdir(parents=True, exist_ok=True)
                self.line(
                    f"<comment>Run artifacts will be stored under: {run_base_dir.relative_to(project_root)}</comment>"
                )

                self._run_paths = RunPaths(
                    run_base_dir, workflow_outputs_base, pipefunc_run_specific_cache_dir
                )
            except Exception as e:
                self.line_error(
                    f"<error>Could not create run directory structure at {run_base_dir}: {e}</error>"
                )
                raise e
        return self._run_paths

    @property
    def effective_cache_kwargs(
        self,
    ) -> dict[str, Any] | None:
        """Determines effective cache_kwargs for pipefunc."""
        effective_kwargs: dict[str, Any] = {}
        cache_type = None

        if self.workflow_model.spec.pipeline_config:
            if self.workflow_model.spec.pipeline_config.cache_type:
                cache_type = self.workflow_model.spec.pipeline_config.cache_type.value
            if self.workflow_model.spec.pipeline_config.cache_kwargs:
                effective_kwargs = (
                    self.workflow_model.spec.pipeline_config.cache_kwargs.copy()
                )

        if cache_type == "disk":
            current_yaml_cache_dir = effective_kwargs.get("cache_dir")
            if self.option("force-run-specific-cache") or not current_yaml_cache_dir:
                effective_kwargs["cache_dir"] = str(self.run_paths.cache_dir)

        return effective_kwargs if cache_type else None

    def save_outputs(self, pipefunc_results: dict[str, Any]) -> dict[str, str]:
        """Saves declared pipeline_outputs based on their 'path' attribute."""
        saved_output_paths: dict[str, str] = {}
        if not self.workflow_model.spec.pipeline_outputs or not pipefunc_results:
            return saved_output_paths

        for output in self.workflow_model.spec.pipeline_outputs:
            if isinstance(output, str):
                if output in pipefunc_results:
                    self.line(
                        f"<comment>Declared output (not saved by flowfunc): '{output}' - value present in results.</comment>"
                    )
                continue

            if output.path and output.name in pipefunc_results:
                data_to_save = getattr(
                    pipefunc_results[output.name],
                    "output",
                    pipefunc_results[output.name],
                )

                actual_save_path = (
                    self.run_paths.workflow_output_dir / output.path
                ).resolve()

                try:
                    saved_output_paths[output.name] = str(
                        self.serialize_output(
                            data_to_save,
                            output_name=output.name,
                            target_path=actual_save_path,
                        )
                    )
                except Exception as e:
                    self.line_error(
                        f"<error>Failed to save declared output '{output.name}' to '{actual_save_path}': {e}</error>"
                    )
                    import traceback

                    self.io.write_error_line(traceback.format_exc())  # More debug info
        return saved_output_paths

    def serialize_output(
        self, data_to_save: Any, *, output_name: str, target_path: Path
    ) -> Path | None:
        file_suffix = target_path.suffix.lower()
        relative_save_path = target_path.relative_to(locations.project_root())

        if not (file_serializer := _SERIALIZERS.get(file_suffix)):
            self.line_error(
                f"<warning>Cannot automatically serialize output '{output_name}' to unsupported extension '{file_suffix}' for path '{target_path}'. Data not saved by flowfunc.</warning>"
            )
            return None

        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            self.line(
                f"<comment>Attempting to save output '{output_name}' to '{target_path}' (format: {file_suffix or 'unknown'})</comment>"
            )
            file_serializer(data_to_save, target_path)
            self.line(
                f"<info>Saved declared output '{output_name}' to '{relative_save_path}'</info>"
            )
        except Exception as e:
            self.line_error(
                f"<error>Failed to save declared output '{output_name}' to '{target_path}': {e}</error>"
            )

        return relative_save_path

    def save_run_info(
        self,
        run_info: RunInfoModel,
        run_dir: Path,
    ) -> None:
        """Saves the RunInfoModel to a 'run_info.json' file in the run_directory."""
        run_info_file_path = run_dir / "run_info.json"
        try:
            run_info_file_path.parent.mkdir(parents=True, exist_ok=True)
            run_info_file_path.write_text(run_info.model_dump_json(indent=2))
            self.line(
                f"<comment>Run info saved: {run_info_file_path.relative_to(locations.project_root())}</comment>"
            )
        except Exception as e:
            self.line_error(
                f"<error>Failed to write {run_info_file_path.name} for run {run_info.run_id}: {e}</error>"
            )
            if self.io.is_debug() or self.io.is_very_verbose():
                import traceback

                self.io.write_error_line(traceback.format_exc())

    @property
    def run_id(self) -> str:
        if not self._run_id:
            self._run_id = generate_id(self.option("run-name"))
        return self._run_id

    def handle(self) -> int:
        self.line(
            f"<info>Starting Run ID: {self.run_id} for workflow: {self.workflow_model.metadata.name}</info>"
        )

        if not self.run_paths:
            return 1

        start_time = datetime.now()
        status = "FAILED"  # Default status
        final_run_inputs: dict[str, Any] = {}
        saved_outputs_manifest: dict[str, str] = {}
        user_inputs_loaded: dict[str, Any] | None = {}

        # Determine cache config early if needed, or just before populating RunInfoModel
        final_cache_kwargs = self.effective_cache_kwargs

        try:
            if not self.workflow.info():
                raise ValueError("Workflow could not be loaded or info is unavailable.")

            if (
                user_inputs_loaded := self.user_input_values
            ) is None:  # Error already printed by _load_user_inputs
                raise ValueError("Failed to load user inputs.")

            all_pipeline_input_names = self.workflow.info().get("inputs", tuple())
            inputs_with_defaults = self.apply_global_defaults(
                user_inputs_loaded, all_pipeline_input_names
            )

            if missing_inputs := self.check_for_missing_inputs(inputs_with_defaults):
                self.line_error(
                    "<error>Execution aborted. The following required inputs are missing:</error>"
                )
                for missing_input in missing_inputs:
                    self.line_error(f"  - {missing_input}")
                raise ValueError("Missing required inputs.")

            results = self.workflow.map(inputs_with_defaults)  # Execute the workflow
            status = "SUCCESS"
            self.line("<info>Workflow execution finished successfully.</info>")

            if results:
                saved_outputs_manifest = self.save_outputs(results)
            else:
                self.line(
                    "<comment>Pipefunc execution returned no result data (or pipeline has no outputs).</comment>"
                )

        except Exception as e:
            status = "FAILED"
            self.line_error(
                f"<error>Workflow execution failed for run {self.run_id}: {e}</error>"
            )
            if self.io.is_debug() or self.io.is_very_verbose():
                import traceback

                self.io.write_error_line(traceback.format_exc())
        finally:
            end_time = datetime.now()
            project_root = locations.project_root()

            # Construct PipefuncCacheConfigUsed
            cache_type_val = None
            if (
                self.workflow_model.spec.pipeline_config
                and self.workflow_model.spec.pipeline_config.cache_type
            ):
                cache_type_val = (
                    self.workflow_model.spec.pipeline_config.cache_type.value
                )

            pipefunc_cache_config_for_run = PipefuncCacheConfigUsed(
                cache_type=cache_type_val,
                cache_kwargs=final_cache_kwargs or {},
            )

            # Construct the RunInfoModel instance
            # Ensure workflow_model is available here; it should be from earlier checks.
            if self.workflow_model:
                run_info_to_save = RunInfoModel(
                    run_id=self.run_id,
                    flowfunc_version=flowfunc_version.__version__,
                    workflow_metadata_name=self.workflow_model.metadata.name,
                    workflow_metadata_version=self.workflow_model.metadata.version,
                    workflow_file_relative_path=str(
                        Path(self.argument("workflow"))
                        .absolute()
                        .relative_to(project_root)
                    ),
                    status=status,
                    start_time_utc=start_time.astimezone(),
                    end_time_utc=end_time.astimezone(),
                    duration_seconds=round((end_time - start_time).total_seconds(), 3),
                    user_provided_inputs=user_inputs_loaded
                    if user_inputs_loaded is not None
                    else {},
                    effective_inputs_used_by_pipefunc=final_run_inputs,
                    flowfunc_persisted_outputs=saved_outputs_manifest,
                    pipefunc_cache_config_used=pipefunc_cache_config_for_run,
                    run_artifacts_base_dir_relative=str(
                        self.run_paths.run_dir.relative_to(project_root)
                        if self.run_paths.run_dir.is_absolute()
                        and project_root in self.run_paths.run_dir.parents
                        else str(self.run_paths.run_dir)
                    ),
                )
                self.save_run_info(run_info_to_save, self.run_paths.run_dir)
            else:
                self.line_error(
                    f"<error>Could not save run_info for run {self.run_id} as workflow model was not available.</error>"
                )

        return 0 if status == "SUCCESS" else 1
