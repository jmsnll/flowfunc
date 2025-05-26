from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import cast

from cleo.helpers import option
from cleo.io.inputs.option import Option

from pipeflow import __version__ as pipeflow_version
from pipeflow import locations
from pipeflow.console.commands.command import WorkflowCommand
from pipeflow.workflow.schema import PipeflowPipelineModel

PIPEFLOW_RUNS_DIR_NAME = "runs"


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

    def _load_user_inputs(self) -> dict[str, Any] | None:
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
        resolved_inputs = current_inputs.copy()
        if not self.workflow_model or not self.workflow_model.spec.global_inputs:  # type: ignore
            return resolved_inputs

        for name, definition in self.workflow_model.spec.global_inputs.items():  # type: ignore
            if name not in resolved_inputs and definition.default is not None:
                if name in all_pipeline_input_names:
                    resolved_inputs[name] = definition.default
                    self.line(
                        f"<comment>Using default value for '{name}' from workflow's global_inputs: {definition.default}</comment>"
                    )
        return resolved_inputs

    def _get_missing_inputs_using_pipeline_info(
        self, effective_inputs: dict[str, Any]
    ) -> list[str]:
        missing: list[str] = []
        pipefunc_required_inputs = self.workflow.info().get("required_inputs", [])

        for req_input_name in pipefunc_required_inputs:
            if req_input_name not in effective_inputs:
                missing.append(req_input_name)
        return missing

    def _generate_run_id(self, user_provided_name: str | None) -> str:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_suffix = uuid.uuid4().hex[:6]
        if user_provided_name:
            safe_name = "".join(
                c if c.isalnum() or c in ["-", "_"] else "_" for c in user_provided_name
            ).strip("_")
            return (
                f"{ts}_{safe_name}_{unique_suffix}"
                if safe_name
                else f"run_{ts}_{unique_suffix}"
            )
        return f"run_{ts}_{unique_suffix}"

    def _setup_run_paths(
        self, run_id: str, workflow_model: PipeflowPipelineModel
    ) -> tuple[Path, Path, Path] | None:
        """Creates run directory structure and returns (run_base_dir, outputs_dir, cache_dir_for_pipefunc)."""
        project_root = locations.project_root()

        if cli_output_root := self.option("output-root"):
            global_runs_base_dir = Path(cli_output_root).resolve()
        else:
            global_runs_base_dir = project_root / self.pyproject.data.get(
                "pipeflow", {}
            ).get("output_root_dir", PIPEFLOW_RUNS_DIR_NAME)

        # Sanitize workflow name for directory creation
        workflow_name_sanitized = "".join(
            c if c.isalnum() or c in ["-", "_"] else "_"
            for c in workflow_model.metadata.name
        ).strip("_")

        run_base_dir = global_runs_base_dir / workflow_name_sanitized / run_id

        # Determine specific output path for this workflow's explicitly saved artifacts
        # This considers workflow_model.spec.output_config
        workflow_outputs_base = run_base_dir / "outputs"

        if (
            workflow_model.spec.output_config
            and workflow_model.spec.output_config.base_path
        ):
            # Assuming output_config.base_path is relative within the run's "outputs" dir for now
            workflow_outputs_base = (
                workflow_outputs_base / workflow_model.spec.output_config.base_path
            )

        pipefunc_run_specific_cache_dir = run_base_dir / ".pipefunc_cache"

        try:
            workflow_outputs_base.mkdir(parents=True, exist_ok=True)
            # pipefunc_run_specific_cache_dir will be created by DiskCache if used.
            self.line(
                f"<comment>Run artifacts will be stored under: {run_base_dir.relative_to(project_root)}</comment>"
            )
            return run_base_dir, workflow_outputs_base, pipefunc_run_specific_cache_dir
        except Exception as e:
            self.line_error(
                f"<error>Could not create run directory structure at {run_base_dir}: {e}</error>"
            )
            return None

    def _get_effective_cache_kwargs(
        self,
        workflow_model: PipeflowPipelineModel,
        pipefunc_run_specific_cache_dir: Path,  # Now explicitly passed
        force_run_specific_cache: bool,
    ) -> dict[str, Any] | None:  # Return type hint to Dict
        """Determines effective cache_kwargs for pipefunc."""
        effective_kwargs: dict[str, Any] = {}  # Explicitly Dict
        cache_type = None

        if workflow_model.spec.pipeline_config:
            if workflow_model.spec.pipeline_config.cache_type:
                cache_type = workflow_model.spec.pipeline_config.cache_type.value
            if workflow_model.spec.pipeline_config.cache_kwargs:
                effective_kwargs = (
                    workflow_model.spec.pipeline_config.cache_kwargs.copy()
                )

        if cache_type == "disk":
            current_yaml_cache_dir = effective_kwargs.get("cache_dir")
            if force_run_specific_cache or not current_yaml_cache_dir:
                effective_kwargs["cache_dir"] = str(pipefunc_run_specific_cache_dir)
                # Logged by caller if different
            # If current_yaml_cache_dir exists and not forcing, it will be used.
            # The run_info.json will log what was actually used.

        return effective_kwargs if cache_type else None

    def _save_declared_outputs(
        self,
        pipefunc_results: dict[str, Any],
        workflow_model: PipeflowPipelineModel,
        workflow_outputs_save_dir: Path,  # Base directory to save outputs for this run
    ) -> dict[str, str]:
        """Saves declared pipeline_outputs based on their 'path' attribute."""
        saved_output_paths: dict[str, str] = {}
        if not workflow_model.spec.pipeline_outputs or not pipefunc_results:
            return saved_output_paths

        for out_def_union in workflow_model.spec.pipeline_outputs:
            if isinstance(out_def_union, str):
                # This output is declared by name only, pipeflow doesn't save it explicitly here.
                # It might be an intermediate result, or a file path returned by a step directly.
                # We could log its value if found in results.
                if out_def_union in pipefunc_results:
                    self.line(
                        f"<comment>Declared output (not saved by pipeflow): '{out_def_union}' - value present in results.</comment>"
                    )
                continue

            out_def = cast("PipelineOutputItem", out_def_union)

            if out_def.path and out_def.name in pipefunc_results:
                # Data to save, could be nested in an 'output' attribute if pipefunc wraps it
                data_to_save = getattr(
                    pipefunc_results[out_def.name],
                    "output",
                    pipefunc_results[out_def.name],
                )

                # Path is relative to workflow_outputs_save_dir
                actual_save_path = (workflow_outputs_save_dir / out_def.path).resolve()

                try:
                    actual_save_path.parent.mkdir(parents=True, exist_ok=True)

                    # Basic serialization based on extension. Needs to be more robust.
                    # TODO: Implement more serializers (pandas for csv/parquet, pickle, etc.)
                    file_suffix = actual_save_path.suffix.lower()
                    self.line(
                        f"<comment>Attempting to save output '{out_def.name}' to '{actual_save_path}' (format: {file_suffix or 'unknown'})</comment>"
                    )

                    if file_suffix == ".json":
                        with open(actual_save_path, "w") as f:
                            json.dump(data_to_save, f, indent=2, default=str)
                    elif file_suffix == ".txt":
                        with open(actual_save_path, "w") as f:
                            f.write(str(data_to_save))
                    elif file_suffix == ".pkl":
                        import pickle

                        with open(actual_save_path, "wb") as f:
                            pickle.dump(data_to_save, f)
                    # Add more elif for .csv (needs pandas), .parquet (needs pyarrow/fastparquet) etc.
                    else:
                        self.line_error(
                            f"<warning>Cannot automatically serialize output '{out_def.name}' to unsupported extension '{file_suffix}' for path '{out_def.path}'. Data not saved by pipeflow.</warning>"
                        )
                        continue

                    relative_saved_path = str(
                        actual_save_path.relative_to(locations.project_root())
                    )
                    self.line(
                        f"<info>Saved declared output '{out_def.name}' to '{relative_saved_path}'</info>"
                    )
                    saved_output_paths[out_def.name] = relative_saved_path
                except Exception as e:
                    self.line_error(
                        f"<error>Failed to save declared output '{out_def.name}' to '{actual_save_path}': {e}</error>"
                    )
                    import traceback

                    self.io.write_error_line(traceback.format_exc())  # More debug info
        return saved_output_paths

    def _write_run_info(
        self,
        run_base_dir: Path,
        run_id: str,
        workflow_model: PipeflowPipelineModel,
        user_inputs: dict,
        final_inputs_used: dict,
        status: str,
        start_time: datetime,
        end_time: datetime,
        saved_output_paths_manifest: dict[str, str],
        final_cache_kwargs_used: dict | None,
        workflow_file_abs_path: Path,
    ):
        run_info_path = run_base_dir / "run_info.json"
        project_root = locations.project_root()
        run_metadata = {
            "run_id": run_id,
            "pipeflow_version": pipeflow_version.__version__,
            "workflow_metadata_name": workflow_model.metadata.name,
            "workflow_metadata_version": workflow_model.metadata.version,
            "workflow_file_relative_path": str(
                workflow_file_abs_path.relative_to(project_root)
            ),
            "status": status,
            "start_time_utc": start_time.astimezone().isoformat(),
            "end_time_utc": end_time.astimezone().isoformat(),
            "duration_seconds": round((end_time - start_time).total_seconds(), 3),
            "user_provided_inputs": user_inputs,
            "effective_inputs_used_by_pipefunc": final_inputs_used,
            "pipeflow_persisted_outputs": saved_output_paths_manifest,
            "pipefunc_cache_config_used": {
                "cache_type": workflow_model.spec.pipeline_config.cache_type.value
                if workflow_model.spec.pipeline_config
                and workflow_model.spec.pipeline_config.cache_type
                else None,
                "cache_kwargs": final_cache_kwargs_used or {},
            },
            "run_artifacts_base_dir_relative": str(
                run_base_dir.relative_to(project_root)
            )
            if run_base_dir.is_absolute() and project_root in run_base_dir.parents
            else str(run_base_dir),
        }
        try:
            run_info_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure dir exists
            with open(run_info_path, "w") as f:
                json.dump(run_metadata, f, indent=2, default=str)
            self.line(
                f"<comment>Run info saved: {run_info_path.relative_to(project_root)}</comment>"
            )
        except Exception as e:
            self.line_error(
                f"<error>Failed to write run_info.json for run {run_id}: {e}</error>"
            )

    def handle(self) -> int:
        self.line(
            f"<info>Attempting to run workflow: {self.argument('workflow')}</info>"
        )

        run_id = self._generate_run_id(self.option("run-name"))
        self.line(
            f"<info>Starting Run ID: {run_id} for workflow: {self.workflow_model.metadata.name}</info>"
        )

        paths_setup = self._setup_run_paths(run_id, self.workflow_model)
        if not paths_setup:
            return 1
        run_base_dir, workflow_outputs_save_dir, pipefunc_run_specific_cache_dir = (
            paths_setup
        )

        start_time = datetime.now()
        status = "FAILED"
        final_run_inputs: dict[str, Any] = {}
        saved_outputs_manifest: dict[str, str] = {}
        user_inputs: dict[str, Any] | None = {}  # To store what user initially provided

        final_cache_kwargs = self._get_effective_cache_kwargs(
            self.workflow_model,
            pipefunc_run_specific_cache_dir,  # Pass the actual Path object
            self.option("force-run-specific-cache"),
        )

        try:
            if self.workflow.info() is None:
                raise ValueError(
                    "Could not retrieve pipeline input/output information from pipefunc."
                )

            user_inputs = self._load_user_inputs()
            if user_inputs is None:
                raise ValueError("Failed to load user inputs.")

            all_pipeline_input_names = self.workflow.info().get("inputs", tuple())
            inputs_with_defaults = self._apply_pipeflow_global_defaults(
                user_inputs, all_pipeline_input_names
            )

            missing_inputs = self._get_missing_inputs_using_pipeline_info(
                inputs_with_defaults
            )

            if missing_inputs:
                self.line_error(
                    "<error>Execution aborted. The following required inputs are missing:</error>"
                )
                for missing_input in missing_inputs:
                    self.line_error(f"  - {missing_input}")
                raise ValueError("Missing required inputs.")  # Propagate to finally

            final_run_inputs = inputs_with_defaults  # Store for metadata
            self.line(
                f"<comment>Final effective inputs for run {run_id}: {json.dumps(final_run_inputs, indent=2, default=str)}</comment>"
            )
            if final_cache_kwargs and final_cache_kwargs.get("cache_dir"):
                self.line(
                    f"<comment>Pipefunc disk cache for this run expected at: {final_cache_kwargs['cache_dir']}</comment>"
                )

            results = self.workflow.map(final_run_inputs)
            status = "SUCCESS"
            self.line("<info>Workflow execution finished successfully.</info>")

            if results:
                saved_outputs_manifest = self._save_declared_outputs(
                    results, self.workflow_model, workflow_outputs_save_dir
                )
                # Optionally print summary of pipefunc results not explicitly saved by pipeflow
                self.line("<comment>Raw pipefunc results overview (keys):</comment>")
                self.line(json.dumps(list(results.keys()), indent=2))

            else:
                self.line(
                    "<comment>Pipefunc execution returned no result data (or pipeline has no outputs).</comment>"
                )

        except Exception as e:
            status = "FAILED"
            self.line_error(
                f"<error>Workflow execution failed for run {run_id}: {e}</error>"
            )
            if self.io.is_debug() or self.io.is_very_verbose():
                import traceback

                self.io.write_error_line(traceback.format_exc())
        finally:
            end_time = datetime.now()
            # Ensure user_inputs is defined for _write_run_info
            ui = user_inputs if user_inputs is not None else {}
            self._write_run_info(
                run_base_dir,
                run_id,
                self.workflow_model,
                ui,
                final_run_inputs,
                status,
                start_time,
                end_time,
                saved_outputs_manifest,
                final_cache_kwargs,
                Path(self.argument("workflow")).absolute(),
            )

        return 0 if status == "SUCCESS" else 1
