from __future__ import annotations

import traceback
from collections import namedtuple
from datetime import datetime
from typing import Any
from typing import ClassVar

from cleo.helpers import option
from cleo.io.inputs.option import Option

from flowfunc import locations
from flowfunc.console.commands.command import WorkflowCommand
from flowfunc.workflow import run
from flowfunc.workflow.schema import PipefuncCacheConfigUsed

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
    ]

    _run_id: str | None = None
    _run_paths: run.ArtifactPaths | None = None

    @property
    def run_paths(self) -> run.ArtifactPaths:
        """Creates run directory structure and returns (run_base_dir, outputs_dir, cache_dir_for_pipefunc)."""
        if self._run_paths is None:
            flowfunc = self.pyproject.data.get("tool").get("flowfunc", {})
            runs_directory_relative = flowfunc.get("runs_directory", "runs")
            runs_directory_absolute = locations.project_root() / runs_directory_relative
            self._run_paths = run.setup_directories(
                self.run_id, self.workflow_model.metadata.name, runs_directory_absolute
            )
        return self._run_paths

    @property
    def run_id(self) -> str:
        if not self._run_id:
            self._run_id = run.generate_unique_run_id(self.option("run-name"))
        return self._run_id

    def handle(self) -> int:
        start_time = datetime.now()
        run_status = "FAILED"  # Default status
        persisted_outputs_manifest: dict[str, str] = {}
        initial_user_inputs: dict[str, Any] = {}
        final_effective_inputs: dict[str, Any] = {}

        try:
            self.line(
                f"<info>Starting Run ID: {self.run_id} for workflow: {self.workflow_model.metadata.name}</info>"
            )
            self.line(
                f"<info>Initialised run output directories: {self.run_paths} for workflow: {self.workflow_model.metadata.name}</info>"
            )

            # TODO: don't pass both inputs
            initial_user_inputs = run.load_initial_inputs_from_sources(
                self.option("inputs"), self.option("inputs-file")
            )

            final_effective_inputs = run.resolve_inputs(
                initial_user_inputs,
                self.workflow_model.spec.global_inputs,
                self.workflow.info().get("inputs", tuple()),
                self.workflow.info().get("required_inputs", []),
            )
            cache_kwargs = run.determine_pipefunc_cache_kwargs(
                self.workflow_model.spec.pipeline_config, self.run_paths.cache_dir
            )

            results = self.workflow.map(final_effective_inputs)
            run_status = "SUCCESS"
            persisted_outputs_manifest = run.persist_workflow_outputs(
                results,
                self.workflow_model.spec.pipeline_outputs,
                self.run_paths.workflow_output_dir,
            )
        except Exception as e:  # Catch-all for unexpected errors
            self.line_error(
                f"<error>An unexpected error occurred during workflow run {self.run_id}: {e}</error>"
            )
            if self.io.is_debug() or self.io.is_very_verbose():
                self.io.write_error_line(traceback.format_exc())
        finally:
            end_time = datetime.now()
            from flowfunc import __version__ as flowfunc_version

            cache_type = None
            cache_config = PipefuncCacheConfigUsed(cache_type=None, cache_kwargs={})

            if self.workflow_model.spec.pipeline_config:
                if self.workflow_model.spec.pipeline_config.cache_type:
                    cache_type = str(
                        self.workflow_model.spec.pipeline_config.cache_type.value
                    )
                if self.workflow_model.spec.pipeline_config.cache_kwargs:
                    cache_config.cache_kwargs = (
                        self.workflow_model.spec.pipeline_config.cache_kwargs.copy()
                    )

            if cache_type == "disk":
                current_yaml_cache_dir = cache_config.cache_kwargs.get("cache_dir")
                if (
                    self.option("force-run-specific-cache")
                    or not current_yaml_cache_dir
                ):
                    cache_config.cache_kwargs["cache_dir"] = str(
                        self.run_paths.cache_dir
                    )

            run_info = run.prepare_run_info_model(
                run_id=self.run_id,
                status=run_status,
                start_time=start_time,
                end_time=end_time,
                flowfunc_version_str=flowfunc_version.__version__,
                workflow_model_instance=self.workflow_model,
                workflow_file_cli_arg=str(
                    self._workflow_path.absolute().relative_to(locations.project_root())
                ),
                initial_user_inputs=initial_user_inputs,
                final_effective_inputs=final_effective_inputs,
                persisted_outputs_manifest=persisted_outputs_manifest,
                effective_cache_config=cache_config,
                run_artifacts_dir_abs_path=self.run_paths.run_dir,
            )
            run_info.save_run_info(run_dir=self.run_paths.run_dir)

        return 0 if run_status == "SUCCESS" else 1
