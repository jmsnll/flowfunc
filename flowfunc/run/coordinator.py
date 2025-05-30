# flowfunc/run/coordinator.py
import contextlib
import logging
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from pipefunc import Pipeline

from flowfunc.console.reporter import ConsoleReporter
from flowfunc.exceptions import WorkflowRunError
from flowfunc.pipeline.builder import PipelineBuilder
from flowfunc.pipeline.builder import PipelineBuildError
from flowfunc.pipeline.executor import PipelineExecutionError
from flowfunc.pipeline.executor import PipelineExecutor
from flowfunc.run.environment import RunEnvironmentManager
from flowfunc.run.environment import RunEnvironmentManagerError
from flowfunc.run.input_provider import InputProvider
from flowfunc.run.input_provider import InputProviderError
from flowfunc.run.input_resolver import InputResolver
from flowfunc.run.input_resolver import InputResolverError
from flowfunc.run.output_persister import OutputPersister
from flowfunc.run.output_persister import OutputPersisterError
from flowfunc.run.state_tracker import RunStateTracker
from flowfunc.run.summary_model import Status
from flowfunc.run.summary_model import Summary
from flowfunc.run.summary_persister import SummaryPersistenceError
from flowfunc.run.summary_persister import SummaryPersister
from flowfunc.workflow_definition.loader import WorkflowDefinitionLoader
from flowfunc.workflow_definition.loader import WorkflowDefinitionLoaderError

logger = logging.getLogger(__name__)


class WorkflowRunCoordinator:
    """
    Orchestrates the entire lifecycle of a workflow run,
    from loading the definition to executing the pipeline and summarizing the results.
    """

    def __init__(
        self,
        # Core services (can be injected or instantiated)
        definition_loader: WorkflowDefinitionLoader | None = None,
        pipeline_builder: PipelineBuilder | None = None,
        env_manager: RunEnvironmentManager | None = None,
        input_provider: InputProvider | None = None,
        input_resolver: InputResolver | None = None,
        pipeline_executor: PipelineExecutor | None = None,
        output_persister: OutputPersister | None = None,
        summary_persister: SummaryPersister | None = None,
        reporter: ConsoleReporter | None = None,
        project_config_path: Path | None = None,
    ) -> None:
        self.definition_loader = definition_loader or WorkflowDefinitionLoader()
        self.pipeline_builder = pipeline_builder or PipelineBuilder()
        self.env_manager = env_manager or RunEnvironmentManager(
            config_file_path=project_config_path
        )
        self.input_provider = input_provider or InputProvider()
        self.input_resolver = input_resolver or InputResolver()
        self.pipeline_executor = pipeline_executor or PipelineExecutor()
        self.output_persister = output_persister or OutputPersister()
        self.summary_persister = summary_persister or SummaryPersister()
        self.reporter = reporter

    def execute_workflow(
        self,
        workflow_file_path: Path,
        input_data: dict[str, Any] | None = None,
        input_file_path: Path | None = None,
        custom_run_name: str | None = None,
        custom_run_id: str | None = None,
    ) -> Summary:
        """Executes a workflow from a definition file with specified inputs."""
        if self.reporter:
            self.reporter.print_welcome_message()

        state_tracker = RunStateTracker(
            run_id=custom_run_id, custom_run_name=custom_run_name
        )
        run_id = state_tracker.run_id
        summary: Summary | None = None

        try:
            with self._report_status("Loading workflow definition..."):
                workflow_model = self.definition_loader.from_path(workflow_file_path)

            with self._report_status("Setting up run environment..."):
                run_dir, _ = self.env_manager.setup_run_directories(
                    workflow_model.metadata.name, run_id
                )

            state_tracker.start_run(
                workflow_name=workflow_model.metadata.name,
                run_dir=run_dir,
                workflow_file=workflow_file_path,
            )
            summary = state_tracker.get_summary()

            pipeline: Pipeline
            with self._report_status("Building pipeline..."):
                pipeline = self.pipeline_builder.build(workflow_model)

            raw_user_inputs: dict[str, Any] = {}
            if input_file_path:
                with self._report_status(
                    f"Loading inputs from file: {input_file_path}..."
                ):
                    raw_user_inputs = self.input_provider.load_from_file(
                        input_file_path
                    )
            elif input_data is not None:
                raw_user_inputs = input_data
                logger.info("Using directly provided input data.")
            else:
                logger.info(
                    "No input file or data provided. Proceeding with defaults if any."
                )
            state_tracker.update_user_inputs(raw_user_inputs)

            resolved_inputs: dict[str, Any]
            with self._report_status("Resolving inputs..."):
                pipeline_info = pipeline.info()
                resolved_inputs = self.input_resolver.resolve(
                    user_inputs=raw_user_inputs,
                    workflow_model=workflow_model,
                    pipeline_inputs=pipeline_info.get("inputs", []),
                    pipeline_required_inputs=pipeline_info.get("required_inputs", []),
                )
            state_tracker.update_resolved_inputs(resolved_inputs)

            pipeline_results: dict[str, Any]
            with self._report_status("Executing pipeline..."):
                pipeline_results = self.pipeline_executor.execute(
                    pipeline, resolved_inputs, workflow_model.metadata.name
                )

            persisted_outputs_manifest: dict[str, str] = {}
            with self._report_status("Persisting outputs..."):
                persisted_outputs_manifest = self.output_persister.persist(
                    results=pipeline_results,
                    workflow_model=workflow_model,
                    output_dir=summary.output_dir,
                )
            state_tracker.update_persisted_outputs(persisted_outputs_manifest)

            state_tracker.complete_run(status=Status.SUCCESS)

        except (
            WorkflowDefinitionLoaderError,
            PipelineBuildError,
            RunEnvironmentManagerError,
            InputProviderError,
            InputResolverError,
            PipelineExecutionError,
            OutputPersisterError,
            SummaryPersistenceError,
            Exception,
        ) as e:
            logger.error(f"Workflow run '{run_id}' failed: {e}", exc_info=True)
            if state_tracker and state_tracker._summary_data:
                state_tracker.complete_run(status=Status.FAILED, error_message=str(e))
            else:
                # Critical failure before state_tracker could init summary
                # Create a minimal summary for error reporting if possible
                # This part might need more robust handling depending on when error can occur
                workflow_name = (
                    getattr(workflow_model, "metadata.name", "unknown")
                    if "workflow_model" in locals()
                    else "unknown"
                )
                run_dir = (
                    Path(self.env_manager.runs_base_dir / workflow_name / run_id)
                    if self.env_manager
                    else f"Unknown Run Directory ({run_id=})"
                )
                minimal_summary = Summary(
                    run_id=run_id,
                    workflow_name=getattr(
                        workflow_model, "metadata.name", "Unknown Workflow"
                    )
                    if "workflow_model" in locals()
                    else "Unknown Workflow",
                    workflow_file=workflow_file_path,
                    status=Status.FAILED,
                    run_dir=run_dir,
                    error_message=str(e),
                    start_time=summary.start_time,
                    end_time=datetime.now(UTC),
                )

                # Ensure summary is assigned for finally block
                summary = minimal_summary

            raise WorkflowRunError(f"Run '{run_id}' failed.") from e
        finally:
            if summary:
                try:
                    with self._report_status("Saving run summary..."):
                        self.summary_persister.save(summary)
                except Exception as e_sum:  # pylint: disable=broad-except
                    # Do not re-raise here, the original error (if any) is more important.
                    logger.error(
                        f"Failed to save summary for run '{run_id}': {e_sum}",
                        exc_info=True,
                    )

                if self.reporter:
                    if (
                        "persisted_outputs_manifest" in locals()
                        and persisted_outputs_manifest
                    ):
                        self.reporter.display_outputs_table(persisted_outputs_manifest)
                    self.reporter.display_run_summary_panel(summary)
            else:
                logger.error(
                    f"Critical error in run '{run_id}': No summary data was generated to save or report."
                )

        return summary

    def _report_status(self, message: str):
        """Helper to use console reporter's status context manager if available."""
        if self.reporter:
            return self.reporter.status(message)
        return contextlib.nullcontext()  # No-op context manager
