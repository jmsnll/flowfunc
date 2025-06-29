import contextlib
import logging
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from flowfunc.console.reporter import ConsoleReporter
from flowfunc.exceptions import FlowfuncError
from flowfunc.exceptions import WorkflowRunError
from flowfunc.pipeline.builder import PipelineBuilder
from flowfunc.pipeline.executor import PipelineExecutor
from flowfunc.run.environment import RunEnvironmentManager
from flowfunc.run.input_provider import InputProvider
from flowfunc.run.input_resolver import InputResolver
from flowfunc.run.output_persister import OutputPersister
from flowfunc.run.state_tracker import RunStateTracker
from flowfunc.run.summary_model import Status
from flowfunc.run.summary_model import Summary
from flowfunc.run.summary_persister import SummaryPersister
from flowfunc.workflow_definition.loader import WorkflowDefinitionLoader

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
        if self.reporter:
            self.reporter.print_welcome_message()

        state_tracker = RunStateTracker(custom_run_id, custom_run_name)
        run_id = state_tracker.run_id
        summary: Summary | None = None

        try:
            workflow_model = self._load_workflow(workflow_file_path)
            run_dir = self._setup_environment(workflow_model, run_id)
            state_tracker.start_run(
                workflow_model.metadata.name, run_dir, workflow_file_path
            )
            summary = state_tracker.get_summary()

            pipeline = self._build_pipeline(workflow_model)
            raw_user_inputs = self._get_inputs(input_file_path, input_data)
            state_tracker.update_user_inputs(raw_user_inputs)

            resolved_inputs = self._resolve_inputs(
                pipeline, workflow_model, raw_user_inputs
            )
            state_tracker.update_resolved_inputs(resolved_inputs)

            pipeline_results = self._execute_pipeline(
                pipeline, resolved_inputs, workflow_model
            )
            outputs = self._persist_outputs(
                pipeline_results, workflow_model, summary.output_dir
            )
            state_tracker.update_persisted_outputs(outputs)

            state_tracker.complete_run(Status.SUCCESS)

        except (
            FlowfuncError,
            Exception,
        ) as e:
            logger.error(f"Workflow run '{run_id}' failed: {e}", exc_info=True)
            summary = self._handle_failure(
                e, state_tracker, run_id, summary, workflow_file_path
            )
            raise WorkflowRunError(f"Run '{run_id}' failed.") from e

        finally:
            self._finalize(summary, run_id)

        return summary

    def _load_workflow(self, path: Path):
        with self._report_status("Loading workflow definition..."):
            return self.definition_loader.from_path(path)

    def _setup_environment(self, model, run_id: str) -> Path:
        with self._report_status("Setting up run environment..."):
            run_dir, _ = self.env_manager.setup_run_directories(
                model.metadata.name, run_id
            )
            return run_dir

    def _build_pipeline(self, model):
        with self._report_status("Building pipeline..."):
            return self.pipeline_builder.build(model)

    def _get_inputs(
        self, input_file_path: Path | None, input_data: dict[str, Any] | None
    ):
        if input_file_path:
            with self._report_status(f"Loading inputs from file: {input_file_path}..."):
                return self.input_provider.load_from_file(input_file_path)
        if input_data is not None:
            logger.info("Using directly provided input data.")
            return input_data
        logger.info("No input file or data provided. Proceeding with defaults if any.")
        return {}

    def _resolve_inputs(self, pipeline, model, user_inputs):
        with self._report_status("Resolving inputs..."):
            info = pipeline.info()
            return self.input_resolver.resolve(
                user_inputs=user_inputs,
                workflow_model=model,
                pipeline_inputs=info.get("inputs", []),
                pipeline_required_inputs=info.get("required_inputs", []),
            )

    def _execute_pipeline(self, pipeline, inputs, model):
        with self._report_status("Executing pipeline..."):
            return self.pipeline_executor.execute(pipeline, inputs, model.metadata.name)

    def _persist_outputs(self, results, model, output_dir: Path):
        with self._report_status("Persisting outputs..."):
            return self.output_persister.persist(results, model, output_dir)

    def _handle_failure(self, error, tracker, run_id, summary, workflow_file_path):
        if tracker and tracker._summary:
            tracker.complete_run(Status.FAILED, str(error))
            return tracker.get_summary()

        # Build a minimal summary
        workflow_name = getattr(
            getattr(error, "workflow_model", {}), "metadata.name", "unknown"
        )
        run_dir = Path(self.env_manager.runs_base_dir / workflow_name / run_id)
        return Summary(
            run_id=run_id,
            workflow_name=workflow_name,
            workflow_file=workflow_file_path,
            status=Status.FAILED,
            run_dir=run_dir,
            error_message=str(error),
            start_time=summary.start_time if summary else datetime.now(UTC),
            end_time=datetime.now(UTC),
        )

    def _finalize(self, summary: Summary | None, run_id: str):
        if not summary:
            logger.error(f"No summary data available for run '{run_id}'")
            return

        try:
            with self._report_status("Saving run summary..."):
                self.summary_persister.save(summary)
        except Exception as e:
            logger.error(
                f"Failed to save summary for run '{run_id}': {e}", exc_info=True
            )

        if self.reporter:
            if summary.persisted_outputs:
                self.reporter.display_outputs_table(summary.persisted_outputs)
            self.reporter.display_run_summary_panel(summary)

    def _report_status(self, message: str):
        if self.reporter:
            return self.reporter.status(message)
        return contextlib.nullcontext()
