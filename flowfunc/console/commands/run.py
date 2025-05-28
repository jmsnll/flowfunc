from __future__ import annotations

import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import ClassVar

from cleo.helpers import option
from cleo.io.inputs.option import Option

from flowfunc.console.commands.command import WorkflowCommand
from flowfunc.workflow import inputs
from flowfunc.workflow import loader
from flowfunc.workflow import outputs
from flowfunc.workflow import pipeline
from flowfunc.workflow import run
from flowfunc.workflow.context import PathsContext
from flowfunc.workflow.context import Status

logger = logging.getLogger(__name__)


class RunCommand(WorkflowCommand):
    name = "run"
    description = (
        "Runs a workflow, managing run history, outputs, and run-specific caching."
    )

    arguments: ClassVar[list[Option]] = [
        *WorkflowCommand._group_arguments(),
    ]

    options: ClassVar[list[Option]] = [  # type: ignore[assignment]
        option(
            "inputs",
            None,
            description="Path to a JSON file containing input data for the workflow.",
            flag=False,
        ),
        option(
            "name",
            None,
            description="A custom name for this run (will be part of the run ID / directory).",
            flag=False,
        ),
    ]

    def handle(self) -> int:
        workflow_path = Path(self.argument("workflow"))

        if not workflow_path.exists():
            raise FileNotFoundError(f"Workflow file not found: {workflow_path}")

        self.context.workflow.file_path = workflow_path
        self.context.workflow.model = loader.load_from_path(workflow_path)
        self.context.workflow.pipeline = pipeline.from_model(
            loader.load_from_path(workflow_path).spec
        )

        self.context.metadata.run_id = run.generate_unique_id()
        self.context.metadata.start_time = datetime.now()
        self.context.metadata.status = Status.FAILED
        self.context.paths = PathsContext.build(
            self.context.workflow.model.metadata.name,
            self.context.metadata.run_id,
            self.toml_config,
        )

        self.context.metadata.start()
        try:
            logger.debug(f"Running paths: {self.context.paths}")
            logger.info(
                f"Starting Run ID: {self.context.metadata.run_id} for workflow: {self.context.workflow.model.metadata.name}"
            )
            logger.info(
                f"Initialised run output directories: {self.context.paths.output_dir} for workflow: {self.context.workflow.model.metadata.name}"
            )

            if input_file_path := self.option("inputs"):
                input_file_path = Path(input_file_path)

                self.context.inputs.user_inputs = inputs.from_file(input_file_path)

            self.context.inputs.resolved_inputs = inputs.resolve(
                self.context.inputs.user_inputs,
                self.context.workflow.model.spec.global_inputs,
                self.context.workflow.pipeline.info().get("inputs", tuple()),
                self.context.workflow.pipeline.info().get("required_inputs", []),
            )

            self.context.outputs.results = self.context.workflow.pipeline.map(
                self.context.inputs.resolved_inputs
            )
            self.context.metadata.status = Status.SUCCESS.value

            self.context.outputs.persisted_outputs = outputs.persist_workflow_outputs(
                self.context.outputs.results,
                self.context.workflow.model.spec.pipeline_outputs,
                self.context.paths.output_dir,
            )
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during workflow run {self.context.metadata.run_id}: {e}"
            )
            if self.io.is_debug() or self.io.is_very_verbose():
                traceback.print_exc()
        finally:
            self.context.save_summary()

        return 0 if self.context.metadata.status == Status.SUCCESS else 1
