from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar

from cleo.commands.command import Command as BaseCommand
from cleo.exceptions import CleoValueError
from cleo.helpers import argument

from pipeflow.workflow import loader
from pipeflow.workflow import pipeline
from pipeflow.workflow.schema import PipeflowPipelineModel

if TYPE_CHECKING:
    import pipefunc
    from cleo.io.inputs.argument import Argument

    from pipeflow.app import Pipeflow
    from pipeflow.console.application import PipeflowConsole


class Command(BaseCommand):
    loggers: ClassVar[list[str]] = []

    _pipeflow: Pipeflow | None = None

    @property
    def pipeflow(self) -> Pipeflow:
        if self._pipeflow is None:
            return self.get_application().pipeflow

        return self._pipeflow

    def set_pipeflow(self, pipeflow: Pipeflow) -> None:
        self._pipeflow = pipeflow

    def get_application(self) -> PipeflowConsole:
        from pipeflow.console.application import PipeflowConsole

        application = self.application
        assert isinstance(application, PipeflowConsole)
        return application

    def reset_pipeflow(self) -> None:
        self.get_application().reset_pipeflow()

    def option(self, name: str, default: Any = None) -> Any:
        try:
            return super().option(name)
        except CleoValueError:
            return default


class WorkflowCommand(Command):
    arguments: ClassVar[list[Argument]] = [
        argument(
            "workflow",
            "The workflow file.",
        )
    ]

    _workflow: pipefunc.Pipeline | None = None
    _workflow_model: PipeflowPipelineModel | None = None

    @property
    def workflow(self) -> pipefunc.Pipeline | None:
        """The pipefunc.Pipeline instance generated from the workflow model."""
        if self._workflow is None:
            workflow_arg = self.argument("workflow")
            workload_path = Path(workflow_arg)
            if not workload_path.exists():
                self.io.line_error(
                    f"<error>Workflow file not found: {workload_path}</error>"
                )
                self._workflow_model = None  # Ensure model is also None
                return None
            try:
                loaded_model = loader.load_from_path(workload_path)
                self._workflow_model = loaded_model  # Store the model
                self._workflow = pipeline.from_model(loaded_model.spec)
            except Exception as e:
                self.io.line_error(
                    f"<error>Failed to load or build workflow '{workload_path}': {e}</error>"
                )
                if self.io.is_debug() or self.io.is_very_verbose():
                    import traceback

                    self.io.write_error_line(traceback.format_exc())
                self._workflow_model = None
                self._workflow = None
                return None
        return self._workflow

    @property
    def workflow_model(self) -> PipeflowPipelineModel | None:
        """The loaded and validated PipeflowPipelineModel."""
        if self._workflow_model is None:
            # Attempt to load the workflow, which will also populate _workflow_model
            if self.workflow is None and self._workflow_model is None:
                # Loading failed in the workflow property access, error will have been printed there.
                pass
        return self._workflow_model
