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

    @property
    def workflow(self):
        if self._workflow is None:
            workload_path = Path(self.argument("workflow"))
            workflow_model = loader.load_from_path(workload_path)
            return pipeline.from_model(workflow_model.spec)
        return self._workflow
