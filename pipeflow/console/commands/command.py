from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from cleo.commands.command import Command as BaseCommand
from cleo.exceptions import CleoValueError

if TYPE_CHECKING:
    from pipeflow.console.application import PipeflowConsole
    from pipeflow.app import Pipeflow


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
