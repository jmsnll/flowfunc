from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar

from cleo.commands.command import Command as BaseCommand
from cleo.exceptions import CleoValueError
from cleo.helpers import argument

from flowfunc.pyproject.toml import PyProjectTOML
from flowfunc.workflow import loader
from flowfunc.workflow import pipeline

if TYPE_CHECKING:
    import pipefunc
    from cleo.io.inputs.argument import Argument

    from flowfunc.app import FlowFunc
    from flowfunc.console.application import FlowFuncConsole
    from flowfunc.workflow.schema import FlowFuncPipelineModel


class Command(BaseCommand):
    loggers: ClassVar[list[str]] = []

    @cached_property
    def flowfunc(self) -> FlowFunc:
        return self.get_application().flowfunc

    @cached_property
    def pyproject(self) -> PyProjectTOML:
        pyproject_path = Path.cwd() / "pyproject.toml"
        self._pyproject_toml = PyProjectTOML(pyproject_path)

    def set_flowfunc(self, flowfunc: FlowFunc) -> None:
        self._flowfunc = flowfunc

    def get_application(self) -> FlowFuncConsole:
        from flowfunc.console.application import FlowFuncConsole

        application = self.application
        assert isinstance(application, FlowFuncConsole)
        return application

    def reset_flowfunc(self) -> None:
        self.get_application().reset_flowfunc()

    def option(self, name: str, default: Any = None) -> Any:
        try:
            return super().option(name)
        except CleoValueError:
            return default

    def must_choose(
        self,
        question: str,
        choices: list[str],
        default: Any | None = None,
        attempts: int | None = None,
        multiple: bool = False,
    ):
        while not (
            choice := self.choice(question, choices, default, attempts, multiple)
        ):
            self.line("You must make a choice to continue.", style="error")
        return choice

    @property
    def no_interactive(self) -> bool:
        return self.option("no_interactive", False)


class WorkflowCommand(Command):
    arguments: ClassVar[list[Argument]] = [
        argument(
            "workflow",
            "The workflow file.",
        )
    ]

    _workflow_path: Path | None = None
    _workflow_model: FlowFuncPipelineModel | None = None

    @cached_property
    def workflow(self) -> pipefunc.Pipeline | None:
        """The pipefunc.Pipeline instance generated from the workflow model."""
        workflow_arg = self.argument("workflow")
        workload_path = Path(workflow_arg)
        if not workload_path.exists():
            self.io.write_error_line(
                f"<error>Workflow file not found: {workload_path}</error>"
            )
            self._workflow_model = None  # Ensure model is also None
            return None
        try:
            loaded_model = loader.load_from_path(workload_path)
            self._workflow_model = loaded_model  # Store the model
            self._workflow_path = workload_path
            return pipeline.from_model(loaded_model.spec)
        except Exception as e:
            raise RuntimeError(f"Failed to load workflow model: {e}") from e

    @property
    def workflow_model(self) -> FlowFuncPipelineModel | None:
        """The loaded and validated FlowFuncPipelineModel."""
        if self.workflow is None and self._workflow_model is None:
            pass
        return self._workflow_model

    @property
    def workflow_path(self) -> Path | None:
        """The path to the workflow file."""
        return self._workflow_path
