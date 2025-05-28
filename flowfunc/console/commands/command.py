from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar

import tomlkit.container
from cleo.commands.command import Command as BaseCommand
from cleo.exceptions import CleoValueError
from cleo.helpers import argument

from flowfunc import locations
from flowfunc.pyproject.toml import PyProjectTOML
from flowfunc.workflow import context
from flowfunc.workflow import loader
from flowfunc.workflow import pipeline

if TYPE_CHECKING:
    from flowfunc.app import FlowFunc
    from flowfunc.console.application import FlowFuncConsole


class Command(BaseCommand):
    loggers: ClassVar[list[str]] = []

    @cached_property
    def flowfunc(self) -> FlowFunc:
        return self.get_application().flowfunc

    @cached_property
    def pyproject(self) -> PyProjectTOML:
        return PyProjectTOML(locations.project_root() / "pyproject.toml")

    @property
    def toml_config(self) -> tomlkit.container.Container:
        return self.pyproject.data.get("tool").get("flowfunc", {})

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
    _ctx: context.RunContext | None = context.RunContext()

    @property
    def context(self) -> context.RunContext:
        return self._ctx

    @staticmethod
    def _group_arguments():
        return [
            argument("workflow"),
        ]

    def load_workflow(self):
        workflow_path = Path(self.argument("workflow"))

        if not workflow_path.exists():
            raise FileNotFoundError(f"Workflow file not found: {workflow_path}")

        self.context.workflow.file_path = workflow_path
        self.context.workflow.model = loader.load_from_path(workflow_path)
        self.context.workflow.pipeline = pipeline.from_model(
            loader.load_from_path(workflow_path).spec
        )
