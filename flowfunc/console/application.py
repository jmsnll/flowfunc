from __future__ import annotations

import logging
import traceback
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from cleo.application import Application as BaseApplication
from cleo.events.console_command_event import ConsoleCommandEvent
from cleo.events.console_events import COMMAND
from cleo.events.console_events import SIGNAL
from cleo.events.event_dispatcher import EventDispatcher
from cleo.formatters.style import Style

from flowfunc.__version__ import __version__
from flowfunc.console.command_loader import CommandLoader
from flowfunc.console.commands.command import Command
from flowfunc.console.logging.structlog import configure_structlog

if TYPE_CHECKING:
    from collections.abc import Callable

    from cleo.events.event import Event
    from cleo.io.inputs.input import Input
    from cleo.io.io import IO
    from cleo.io.outputs.output import Output


def load_command(name: str) -> Callable[[], Command]:
    def _load() -> Command:
        words = name.split(" ")
        module = import_module("flowfunc.console.commands." + ".".join(words))
        command_class = getattr(module, "".join(c.title() for c in words) + "Command")
        command: Command = command_class()
        return command

    return _load


COMMANDS = [
    "about",
    "debug",
    "describe",
    "docs",
    "generate schema",
    "graph",
    "init",
    "new",
    "run",
    "step",
    "validate",
]


class FlowFuncConsole(BaseApplication):
    def __init__(self):
        super().__init__("FlowFuncConsole", __version__)

        self._application: FlowFuncConsole | None = None
        self._io: IO | None = None
        self._working_directory: Path = Path.cwd()

        dispatcher = EventDispatcher()
        dispatcher.add_listener(COMMAND, self.register_command_loggers)
        dispatcher.add_listener(SIGNAL, self.handle_interrupt)
        self.set_event_dispatcher(dispatcher)

        command_loader = CommandLoader({name: load_command(name) for name in COMMANDS})

        self.set_command_loader(command_loader)

    @property
    def command_loader(self) -> CommandLoader:
        command_loader = self._command_loader
        assert isinstance(command_loader, CommandLoader)
        return command_loader

    def create_io(
        self,
        input: Input | None = None,
        output: Output | None = None,
        error_output: Output | None = None,
    ) -> IO:
        io = super().create_io(input, output, error_output)

        # Set our own CLI styles
        formatter = io.output.formatter
        formatter.set_style("c1", Style("cyan"))
        formatter.set_style("c2", Style("default", options=["bold"]))
        formatter.set_style("info", Style("blue"))
        formatter.set_style("comment", Style("green"))
        formatter.set_style("warning", Style("yellow"))
        formatter.set_style("debug", Style("default", options=["dark"]))
        formatter.set_style("success", Style("green"))

        # Dark variants
        formatter.set_style("c1_dark", Style("cyan", options=["dark"]))
        formatter.set_style("c2_dark", Style("default", options=["bold", "dark"]))
        formatter.set_style("success_dark", Style("green", options=["dark"]))

        io.output.set_formatter(formatter)
        io.error_output.set_formatter(formatter)

        self._io = io

        return io

    def _run(self, io: IO) -> int:
        """
        Runs the application.

        This is a critical place to initialize IO-dependent services like logging.
        """
        # Cleo's base run method creates/sets up the IO object.
        # We need to ensure our logging is configured *after* IO is ready
        # and *before* any commands (which might use logging) are executed.

        # Create or get the IO object using Cleo's internal mechanism
        # This ensures self.io is set for the application instance.
        current_io = self._io  # Store current IO if any
        if io is None:
            io = self._create_io()  # This also sets self._io
        else:
            self._io = io

        # Determine the desired log level based on Cleo's verbosity
        # This mapping can be adjusted to your preference.
        if io.is_debug():  # -vvv
            log_level_to_set = logging.DEBUG
        elif io.is_very_verbose():  # -vv
            log_level_to_set = (
                logging.DEBUG
            )  # Also DEBUG, or a custom level if you had one
        elif io.is_verbose():  # -v
            log_level_to_set = logging.INFO
        else:  # Normal output
            log_level_to_set = (
                logging.WARNING
            )  # Or logging.INFO if you want INFO by default

        # NOW call configure_structlog, passing the fully initialized IO object
        # and the determined log level.
        try:
            configure_structlog(
                log_level=log_level_to_set,
                io_for_console=io,  # Pass the actual IO object
            )
            # Log that configuration happened (this log will now use the new setup)
            structlog_logger = structlog.get_logger(
                "flowfunc.app"
            )  # Get a structlog logger
            structlog_logger.debug(
                "Structlog and standard logging configured.",
                log_level=logging.getLevelName(log_level_to_set),
            )

        except Exception as e:
            # Fallback if logging config fails, print to stderr
            import sys

            print(f"CRITICAL: Failed to configure logging: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            # Optionally, you might want to exit or continue with unconfigured logging

        # Call the original run method of the BaseApplication
        exit_code = super()._run(io)  # Pass the io object we ensured is set

        # Restore previous IO if it was changed (Cleo's run does this too)
        self._io = current_io
        return exit_code

    # def _configure_io(self, io: IO) -> None:
    #     # We need to check if the command being run
    #     # is the "run" command.
    #     definition = self.definition
    #     with suppress(CleoError):
    #         io.input.bind(definition)
    #
    #     super()._configure_io(io)

    def register_command_loggers(
        self, event: Event, event_name: str, _: EventDispatcher
    ) -> None:
        from flowfunc.console.logging.filters import FLOWFUNC_FILTER
        from flowfunc.console.logging.filters import PIPEFUNC_FILTER
        from flowfunc.console.logging.io_formatter import IOFormatter
        from flowfunc.console.logging.io_handler import IOHandler

        assert isinstance(event, ConsoleCommandEvent)
        command = event.command
        if not isinstance(command, Command):
            return

        io = event.io

        loggers = []
        loggers += command.loggers

        handler = IOHandler(io)
        handler.setFormatter(IOFormatter())

        level = logging.WARNING

        if io.is_debug():
            level = logging.DEBUG
        elif io.is_very_verbose() or io.is_verbose():
            level = logging.INFO

        logging.basicConfig(level=level, handlers=[handler])

        # only log third-party packages when very verbose
        if not io.is_very_verbose():
            handler.addFilter(FLOWFUNC_FILTER)
            handler.addFilter(PIPEFUNC_FILTER)

        for name in loggers:
            logger = logging.getLogger(name)
            logger.setLevel(level)

    def handle_interrupt(
        self, event: Event, event_name: str, _: EventDispatcher
    ) -> None:
        pass


def main() -> int:
    exit_code: int = FlowFuncConsole().run()
    return exit_code


if __name__ == "__main__":
    main()
