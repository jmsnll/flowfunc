# flowfunc/console/reporter.py (More Robust)

import contextlib
import logging
from collections.abc import Iterator

from rich.console import Console  # Import the Console type for type hinting
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

# Import Summary for type hinting if it's used in methods
from flowfunc.run.summary_model import Summary

logger = logging.getLogger(__name__)


class ConsoleReporter:
    """
    Handles displaying information to the console (status, tables, summaries).
    """

    def __init__(self, rich_console: Console | None = None, verbose: bool = False):
        if rich_console is None:
            # If no console is provided, import and use the global one from flowfunc.console
            # This import happens at the time of instantiation, not module load.
            from flowfunc.console import console as global_flowfunc_console

            self.console = global_flowfunc_console
        else:
            self.console = rich_console
        self.verbose = verbose

    @contextlib.contextmanager
    def status(self, message: str, ephemeral: bool = True) -> Iterator[None]:
        """
        Displays a status message using rich.status.
        The `verbose` attribute of the reporter instance controls behavior.
        """
        # If not verbose, and you want ephemeral statuses to be truly hidden,
        # you might adjust logic here. Original @status had complex verbosity.
        # This version shows status spinner if self.verbose is True OR if not ephemeral.
        if not self.verbose and ephemeral:
            yield  # Do nothing, but maintain context manager structure
            return

        try:
            # The spinner argument is part of console.status in newer Rich versions
            with self.console.status(message, spinner="dots"):
                yield
        finally:
            pass  # Status clears automatically on context exit

    # ... (rest of your ConsoleReporter methods: display_outputs_table, display_run_summary_panel, etc.)
    # Ensure these methods use `self.console`
    def display_outputs_table(self, persisted_outputs: dict[str, str]) -> None:
        if not persisted_outputs:
            self.console.print("[italic]No outputs were persisted.[/italic]")
            return
        # ... (table creation using self.console) ...
        table = Table(title="Outputs", show_header=True, header_style="bold magenta")
        table.add_column("Output Key", style="dim cyan", width=30)
        table.add_column("Path", overflow="fold")
        for key, path_str in persisted_outputs.items():
            table.add_row(key, path_str)
        self.console.print(table)

    def display_run_summary_panel(self, summary: Summary) -> None:
        if not summary:
            return
        # ... (panel creation using self.console) ...
        panel_title = f"📝 Run Summary: {summary.status.value.upper()}"
        border_style = (
            "green" if summary.status.value == "success" else "red"
        )  # Use .value for Enum comparison
        content = Text()
        content.append(f"Run ID: {summary.run_id}\n")
        content.append(f"Workflow: {summary.workflow_name}\n")
        if summary.workflow_file:
            content.append(f"File: {summary.workflow_file!s}\n")
        content.append(f"Status: {summary.status.value}\n")
        if summary.start_time:  # Check if start_time is not None
            content.append(
                f"Started: {summary.start_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            )
        if summary.end_time:
            content.append(
                f"Finished: {summary.end_time.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            )
        if summary.duration_seconds is not None:
            content.append(f"Duration: {summary.duration_seconds:.2f}s\n")
        if summary.run_dir:  # Check if run_dir is not None
            content.append(f"Run Directory: {summary.run_dir!s}\n")
        if summary.error_message:
            content.append(f"Error: {summary.error_message}\n", style="bold red")
        self.console.print(
            Panel(content, title=panel_title, border_style=border_style, expand=False)
        )

    def log_info(self, message: str) -> None:
        logger.info(message)

    def log_error(
        self, message: str, exc: Exception | None = None
    ) -> None:  # exc should be Optional
        logger.error(message, exc_info=exc if exc else False)

    def print_welcome_message(self) -> None:
        self.console.print("[bold cyan]FlowFunc Workflow Execution[/bold cyan]")
