import functools
import logging
from collections.abc import Callable
from typing import TypeVar

from rich.console import Console as RichConsole
from rich.logging import RichHandler
from rich.traceback import install

from flowfunc.console.reporter import ConsoleReporter

console = RichConsole()
install(show_locals=True, width=200)
logging.basicConfig(
    level="DEBUG",
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(console=console, rich_tracebacks=True)],
)
logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., object])


def status(message: str, spinner: str = "dots") -> Callable[[F], F]:
    """Decorator to show a status spinner while executing a function."""

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with console.status(message, spinner=spinner):
                return func(*args, **kwargs)

        return wrapper  # type: ignore

    return decorator


__all__ = [
    "ConsoleReporter",
    "console",
    "status",
]
