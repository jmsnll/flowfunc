import functools
from collections.abc import Callable
from typing import TypeVar

from rich.console import Console as RichConsole

console = RichConsole()

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
