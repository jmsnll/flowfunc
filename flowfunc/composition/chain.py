from collections.abc import Callable
from collections.abc import Iterable
from functools import reduce
from typing import Any


class Chain:
    """
    A simple helper to chain a series of functions, where the output of one is the input of the next.
    """

    def __init__(self, funcs: Iterable[Callable]):
        self._funcs = funcs

    def __call__(self, initial_value: Any, *args, **kwargs) -> Any:
        """
        Executes the chain of functions.

        Args:
            initial_value: The starting value passed to the first function.
            *args: Constant positional arguments passed to *every* function in the chain.
            **kwargs: Constant keyword arguments passed to *every* function in the chain.
        """
        return reduce(
            lambda value, func: func(value, *args, **kwargs),
            self._funcs,
            initial_value,
        )
