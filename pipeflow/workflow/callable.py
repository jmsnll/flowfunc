import importlib
from collections.abc import Callable

from pipeflow.workflow.exceptions import CallableImportError


def import_callable(fqn: str) -> Callable:
    """
    Imports a callable (function or class) given its fully qualified name.
    """
    if not isinstance(fqn, str) or "." not in fqn:
        raise CallableImportError(
            f"Invalid fully qualified name: '{fqn}'. Must be a dot-separated string."
        )
    try:
        module_name, object_name = fqn.rsplit(".", 1)
        module = importlib.import_module(module_name)
        callable_obj = getattr(module, object_name)
        if not callable(callable_obj):
            raise CallableImportError(f"The object '{fqn}' is not callable.")
        return callable_obj
    except (ImportError, AttributeError, ValueError) as e:
        raise CallableImportError(f"Could not import callable '{fqn}': {e}")
