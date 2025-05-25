import logging

import pipefunc

from pipeflow.workflow.callable import import_callable
from pipeflow.workflow.exceptions import CallableImportError
from pipeflow.workflow.exceptions import PipelineBuildError

logger = logging.getLogger(__name__)


def _prepare_constructor_kwargs(step_definition: dict) -> dict:
    constructor_args = {}
    options_yaml = step_definition.get("options", {})
    step_id_info = f"for step '{step_definition.get('id', 'N/A')}'"

    if not isinstance(options_yaml, dict):
        raise PipelineBuildError(
            f"'options' {step_id_info} must be an object, found {type(options_yaml).__name__}."
        )

    supported_pipefunc_constructor_args = {
        "output_name",
        "renames",
        "defaults",
        "profile",
        "debug",
        "cache",
        "mapspec",
        "scope",
    }
    for key, value in options_yaml.items():
        if key not in supported_pipefunc_constructor_args:
            logger.warning(
                f"Unknown key '{key}' in 'options' {step_id_info}. It will be ignored."
            )
            continue
        constructor_args[key] = value

    return constructor_args


def new_from_yaml(definition: dict) -> pipefunc.PipeFunc:
    """
    Validates the 'inputs' defined in the step definition against the
    signature of the specified Python function.

    Args:
        definition (dict): A dictionary representing a single step from the
                           pipeline definition YAML/JSON. Expected to contain
                           at least 'function' (FQN string) and optionally 'inputs' (dict).
                           It may also contain 'id' for better error messages.

    Returns:
        The imported callable if validation passes.

    Raises:
        PipelineBuildError: If function import fails or if the input keys
                            in the definition do not exactly match the
                            named parameters of the imported function.
    """
    step_id_info = f"in step '{definition.get('name', 'N/A')}'"
    function_fqn = definition.get("function")

    if not function_fqn:
        raise PipelineBuildError(f"Missing 'function' field {step_id_info}.")

    try:
        callable_obj = import_callable(function_fqn)
    except CallableImportError as e:
        raise PipelineBuildError(
            f"Failed to import function '{function_fqn}' {step_id_info}: {e}"
        ) from e

    spec_inputs_config = definition.get("options", None)
    if not isinstance(spec_inputs_config, dict):
        # This should ideally be caught by schema validation earlier
        raise PipelineBuildError(
            f"'options' field for function '{function_fqn}' {step_id_info} must be an object (dictionary), "
            f"but found type {type(spec_inputs_config).__name__}."
        )

    constructor_kwargs = _prepare_constructor_kwargs(definition)
    try:
        return pipefunc.PipeFunc(func=callable_obj, **constructor_kwargs)
    except Exception as e:
        raise PipelineBuildError(
            f"Failed to instantiate PipeFunc for '{function_fqn}' {step_id_info} "
            f"with func='{callable_obj.__name__}' and options={constructor_kwargs}. Error: \n{e}"
        ) from e
