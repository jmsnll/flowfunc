import inspect
import logging
from typing import Any

from flowfunc.exceptions import PipelineBuildError
from flowfunc.utils.python import import_callable
from flowfunc.workflow_definition import StepOptions
from flowfunc.workflow_definition.schema import StepDefinition
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


def create_initial_options(
    options: dict[str, Any], step: StepDefinition, **_
) -> StepOptions:
    """Creates the initial StepOptions model from the raw step options."""
    return StepOptions(**options)


def resolve_output_name(options: StepOptions, step: StepDefinition, **_) -> StepOptions:
    """Sets the 'output_name' from the step's name or outputs list."""
    if options.output_name:
        return options

    if step.outputs:
        output_name = step.outputs
    elif step.name:
        output_name = step.name
    else:
        raise PipelineBuildError("Step must have a 'name' or 'outputs'.")
    return options.model_copy(update={"output_name": output_name})


def resolve_callable(
    options: StepOptions, step: StepDefinition, workflow: WorkflowDefinition, **_
) -> StepOptions:
    """Resolves and imports the callable function for the step."""
    function_path_str = step.func or f"{workflow.spec.default_module}.{step.name}"
    try:
        resolved_func = import_callable(function_path_str)
        return options.model_copy(update={"func": resolved_func})
    except (ImportError, AttributeError, TypeError) as e:
        raise PipelineBuildError(
            f"Could not import callable '{function_path_str}' for step '{step.name}': {e}"
        ) from e


def validate_step_inputs(
    options: StepOptions, step: StepDefinition, workflow: WorkflowDefinition, **_
) -> StepOptions:
    """Validates that the step's inputs match the resolved function's signature."""
    if not callable(options.func):
        raise PipelineBuildError(
            "Cannot validate inputs: 'func' has not been resolved."
        )

    func_path = step.func or f"{workflow.spec.default_module}.{step.name}"
    try:
        signature = inspect.signature(options.func)
    except (ValueError, TypeError):
        logger.warning(
            f"Could not inspect signature for '{func_path}'. Skipping validation."
        )
        return options

    func_params = set(signature.parameters.keys())
    provided_args = set(step.inputs.keys()) | set(step.parameters.keys())
    unknown_args = provided_args - func_params
    if unknown_args:
        raise PipelineBuildError(
            f"Step '{step.name}': Unknown arguments: {sorted(list(unknown_args))}. Valid: {sorted(list(func_params))}."
        )

    missing_args = {
        name
        for name, param in signature.parameters.items()
        if param.default is inspect.Parameter.empty
        and name not in provided_args
        and name != "self"
    }
    if missing_args:
        raise PipelineBuildError(
            f"Step '{step.name}': Missing required arguments: {sorted(list(missing_args))}."
        )

    return options


def resolve_renames(options: StepOptions, step: StepDefinition, **_) -> StepOptions:
    """
    Resolves input renames for '$global' references using an explicit, readable loop.
    """
    if not step.inputs:
        return options

    new_renames: dict[str, str] = {}
    for name, input_item in step.inputs.items():
        input_value = input_item.value if hasattr(input_item, "value") else input_item

        if isinstance(input_value, str) and input_value.startswith("$global."):
            global_var_name = input_value.split(".", 1)[1]

            if name != global_var_name:
                new_renames[name] = global_var_name

    if not new_renames:
        return options

    final_renames = {**options.renames, **new_renames}
    return options.model_copy(update={"renames": final_renames})


def resolve_defaults(options: StepOptions, step: StepDefinition, **_) -> StepOptions:
    """Resolves input defaults from step parameters."""
    if not step.parameters:
        return options
    return options.model_copy(
        update={"defaults": {**options.defaults, **step.parameters}}
    )


def resolve_resources(
    options: StepOptions, step: StepDefinition, workflow: WorkflowDefinition, **_
) -> StepOptions:
    """Merges global and step-specific resources."""
    global_res = (
        workflow.spec.options.default_resources if workflow.spec.options else None
    )
    global_dict = global_res.model_dump(exclude_none=True) if global_res else {}
    step_dict = step.resources.model_dump(exclude_none=True) if step.resources else {}
    merged = {**global_dict, **step_dict}
    if not merged:
        return options
    return options.model_copy(update={"resources": {**options.resources, **merged}})


def resolve_scope(options: StepOptions, step: StepDefinition, **_) -> StepOptions:
    """Sets the 'scope' for the step from step options."""
    if step.options and step.options.scope is not None:
        return options.model_copy(update={"scope": step.options.scope})
    return options


# The final, ordered chain using the new strongly-typed model.
ALL = [
    create_initial_options,  # First, convert the raw dict to our Pydantic model
    resolve_output_name,
    resolve_callable,
    validate_step_inputs,
    resolve_renames,
    resolve_defaults,
    resolve_resources,
    resolve_scope,
]
