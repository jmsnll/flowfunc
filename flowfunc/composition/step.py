import inspect
import logging
import re
import string
from typing import Any

import jinja2

from flowfunc.composition.utils import import_callable
from flowfunc.exceptions import PipelineBuildError
from flowfunc.workflow_definition import StepOptions
from flowfunc.workflow_definition.schema import MapMode
from flowfunc.workflow_definition.schema import StepDefinition
from flowfunc.workflow_definition.schema import WorkflowDefinition
from flowfunc.workflow_definition.utils import is_direct_jinja_reference
from flowfunc.workflow_definition.utils import is_jinja_template

logger = logging.getLogger(__name__)

_JINJA_ENV = jinja2.Environment(undefined=jinja2.StrictUndefined)
_JINJA_ENV.globals.update({"True": True, "False": False, "None": None})


def create_initial_options(options: dict[str, Any], **_) -> StepOptions:
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


def resolve_inputs(
    options: StepOptions,
    step: StepDefinition,
    rendering_context: dict[str, Any],
    **_,
) -> StepOptions:
    """
    Parses Jinja2 templates in a step's `inputs` block to populate pipefunc's
    `renames` and `defaults` options.

    - A direct reference `{{ steps.step_one.outputs.raw_data }}` becomes a `rename`.
    - An interpolated string `path/{{ globals.run_id }}` or literal becomes a `default`.
    - Per tests, an interpolated string is also rendered and added to `renames`.
    """
    if not step.inputs:
        return options

    new_renames: dict[str, Any] = {}
    new_defaults: dict[str, Any] = {}

    for name, input_item in step.inputs.items():
        try:
            input_value = (
                input_item.value if hasattr(input_item, "value") else input_item
            )

            if not is_jinja_template(input_value):
                new_defaults[name] = input_value
                continue

            template = _JINJA_ENV.from_string(input_value)
            render = template.render(rendering_context)
            new_renames[name] = render

            if not is_direct_jinja_reference(input_value):
                new_defaults[name] = input_value

        except jinja2.UndefinedError as e:
            raise PipelineBuildError(
                f"In step '{step.name}', input '{name}' has an invalid reference. "
                f"The expression '{input_item}' references an unknown variable: {e}"
            ) from e
        except Exception as e:
            raise PipelineBuildError(
                f"In step '{step.name}', a critical error occurred while rendering input '{name}' "
                f"with template '{input_item}'. Details: {e}"
            ) from e

    updated_renames = {**(options.renames or {}), **new_renames}
    updated_defaults = {**(options.defaults or {}), **new_defaults}

    return options.model_copy(
        update={
            "renames": updated_renames,
            "defaults": updated_defaults,
        }
    )


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
            f"Step '{step.name}': Unknown arguments: {sorted(unknown_args)}. Valid: {sorted(func_params)}."
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
            f"Step '{step.name}': Missing required arguments: {sorted(missing_args)}."
        )

    return options


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


def resolve_mapspec(options: StepOptions, step: StepDefinition, **_) -> StepOptions:
    """
    Generates and adds a `mapspec` string to the options based on step inputs and map_mode.

    This resolver makes it easier to handle common mapping patterns (broadcasting,
    zipping, aggregation) without writing a full `mapspec` string manually.
    """
    if options.mapspec:
        logger.debug(
            f"Adding mapspec from existing options for step '{step.name}': '{options.mapspec}'"
        )
        return options

    if (map_mode := step.options.map_mode) == MapMode.NONE:
        return options

    iterable_inputs = options.renames or {}
    constant_inputs = list((options.defaults or {}).keys())
    constant_inputs = [c for c in constant_inputs if c not in iterable_inputs]

    if not iterable_inputs:
        logger.debug(
            f"No mapspec generated, no iterable inputs detected for step '{step.name}'"
        )
        return options

    indices = list(string.ascii_lowercase[8:])
    input_parts = []
    output_names = (
        [options.output_name]
        if isinstance(options.output_name, str)
        else options.output_name
    )

    match map_mode:
        case MapMode.BROADCAST:
            output_indices = []
            for i, source_name in enumerate(iterable_inputs.values()):
                if i >= len(indices):
                    raise PipelineBuildError(
                        f"Step '{step.name}': Too many iterable inputs for 'broadcast' mode (max {len(indices)})."
                    )
                index = indices[i]
                input_parts.append(f"{source_name}[{index}]")
                output_indices.append(index)

            output_index_str = ",".join(output_indices)
            output_parts = [f"{name}[{output_index_str}]" for name in output_names]
            outputs_str = ", ".join(output_parts)

        case MapMode.ZIP:
            if len(iterable_inputs) > 1:
                logger.warning(
                    f"Step '{step.name}': 'zip' mode with multiple iterable inputs will map all to the same index 'i'."
                )
            index = indices[0]
            for source_name in iterable_inputs.values():
                input_parts.append(f"{source_name}[{index}]")
            output_parts = [f"{name}[{index}]" for name in output_names]
            outputs_str = ", ".join(output_parts)

        case MapMode.AGGREGATE:
            if len(iterable_inputs) > 1:
                raise PipelineBuildError(
                    f"Step '{step.name}': 'aggregate' mode only supports one iterable input."
                )
            index = indices[0]
            for source_name in iterable_inputs.values():
                input_parts.append(f"{source_name}[{index}]")
            outputs_str = ", ".join(output_names)

        case _:
            raise ValueError(f"Unhandled MapMode: {map_mode}. This should not happen.")

    input_parts.extend(constant_inputs)
    inputs_str = ", ".join(input_parts)
    mapspec_string = f"{inputs_str} -> {outputs_str}"

    logger.info(
        f"Step '{step.name}': Automatically generated mapspec "
        f"using mode '{map_mode.value}': '{mapspec_string}'"
    )

    return options.model_copy(update={"mapspec": mapspec_string})


ALL = [
    create_initial_options,
    resolve_output_name,
    resolve_callable,
    resolve_inputs,
    validate_step_inputs,
    resolve_defaults,
    resolve_resources,
    resolve_scope,
    resolve_mapspec,
]
