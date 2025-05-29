import logging

import pipefunc

from flowfunc.utils.python import import_callable
from flowfunc.workflow.exceptions import PipelineBuildError
from flowfunc.workflow.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


def from_model(workflow: WorkflowDefinition, step_index: int) -> pipefunc.PipeFunc:
    """Create a `pipefunc.PipeFunc` from a `StepModel` instance."""
    step = workflow.spec.steps[step_index]
    final_options = step.options.model_dump(exclude_none=True) if step.options else {}

    resolve_function(
        final_options,
        step,
        workflow.spec.default_module,
    )
    resolve_input_renames(final_options, step)
    resolve_input_defaults(final_options, step)
    resolve_resources(
        final_options, workflow.spec.options.default_resources, step.resources
    )
    resolve_scope(final_options, step)
    try:
        return pipefunc.PipeFunc(**final_options)
    except Exception as e:
        raise PipelineBuildError(
            f"Failed to instantiate PipeFunc for step '{step.name}' "
            f"(function: '{final_options['func']}') with effective options: {final_options}. Error: {e}"
        ) from e


def resolve_scope(options, step) -> None:
    options["scope"] = step.options.scope


def resolve_resources(options, global_resources, step_resources) -> None:
    # merges the two resource groups, with the step resources take precedence over global ones for this particular step
    merged_resources = {**global_resources.model_dump(), **step_resources.model_dump()}
    # users can specify more options if they wish in the `advanced_options` field
    flattened_resources = {
        "cpus": merged_resources.get("cpus"),
        "memory": merged_resources.get("memory"),
        **(merged_resources.get("advanced_options") or {}),
    }
    options["resources"] = flattened_resources


def resolve_input_defaults(options, step) -> None:
    if step.parameters:
        if "defaults" not in options:
            options.defaults = {}
        for param_key, param_value in step.parameters.items():
            if param_key not in options.defaults:
                options.defaults[param_key] = param_value


def resolve_input_renames(options, step):
    renames = {}
    if step.inputs:
        for name, input_item in step.inputs.items():
            input_value = input_item
            # TODO: remove when pydantic coercion working
            if isinstance(input_item, dict):
                input_value = input_item.value
            if input_value.startswith("$global."):
                global_var_name = ".".join(input_value.split(".", 1)[1:])
                if name != global_var_name:
                    renames[name] = global_var_name
    if renames:
        if "renames" in options and isinstance(options.renames, dict):
            options.renames = {**options.renames, **renames}
        else:
            options.renames = renames
    return renames


def resolve_function(options, step, default_module) -> None:
    function_path = step.func
    if not function_path:
        if default_module and step.name:
            function_path = f"{default_module}.{step.name}"
            logger.debug(
                f"Step '{step.name}': 'function' not specified. Defaulting to '{function_path}' using default_module and step name."
            )
        else:
            error_msg_parts = []
            if not default_module:
                error_msg_parts.append(
                    "no 'default_module' is specified in the workflow"
                )
            if not step.name:
                error_msg_parts.append("'name' is missing for the step")

            detail = " and ".join(error_msg_parts)
            raise PipelineBuildError(
                f"Step '{step.name or '(unnamed step)'}': 'function' is not specified and cannot be defaulted because {detail}."
            )
    options["func"] = import_callable(function_path)
