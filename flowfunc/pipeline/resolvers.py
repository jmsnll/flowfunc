import logging
from collections.abc import Callable
from typing import Any

from flowfunc.utils.python import import_callable
from flowfunc.workflow_definition.exceptions import PipelineBuildError
from flowfunc.workflow_definition.schema import StepDefinition
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


StepOptionResolverCallable = Callable[
    [dict[str, Any], StepDefinition, WorkflowDefinition], None
]
PipelineOptionResolverCallable = Callable[[dict[str, Any], WorkflowDefinition], None]


def resolve_function_path(
    options: dict[str, Any],
    step: StepDefinition,
    workflow: WorkflowDefinition,
) -> None:
    """
    Resolves and imports the callable function for the step.
    Mutates `options` to add the 'func' key.
    """
    default_module = workflow.spec.default_module
    function_path_str = step.func  # func is the string path from the schema

    if not function_path_str:
        if default_module and step.name:
            function_path_str = f"{default_module}.{step.name}"
            logger.debug(
                f"Step '{step.name}': 'func' not specified. Defaulting to '{function_path_str}' "
                f"using default_module and step name."
            )
        else:
            error_msg_parts = []
            if not default_module:
                error_msg_parts.append(
                    "no 'default_module' is specified in the workflow"
                )
            if not step.name:
                error_msg_parts.append("'name' is missing for the step")
            detail = (
                " and ".join(error_msg_parts)
                if error_msg_parts
                else "required information missing"
            )
            raise PipelineBuildError(
                f"Step '{step.name or '(unnamed step)'}': 'func' is not specified and "
                f"cannot be defaulted because {detail}."
            )

    try:
        # Ensure function_path_str is indeed a string before passing to import_callable
        if not isinstance(function_path_str, str):
            raise TypeError(
                f"'func' path must be a string, got {type(function_path_str)} for step '{step.name}'"
            )
        options["func"] = import_callable(function_path_str)
    except (ImportError, AttributeError, TypeError) as e:  # Added TypeError
        raise PipelineBuildError(
            f"Could not import callable '{function_path_str}' for step '{step.name}': {e}"
        ) from e


def resolve_input_renames(
    options: dict[str, Any],
    step: StepDefinition,
    workflow: WorkflowDefinition,
) -> None:
    """
    Resolves input renames, especially for $global references.
    Mutates `options` to add/update the 'renames' key.
    """
    renames: dict[str, str] = {}
    # Assuming step.inputs is Dict[str, InputItem] after Pydantic parsing
    # where InputItem has a 'value' field.
    if step.inputs:  # step.inputs from the schema
        for (
            name,
            input_item_model,
        ) in step.inputs.items():  # input_item_model is an InputItem instance
            input_value = input_item_model.value if isinstance(input_item_model, dict) else input_item_model
            if input_value.startswith("$global."):
                global_var_name = ".".join(input_value.split(".", 1)[1:])
                if name != global_var_name:
                    renames[name] = global_var_name

    if renames:
        if "renames" in options and isinstance(options.get("renames"), dict):
            options["renames"] = {**options["renames"], **renames}
        else:
            options["renames"] = renames


def resolve_input_defaults(
    options: dict[str, Any],
    step: StepDefinition,
    workflow: WorkflowDefinition,  # Added workflow for consistency
) -> None:
    """
    Resolves input defaults from step parameters.
    Mutates `options` to add/update the 'defaults' key.
    """
    # Assuming step.parameters is Dict[str, Any]
    if step.parameters:
        if "defaults" not in options or not isinstance(options.get("defaults"), dict):
            options["defaults"] = {}

        current_defaults = options["defaults"]  # type: ignore
        for param_key, param_value in step.parameters.items():
            # Only add if not already set (e.g., by step.options or a prior resolver)
            if param_key not in current_defaults:
                current_defaults[param_key] = param_value
        # No need to reassign options["defaults"] = current_defaults if mutating in place


def resolve_resources(
    options: dict[str, Any],
    step: StepDefinition,
    workflow: WorkflowDefinition,
) -> None:
    """
    Merges global and step-specific resources.
    Mutates `options` to add/update the 'resources' key.
    """
    global_resources_model = (
        workflow.spec.options.default_resources if workflow.spec.options else None
    )
    step_resources_model = step.resources

    global_res_dict = (
        global_resources_model.model_dump(exclude_none=True)
        if global_resources_model
        else {}
    )
    step_res_dict = (
        step_resources_model.model_dump(exclude_none=True)
        if step_resources_model
        else {}
    )

    merged_resources = {**global_res_dict, **step_res_dict}

    advanced_opts_dict = merged_resources.pop("advanced_options", {}) or {}

    flattened_resources: dict[str, Any] = {}
    if merged_resources.get("cpus") is not None:
        flattened_resources["cpus"] = merged_resources.get("cpus")
    if merged_resources.get("memory") is not None:
        flattened_resources["memory"] = merged_resources.get("memory")

    flattened_resources.update(advanced_opts_dict)

    if flattened_resources:
        options["resources"] = flattened_resources


def resolve_step_scope(  # Renamed for clarity from original _resolve_scope
    options: dict[str, Any],
    step: StepDefinition,
    workflow: WorkflowDefinition,  # Added workflow for consistency
) -> None:
    """
    Sets the scope for the step if defined in step options.
    Mutates `options` to add/update the 'scope' key.
    """
    if step.options and step.options.scope is not None:
        options["scope"] = step.options.scope
    # If step.options.scope is None, we don't set options["scope"],
    # allowing PipeFunc to use its default scope or inherit.
    # If an empty string "" is a valid scope and means "top-level",
    # and None means "inherit", then this logic is fine.
    # If None should explicitly clear a scope from step.options, that's different.


def resolve_general_pipeline_options(
    pipeline_kwargs: dict[str, Any],
    workflow: WorkflowDefinition,
) -> None:
    """
    Resolves general keyword arguments for the Pipeline constructor from workflow.spec.options.
    (e.g., lazy, debug, profile, cache_type, cache_kwargs, validate_type_annotations)
    """
    if workflow.spec.options:
        options_data = workflow.spec.options.model_dump(
            exclude_none=True, by_alias=True
        )

        valid_pipeline_constructor_args = {
            "lazy",
            "debug",
            "profile",
            "cache_type",
            "cache_kwargs",
            "validate_type_annotations",
        }
        for opt_name, opt_value in options_data.items():
            if opt_name in valid_pipeline_constructor_args:
                pipeline_kwargs[opt_name] = opt_value
        # Note: 'default_resources' and 'scope' are handled by their own resolvers for clarity or precedence.


def resolve_pipeline_global_scope(
    pipeline_kwargs: dict[str, Any],
    workflow: WorkflowDefinition,
) -> None:
    """
    Resolves the global 'scope' for the Pipeline constructor from workflow.spec.config.scope.
    This can be overridden by workflow.spec.options.scope if also handled by resolve_general_pipeline_options
    and that resolver runs later or has higher precedence logic.
    For now, this specifically sets from config.scope.
    """
    if workflow.spec.options and workflow.spec.options.scope is not None:
        # If you want options.scope to take precedence, you might check:
        # if "scope" not in pipeline_kwargs or workflow.spec.options.scope_overrides_options_scope:
        pipeline_kwargs["scope"] = workflow.spec.options.scope


def resolve_pipeline_default_resources(
    pipeline_kwargs: dict[str, Any],
    workflow: WorkflowDefinition,
) -> None:
    """
    Resolves the 'default_resources' for the Pipeline constructor
    from workflow.spec.options.default_resources.
    """
    if workflow.spec.options and workflow.spec.options.default_resources:
        # pipefunc.Pipeline expects default_resources to be dict or its Resources type.
        # Passing the Pydantic model instance directly if it's compatible,
        # or its model_dump(). pipefunc.Pipeline.Resources.maybe_from_dict will handle it.
        pipeline_kwargs["default_resources"] = (
            workflow.spec.options.default_resources.model_dump()
        )
