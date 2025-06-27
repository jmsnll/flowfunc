from typing import Any

from flowfunc.workflow_definition.schema import WorkflowDefinition


def resolve_direct_kwargs(
    options: dict[str, Any], workflow: WorkflowDefinition, **_
) -> dict[str, Any]:
    if not workflow.spec.options:
        return options

    valid_direct_args = {
        "lazy",
        "debug",
        "profile",
        "cache_type",
        "cache_kwargs",
        "validate_type_annotations",
    }

    pipeline_options = workflow.spec.options.model_dump(exclude_none=True)
    new_kwargs = {
        key: value
        for key, value in pipeline_options.items()
        if key in valid_direct_args
    }

    return {**options, **new_kwargs}


def resolve_default_resources(
    options: dict[str, Any], workflow: WorkflowDefinition, **_
) -> dict[str, Any]:
    if workflow.spec.options and workflow.spec.options.default_resources:
        return {**options, "default_resources": workflow.spec.options.default_resources}
    return options


def resolve_scope(
    options: dict[str, Any], workflow: WorkflowDefinition, **_
) -> dict[str, Any]:
    if workflow.spec.options and workflow.spec.options.scope is not None:
        return {**options, "scope": workflow.spec.options.scope}
    return options


ALL = [
    resolve_direct_kwargs,
    resolve_default_resources,
    resolve_scope,
]
