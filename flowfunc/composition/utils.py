import importlib
from collections.abc import Callable
from typing import Any
import jinja2

from flowfunc.exceptions import CallableImportError
from flowfunc.workflow_definition.schema import WorkflowDefinition

_JINJA_ENV = jinja2.Environment(undefined=jinja2.StrictUndefined)
_JINJA_ENV.globals.update({"True": True, "False": False, "None": None})


def import_callable(fqn: str) -> Callable:
    """Imports a callable (function or class) given its fully qualified name."""
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


def build_jinja_rendering_context_for_outputs(
    workflow_model: WorkflowDefinition, pipeline_results: dict[str, Any]
) -> dict[str, Any]:
    """Builds the Jinja rendering context for artifact/output resolution."""
    context = {
        "steps": {},
        "params": workflow_model.spec.params or {},
    }
    
    scope = getattr(workflow_model.spec.options, "scope", None)
    
    for step in workflow_model.spec.steps:
        if step.produces:
            step_name = step.name
            output_names = (
                [step.produces] if isinstance(step.produces, str) else step.produces
            )
            if step_name not in context["steps"]:
                context["steps"][step_name] = {"produces": {}}
            for output_name in output_names:
                scoped = f"{scope}.{output_name}" if scope else output_name
                if scoped in pipeline_results:
                    context["steps"][step_name]["produces"][output_name] = scoped

    # Object-like access for steps
    class StepsContext:
        def __init__(self, steps_dict):
            self._steps = steps_dict

        def __getattr__(self, name):
            if name in self._steps:
                return self._steps[name]
            raise AttributeError(f"'StepsContext' object has no attribute '{name}'")

    print(context)

    context["steps"] = StepsContext(context["steps"])
    return context


def render_jinja_template(template_str: str, context: dict[str, Any]) -> str:
    """Renders a Jinja template string with the given context."""
    template = _JINJA_ENV.from_string(template_str)
    return template.render(context).strip()


def resolve_artifacts(
    workflow_model: WorkflowDefinition,
    pipeline_results: dict[str, Any],
) -> dict[str, Any]:
    """Resolves artifact definitions to their corresponding output data."""
    import re
    from flowfunc.workflow_definition.utils import is_jinja_template
    from flowfunc.exceptions import ArtifactPersistenceError

    DEPENDENCY_STRING_PATTERN = re.compile(
        r"^{{\s*steps\.([a-z0-9_]+)\.produces\.(\w+)\s*}}$"
    )
    artifacts = workflow_model.spec.artifacts
    if not artifacts:
        return {}

    resolved_artifacts = {}
    rendering_context = build_jinja_rendering_context_for_outputs(
        workflow_model, pipeline_results
    )

    for artifact_name, source_template in artifacts.items():
        try:
            if not is_jinja_template(source_template):
                # If not a Jinja template, treat as direct output name
                output_name = source_template
                if output_name in pipeline_results:
                    resolved_artifacts[artifact_name] = pipeline_results[
                        output_name
                    ].output
                    continue
                continue

            rendered_source = render_jinja_template(source_template, rendering_context)

            # If the rendered source is a direct output name, use it
            if rendered_source in pipeline_results:
                resolved_artifacts[artifact_name] = pipeline_results[
                    rendered_source
                ].output
                continue

            # Otherwise, try to match the dependency pattern
            match = DEPENDENCY_STRING_PATTERN.match(rendered_source)
            if match:
                step_name, output_name = match.groups()
                if output_name in pipeline_results:
                    resolved_artifacts[artifact_name] = pipeline_results[
                        output_name
                    ].output
                    continue
                continue

            raise ArtifactPersistenceError(
                f"Invalid artifact source format for '{artifact_name}': {rendered_source}"
            )

        except Exception as e:
            raise ArtifactPersistenceError(
                f"Error resolving artifact '{artifact_name}': {e}"
            ) from e

    return resolved_artifacts
