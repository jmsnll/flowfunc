import logging
from typing import Any

from flowfunc.exceptions import InputResolverError
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class InputResolver:
    """Merges user inputs with global defaults, validates required pipeline inputs."""

    def resolve(
        self,
        user_inputs: dict[str, Any],
        workflow_model: WorkflowDefinition,
        pipeline_inputs: list[str],
        pipeline_required_inputs: list[str],
    ) -> dict[str, Any]:
        logger.info("Resolving pipeline inputs.")

        spec_inputs = workflow_model.spec.inputs or {}
        scope = getattr(workflow_model.spec.options, "scope", None)
        resolved: dict[str, Any] = {}

        for name, spec in spec_inputs.items():
            scoped = f"{scope}.{name}" if scope else name
            value = spec.value if isinstance(spec.value, dict) else spec
            resolved[scoped] = value.model_dump()

        resolved.update(user_inputs)

        missing = set(pipeline_required_inputs) - resolved.keys()
        if missing:
            raise InputResolverError(
                f"Missing required inputs for pipeline '{workflow_model.metadata.name}': missing={missing}, provided={set(resolved.keys())}"
            )

        final = {k: v for k, v in resolved.items() if k in pipeline_inputs}

        logger.info("Pipeline inputs resolved.")
        logger.debug(f"Final resolved inputs: {final}")
        return final
