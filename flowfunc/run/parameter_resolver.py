import logging
from typing import Any

from flowfunc.exceptions import InputResolverError
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class ParameterResolver:
    """Merges user parameters with global defaults, validates required pipeline inputs."""

    def resolve(
        self,
        user_params: dict[str, Any],
        workflow_model: WorkflowDefinition,
        pipeline_inputs: list[str],
        pipeline_required_inputs: list[str],
    ) -> dict[str, Any]:
        logger.info("Resolving pipeline params.")

        params = workflow_model.spec.params or {}
        scope = getattr(workflow_model.spec.options, "scope", None)
        resolved: dict[str, Any] = {}

        for name, value in params.items():
            scoped = f"{scope}.{name}" if scope else name
            resolved[scoped] = value.get("value")

        resolved.update(user_params)

        missing = set(pipeline_required_inputs) - resolved.keys()
        if missing:
            raise InputResolverError(
                f"Missing required parameters for pipeline '{workflow_model.metadata.name}': missing={missing}, provided={set(resolved.keys())}"
            )

        final = {k: v for k, v in resolved.items() if k in pipeline_inputs}

        logger.info("Pipeline parameters resolved.")
        logger.debug(f"Final resolved parameters: {final}")
        return final
