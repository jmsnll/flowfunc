import logging
from typing import Any

from flowfunc.exceptions import InputResolverError
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class InputResolver:
    """
    Resolves the final set of inputs for the pipeline.
    Merges user inputs with global defaults and validates against required inputs.
    """

    def resolve(
        self,
        user_inputs: dict[str, Any],
        workflow_model: WorkflowDefinition,
        pipeline_inputs: list[str],
        pipeline_required_inputs: list[str],
    ) -> dict[str, Any]:
        """Resolves inputs for the pipeline."""
        logger.info("Resolving pipeline inputs.")
        global_inputs_spec = workflow_model.spec.inputs
        workflow_scope = (
            workflow_model.spec.options.scope if workflow_model.spec.options else None
        )

        resolved: dict[str, Any] = {}

        if global_inputs_spec:
            for name, spec in global_inputs_spec.items():
                scoped_name = f"{workflow_scope}.{name}" if workflow_scope else name
                value = spec.value if isinstance(spec.value, dict) else spec
                resolved[scoped_name] = value.model_dump()

        for name, value in user_inputs.items():
            resolved[name] = value

        missing_inputs = set(pipeline_required_inputs) - set(resolved.keys())
        if missing_inputs:
            raise InputResolverError(
                f"Missing required inputs for pipeline '{workflow_model.metadata.name}': {missing_inputs=} {set(resolved.keys())=}"
            )

        final_resolved_inputs = {
            k: v for k, v in resolved.items() if k in pipeline_inputs
        }

        logger.info("Pipeline inputs resolved successfully.")
        logger.debug(f"Resolved inputs: {final_resolved_inputs}")
        return final_resolved_inputs
