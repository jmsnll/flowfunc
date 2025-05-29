# flowfunc/run/input_resolver.py

import logging
from typing import Any

from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class InputResolverError(Exception):
    """Custom exception for input resolution errors."""


class InputResolver:
    """
    Resolves the final set of inputs for the pipeline.
    Merges user inputs with global defaults and validates against required inputs.
    (Adapted from flowfunc.workflow.inputs.resolve)
    """

    def resolve(
        self,
        user_inputs: dict[str, Any],
        workflow_model: WorkflowDefinition,  # For global_inputs and scope
        pipeline_inputs: list[str],  # From pipeline.info().get("inputs", [])
        pipeline_required_inputs: list[
            str
        ],  # From pipeline.info().get("required_inputs", [])
    ) -> dict[str, Any]:
        """
        Resolves inputs for the pipeline.
        """
        logger.info("Resolving pipeline inputs.")
        global_inputs_spec = workflow_model.spec.inputs
        workflow_scope = (
            workflow_model.spec.options.scope if workflow_model.spec.options else None
        )

        resolved: dict[str, Any] = {}

        # 1. Apply global input defaults
        if global_inputs_spec:
            for name, spec in global_inputs_spec.items():
                scoped_name = f"{workflow_scope}.{name}" if workflow_scope else name
                value = spec.value if isinstance(spec.value, dict) else spec
                resolved[scoped_name] = value.model_dump()
                # Handle type validation/coercion for defaults if needed
                # For Pydantic models, this would usually happen at model_validate
                # If spec.type is available, you could use TypeAdapter(spec.type).validate_python(spec.default)

        # 2. Override with user inputs
        for name, value in user_inputs.items():
            # User inputs might already be scoped or not.
            # The original logic for `inputs_with_scope` needs to be determined.
            # For now, assume user_inputs keys are what pipeline expects.
            resolved[name] = value

        # 3. Validate against pipeline's required inputs
        missing_inputs = set(pipeline_required_inputs) - set(resolved.keys())
        if missing_inputs:
            raise InputResolverError(
                f"Missing required inputs for pipeline '{workflow_model.metadata.name}': {missing_inputs=} {set(resolved.keys())=}"
            )

        # 4. Filter to only include inputs the pipeline actually expects
        final_resolved_inputs = {
            k: v for k, v in resolved.items() if k in pipeline_inputs
        }

        # 5. Optional: Type validation for final inputs against global_inputs spec
        # if global_inputs_spec:
        #     for name, value in final_resolved_inputs.items():
        #         original_name = name[len(workflow_scope)+1:] if workflow_scope and name.startswith(workflow_scope + ".") else name
        #         if original_name in global_inputs_spec and global_inputs_spec[original_name].type_str:
        #             try:
        #                 # This requires actual type objects, not just strings for complex types
        #                 # This part of original logic needs careful reimplementation if types are complex
        #                 # type_ = resolve_type(global_inputs_spec[original_name].type_str) # pseudo-code
        #                 # TypeAdapter(type_).validate_python(value)
        #                 pass # Placeholder for actual type validation
        #             except Exception as e:
        #                 raise InputResolverError(f"Type validation failed for input '{name}': {e}")

        logger.info("Pipeline inputs resolved successfully.")
        logger.debug(f"Resolved inputs: {final_resolved_inputs}")
        return final_resolved_inputs
