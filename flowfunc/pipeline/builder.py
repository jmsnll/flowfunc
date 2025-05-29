from __future__ import annotations

import logging

import pipefunc

from flowfunc.exceptions import PipelineBuildError
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class PipelineBuilder:
    """
    Constructs a `pipefunc.Pipeline` object from a `WorkflowDefinition` model.
    It leverages methods within the schema models (`StepDefinition` and `WorkflowDefinition`)
    to generate the necessary options for `pipefunc.PipeFunc` and `pipefunc.Pipeline`.
    """

    def __init__(self):
        """Initializes the PipelineBuilder."""
        # This constructor is now very simple.
        # It could be used for future configurations related to the build process itself, if any.
        logger.debug("PipelineBuilder initialized.")

    def build(self, workflow_model: WorkflowDefinition) -> pipefunc.Pipeline:
        """
        Creates a pipefunc.Pipeline from the validated workflow model.
        """
        workflow_metadata_name = (
            workflow_model.metadata.name
        )  # Assuming this path is correct
        logger.info(f"Building pipeline for workflow: '{workflow_metadata_name}'")

        funcs: list[pipefunc.PipeFunc] = []

        # WorkflowSpec now validates that steps is not empty.
        # No need for an explicit check here if relying on Pydantic validation.

        for step_model in workflow_model.spec.steps:
            try:
                # Delegate option generation to the StepDefinition model itself.
                # The StepDefinition model needs access to parts of the overall workflow
                # (like default_module, global_resources) to correctly resolve all its options.
                pipe_func_options = step_model.to_pipefunc_options(
                    workflow_default_module=workflow_model.spec.default_module,
                    workflow_global_resources=(
                        workflow_model.spec.options.default_resources
                        if workflow_model.spec.options
                        else None
                    ),
                )

                logger.debug(
                    f"Instantiating PipeFunc for step '{step_model.name}' with resolved options: {pipe_func_options}"
                )

                # A sanity check, though to_pipefunc_options should guarantee 'func'
                if "func" not in pipe_func_options or not callable(
                    pipe_func_options["func"]
                ):
                    raise PipelineBuildError(
                        f"'func' was not correctly resolved to a callable by "
                        f"StepDefinition.to_pipefunc_options for step '{step_model.name}'."
                    )

                funcs.append(pipefunc.PipeFunc(**pipe_func_options))

            except (
                PipelineBuildError
            ):  # Re-raise errors from to_pipefunc_options or PipeFunc init
                raise
            except Exception as e:  # Catch other unexpected errors during this step
                step_name_for_error = step_model.name or "(unnamed step)"
                logger.error(
                    f"Unexpected error while creating PipeFunc for step '{step_name_for_error}': {e}",
                    exc_info=True,
                )
                raise PipelineBuildError(
                    f"Unexpected failure to create PipeFunc for step '{step_name_for_error}'. "
                    f"Original error: {type(e).__name__}: {e}"
                ) from e

        # Delegate pipeline constructor keyword argument generation to the WorkflowDefinition model
        pipeline_constructor_kwargs = workflow_model.get_pipeline_constructor_kwargs()

        try:
            logger.debug(
                f"Instantiating pipefunc.Pipeline for '{workflow_metadata_name}' with "
                f"{len(funcs)} funcs and kwargs: {pipeline_constructor_kwargs}"
            )
            # `funcs` is the first positional argument for pipefunc.Pipeline
            pipeline = pipefunc.Pipeline(funcs, **pipeline_constructor_kwargs)

            logger.info(
                f"Pipeline for workflow '{workflow_metadata_name}' built successfully."
            )
            return pipeline
        except Exception as e:
            logger.error(
                f"Error creating final pipefunc.Pipeline for '{workflow_metadata_name}': {e}",
                exc_info=True,
            )
            # Provide more context if it's a TypeError, which often happens with **kwargs issues
            if isinstance(e, TypeError):
                logger.error(
                    f"TypeError during pipefunc.Pipeline instantiation for '{workflow_metadata_name}'. "
                    f"Ensure pipeline_constructor_kwargs are valid for the Pipeline constructor. "
                    f"Kwargs passed: {pipeline_constructor_kwargs}. Error: {e}",
                    exc_info=False,  # Avoid double logging traceback if already done above
                )
            raise PipelineBuildError(
                f"Could not create pipefunc.Pipeline for workflow '{workflow_metadata_name}': {e}"
            ) from e
