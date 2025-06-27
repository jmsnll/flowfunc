from __future__ import annotations

import logging

import pipefunc

from flowfunc.composition import pipeline as pipeline_resolvers
from flowfunc.composition import step as step_resolvers
from flowfunc.composition.chain import Chain
from flowfunc.exceptions import PipelineBuildError
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class PipelineBuilder:
    """
    Constructs a `pipefunc.Pipeline` from a `WorkflowDefinition` by applying
    chains of pure, stateless resolver functions from the `composition` package.
    """

    def __init__(self):
        """Initializes the builder by creating reusable chains of resolver functions."""
        self._step_builder_chain = Chain(step_resolvers.ALL)
        self._pipeline_builder_chain = Chain(pipeline_resolvers.ALL)

    def build(self, workflow_model: WorkflowDefinition) -> pipefunc.Pipeline:
        """Creates a pipefunc.Pipeline from the validated workflow model."""
        workflow_name = workflow_model.metadata.name
        logger.info(f"Building pipeline for workflow: '{workflow_name}'")

        if not workflow_model.spec.steps:
            raise PipelineBuildError(f"Workflow '{workflow_name}' contains no steps.")

        funcs: list[pipefunc.PipeFunc] = []
        for step_model in workflow_model.spec.steps:
            try:
                # Start with a clean slate: the raw options from the step definition
                initial_options = (
                    step_model.options.model_dump(exclude_none=True, by_alias=True)
                    if step_model.options
                    else {}
                )

                # Execute the chain of step resolvers to compose the final options
                pipe_func_options = self._step_builder_chain(
                    initial_options,
                    step=step_model,
                    workflow=workflow_model,
                )

                logger.debug(
                    f"Instantiating PipeFunc for step '{step_model.name}' with resolved options: {pipe_func_options}"
                )
                funcs.append(pipefunc.PipeFunc(**pipe_func_options))

            except PipelineBuildError as e:
                logger.error(f"Failed to build step '{step_model.name}': {e}")
                raise
            except Exception as e:
                logger.error(
                    f"An unexpected error occurred while building step '{step_model.name}': {e}",
                    exc_info=True,
                )
                raise PipelineBuildError(
                    f"Unexpected failure during construction of step '{step_model.name}'."
                ) from e

        # Use the pipeline chain to compose the final kwargs for the Pipeline constructor
        pipeline_kwargs = self._pipeline_builder_chain(
            initial_value={},  # Start with an empty dict
            workflow=workflow_model,
        )

        try:
            logger.debug(
                f"Instantiating pipefunc.Pipeline for '{workflow_name}' with {len(funcs)} funcs "
                f"and kwargs: {pipeline_kwargs}"
            )
            pipeline = pipefunc.Pipeline(funcs, **pipeline_kwargs)
            logger.info(f"Pipeline for workflow '{workflow_name}' built successfully.")
            return pipeline
        except Exception as e:
            logger.error(
                f"Failed to create final pipefunc.Pipeline for '{workflow_name}': {e}",
                exc_info=True,
            )
            raise PipelineBuildError(
                f"Could not create the final pipeline for workflow '{workflow_name}'."
            ) from e
