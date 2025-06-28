from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pipefunc

from flowfunc.composition import pipeline as pipeline_resolvers
from flowfunc.composition import step as step_resolvers
from flowfunc.composition.chain import Chain
from flowfunc.exceptions import PipelineBuildError

if TYPE_CHECKING:
    from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class PipelineBuilder:
    """
    Constructs a `pipefunc.Pipeline` from a `WorkflowDefinition` by applying
    chains of pure, stateless resolver functions from the `composition` package.
    """

    def __init__(self) -> None:
        self._step_chain = Chain(step_resolvers.ALL)
        self._pipeline_chain = Chain(pipeline_resolvers.ALL)

    def build(self, workflow: WorkflowDefinition) -> pipefunc.Pipeline:
        name = workflow.metadata.name
        logger.info(f"Building pipeline for workflow: '{name}'")

        if not workflow.spec.steps:
            raise PipelineBuildError(f"Workflow '{name}' contains no steps.")

        funcs = []
        for step in workflow.spec.steps:
            try:
                options = (
                    step.options.model_dump(
                        exclude_none=True, exclude_unset=True, by_alias=True
                    )
                    if step.options
                    else {}
                )
                resolved = self._step_chain(options, step=step, workflow=workflow)
                func_kwargs = resolved.model_dump(exclude_none=True, exclude_unset=True)
                logger.debug(
                    f"Creating PipeFunc for step '{step.name}' with: {func_kwargs}"
                )
                funcs.append(pipefunc.PipeFunc(**func_kwargs))

            except Exception as e:
                logger.exception(f"Error in step '{step.name}': {e}")
                raise PipelineBuildError(f"Step '{step.name}' failed") from e

        pipeline_kwargs = self._pipeline_chain({}, workflow=workflow)

        try:
            logger.debug(
                f"Creating Pipeline for '{name}' with {len(funcs)} steps and kwargs: {pipeline_kwargs}"
            )
            return pipefunc.Pipeline(funcs, **pipeline_kwargs)
        except Exception as e:
            logger.exception(f"Failed to build final pipeline for '{name}': {e}")
            raise PipelineBuildError(f"Failed to build pipeline for '{name}'") from e
