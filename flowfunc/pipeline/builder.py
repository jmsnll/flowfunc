from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from typing import Any

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

        rendering_context: dict[str, Any] = {
            "params": {name: name for name in workflow.spec.params},
            "steps": {},
        }

        if not workflow.spec.steps:
            raise PipelineBuildError(f"Workflow '{name}' contains no steps.")

        funcs = []
        for step in workflow.spec.steps:
            try:
                initial_options = (
                    step.options.model_dump(
                        exclude_none=True, exclude_unset=True, by_alias=True
                    )
                    if step.options
                    else {}
                )

                final_options = self._step_chain(
                    initial_options,
                    step=step,
                    workflow=workflow,
                    rendering_context=rendering_context,
                )

                if step_output_name := final_options.output_name:
                    outputs_map = {}
                    output_names = (
                        [step_output_name]
                        if isinstance(step_output_name, str)
                        else step_output_name
                    )
                    for out_name in output_names:
                        outputs_map[out_name] = f"{out_name}"

                    rendering_context["steps"][step.name] = {"produces": outputs_map}

                func_kwargs = final_options.model_dump(
                    exclude_unset=True, exclude_none=True
                )
                funcs.append(pipefunc.PipeFunc(**func_kwargs))

            except Exception as e:
                raise PipelineBuildError(f"Step '{step.name}' failed") from e

        try:
            pipeline_kwargs = self._pipeline_chain({}, workflow=workflow)
            logger.debug(
                f"Creating Pipeline for '{name}' with {len(funcs)} steps and kwargs: {pipeline_kwargs}"
            )
            return pipefunc.Pipeline(funcs, **pipeline_kwargs)
        except Exception as e:
            raise PipelineBuildError(f"Failed to build pipeline for '{name}'") from e
