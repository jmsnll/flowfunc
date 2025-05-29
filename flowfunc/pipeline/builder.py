# flowfunc/pipeline/builder.py

import logging
from typing import Any

import pipefunc

from flowfunc.core.exceptions import FlowFuncCoreError
from flowfunc.pipeline.resolvers import PipelineOptionResolverCallable
from flowfunc.pipeline.resolvers import StepOptionResolverCallable
from flowfunc.pipeline.resolvers import resolve_function_path
from flowfunc.pipeline.resolvers import resolve_general_pipeline_options
from flowfunc.pipeline.resolvers import resolve_input_defaults
from flowfunc.pipeline.resolvers import resolve_input_renames
from flowfunc.pipeline.resolvers import resolve_pipeline_default_resources
from flowfunc.pipeline.resolvers import resolve_pipeline_global_scope
from flowfunc.pipeline.resolvers import resolve_resources
from flowfunc.pipeline.resolvers import resolve_step_scope
from flowfunc.workflow_definition.exceptions import PipelineBuildError
from flowfunc.workflow_definition.schema import StepDefinition
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)

DEFAULT_STEP_RESOLVERS: list[StepOptionResolverCallable] = [
    resolve_function_path,
    resolve_input_renames,
    resolve_input_defaults,
    resolve_resources,
    resolve_step_scope,
]

DEFAULT_PIPELINE_OPTION_RESOLVERS: list[PipelineOptionResolverCallable] = [
    resolve_general_pipeline_options,
    resolve_pipeline_global_scope,
    resolve_pipeline_default_resources,
]


class PipelineBuilder:
    """
    Constructs a `pipefunc.Pipeline` object from a `WorkflowDefinition` model
    using lists of configurable resolvers for both step options and pipeline options.
    """

    def __init__(
        self,
        step_resolvers: list[StepOptionResolverCallable] | None = None,
        pipeline_option_resolvers: list[PipelineOptionResolverCallable] | None = None,
    ):
        self.step_resolvers = (
            step_resolvers if step_resolvers is not None else DEFAULT_STEP_RESOLVERS
        )
        self.pipeline_option_resolvers = (
            pipeline_option_resolvers
            if pipeline_option_resolvers is not None
            else DEFAULT_PIPELINE_OPTION_RESOLVERS
        )
        logger.debug(
            f"PipelineBuilder initialized with {len(self.step_resolvers)} step resolver(s) "
            f"and {len(self.pipeline_option_resolvers)} pipeline option resolver(s)."
        )

    def build(self, workflow_model: WorkflowDefinition) -> pipefunc.Pipeline:
        workflow_metadata_name = workflow_model.metadata.name
        logger.info(f"Building pipeline for workflow: {workflow_metadata_name}")

        funcs: list[pipefunc.PipeFunc] = []
        for step_model in workflow_model.spec.steps:
            try:
                pipe_func = self._create_pipe_func_from_step(step_model, workflow_model)
                funcs.append(pipe_func)
            except Exception as e:
                step_name_for_error = step_model.name or "(unnamed step)"
                original_func_path = step_model.func or (
                    f"{workflow_model.spec.default_module}.{step_model.name}"
                    if workflow_model.spec.default_module and step_model.name
                    else "Not specified"
                )
                logger.error(
                    f"Error creating PipeFunc for step '{step_name_for_error}' (declared function: '{original_func_path}'): {e}",
                    exc_info=True,
                )
                if not isinstance(e, PipelineBuildError):
                    raise PipelineBuildError(
                        f"Failed to create PipeFunc for step '{step_name_for_error}' "
                        f"(declared function: '{original_func_path}'). Error: {e}"
                    ) from e
                raise

        pipeline_kwargs: dict[str, Any] = {}
        for resolver in self.pipeline_option_resolvers:
            try:
                resolver(pipeline_kwargs, workflow_model)
            except Exception as e:
                raise PipelineBuildError(
                    f"Error applying pipeline option resolver '{resolver.__name__}' "
                    f"for workflow '{workflow_metadata_name}': {e}"
                ) from e

        try:
            # `funcs` is the first positional argument.
            # All other valid arguments are passed via **pipeline_kwargs.
            logger.debug(
                f"Instantiating pipefunc.Pipeline for '{workflow_metadata_name}' with "
                f"funcs count: {len(funcs)}, kwargs: {pipeline_kwargs}"
            )
            pipeline = pipefunc.Pipeline(funcs, **pipeline_kwargs)

            logger.info(
                f"Pipeline for workflow '{workflow_metadata_name}' built successfully."
            )
            return pipeline
        except Exception as e:
            logger.error(
                f"Error creating pipefunc.Pipeline for '{workflow_metadata_name}': {e}",
                exc_info=True,
            )
            # More detailed logging for type errors
            if isinstance(e, TypeError):
                logger.error(
                    f"TypeError during pipefunc.Pipeline instantiation for '{workflow_metadata_name}'. "
                    f"Ensure pipeline_kwargs are valid. Kwargs: {pipeline_kwargs}. Error: {e}",
                    exc_info=False,
                )  # exc_info=False as it's already logged above
            raise PipelineBuildError(
                f"Error creating pipefunc.Pipeline for '{workflow_metadata_name}': {e}"
            ) from e

    def _create_pipe_func_from_step(
        self,
        step: StepDefinition,
        workflow: WorkflowDefinition,
    ) -> pipefunc.PipeFunc:
        final_options: dict[str, Any] = {}
        if step.options:
            final_options = step.options.model_dump(exclude_none=True, by_alias=True)

        # TODO: move output to the step root like inputs and also support list/tuple for multiple outputs
        # if step.output_name:
        #     final_options.setdefault(
        #         "outputs", [out_item.target for out_item in step.outputs_spec.values()]
        #     )
        # if step.output_name:
        #     final_options.setdefault("output_name", step.output_name)
        # else:
        #     raise PipelineBuildError(
        #         "Step must have a name if 'outputs_spec' is not defined to default PipeFunc output."
        #     )

        for resolver in self.step_resolvers:
            try:
                resolver(final_options, step, workflow)
            except Exception as e:
                raise PipelineBuildError(
                    f"Error applying step option resolver '{resolver.__name__}' for step '{step.name}': {e}"
                ) from e

        try:
            logger.debug(
                f"Instantiating PipeFunc for step '{step.name}' with final options: {final_options}"
            )
            if "func" not in final_options:
                raise PipelineBuildError(
                    f"'func' not resolved for step '{step.name}'. Final options: {final_options}"
                )
            return pipefunc.PipeFunc(**final_options)
        except Exception as e:
            error_func_repr = final_options.get("func", step.func or "Unknown function")
            if isinstance(e, TypeError):
                logger.error(
                    f"TypeError during PipeFunc instantiation for step '{step.name}'. "
                    f"Function: '{error_func_repr}', Options: {final_options}. Error: {e}",
                    exc_info=True,
                )
            raise PipelineBuildError(
                f"Failed to instantiate PipeFunc for step '{step.name}' "
                f"(function: '{error_func_repr}') with effective options: {final_options}. Error: {e}"
            ) from e


class PipelineBuilderError(FlowFuncCoreError):
    pass
