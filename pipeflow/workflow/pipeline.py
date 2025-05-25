import pipefunc

from pipeflow.workflow import function as workflow_function
from pipeflow.workflow.schema import PipelineSpecModel


def from_model(spec: PipelineSpecModel) -> pipefunc.Pipeline:
    functions = []
    for step_model_instance in spec.steps:
        pf_function = workflow_function.from_model(step_model_instance)
        functions.append(pf_function)

    pipeline_constructor_args = {}
    if spec.pipeline_config:
        config_data = spec.pipeline_config.model_dump(exclude_none=True)
        for key, value in config_data.items():
            pipeline_constructor_args[key] = value

    return pipefunc.Pipeline(functions, **pipeline_constructor_args)
