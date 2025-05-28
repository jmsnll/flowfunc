import pipefunc

from flowfunc.workflow import function as workflow_function
from flowfunc.workflow.schema import Workflow


def from_model(workflow: Workflow) -> pipefunc.Pipeline:
    functions = []
    for index, _ in enumerate(workflow.spec.steps):
        pf_function = workflow_function.from_model(workflow, step_index=index)
        functions.append(pf_function)

    pipeline_constructor_args = {}
    if workflow.spec.config:
        config_data = workflow.spec.config.model_dump(exclude_none=True)
        for key, value in config_data.items():
            pipeline_constructor_args[key] = value

    return pipefunc.Pipeline(functions, **pipeline_constructor_args)
