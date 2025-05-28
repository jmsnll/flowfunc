import pipefunc

from flowfunc.workflow import function
from flowfunc.workflow.schema import Workflow


def resolve_scope(spec) -> None:
    scope = spec.config.scope
    if scope:
        scoped_global_inputs = {
            f"{scope}.{name}": value for name, value in spec.global_inputs.items()
        }
        scoped_outputs = {
            f"{scope}.{name}": value for name, value in spec.outputs.items()
        }
        spec.global_inputs = scoped_global_inputs
        spec.outputs = scoped_outputs


def from_model(workflow: Workflow) -> pipefunc.Pipeline:
    functions = []
    for index, _ in enumerate(workflow.spec.steps):
        pf_function = function.from_model(workflow, step_index=index)
        functions.append(pf_function)

    pipeline_constructor_args = {}
    if workflow.spec.config:
        config_data = workflow.spec.config.model_dump(
            exclude_none=True, exclude_unset=True
        )
        for key, value in config_data.items():
            pipeline_constructor_args[key] = value

    resolve_scope(workflow.spec)

    return pipefunc.Pipeline(functions, **pipeline_constructor_args)
