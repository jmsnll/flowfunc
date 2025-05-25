import pipefunc

from pipeflow.workflow.function import new_function_from_dict


def new_from_yaml(definition: dict) -> pipefunc.Pipeline:
    functions = []
    for step in definition.get("spec", {}).get("steps", []):
        step = new_function_from_dict(step)
        functions.append(step)
    return pipefunc.Pipeline(functions)
