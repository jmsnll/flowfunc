import pipefunc

from pipeflow.workflow import function


def new_from_yaml(definition: dict) -> pipefunc.Pipeline:
    functions = []
    for step in definition.get("steps", []):
        step = function.new_from_yaml(step)
        functions.append(step)
    return pipefunc.Pipeline(functions)
