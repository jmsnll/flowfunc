import pipefunc

from pipeflow.utils.python import import_callable
from pipeflow.workflow.exceptions import PipelineBuildError
from pipeflow.workflow.schema import StepModel


def from_model(step_model: StepModel) -> pipefunc.PipeFunc:
    """Create a `pipefunc.PipeFunc` from a `StepModel` instance."""
    if not step_model.function:
        raise ValueError(f"Function FQN is missing for step '{step_model.name}'.")

    _callable = import_callable(step_model.function)

    current_options = (
        step_model.options.model_dump(exclude_none=True) if step_model.options else {}
    )

    input_renames = {}
    if step_model.inputs:
        for func_arg_name, pipeline_source_name in step_model.inputs.items():
            if pipeline_source_name.startswith("$global."):
                global_var_name = pipeline_source_name.split(".", 1)[1]
                if func_arg_name != global_var_name:
                    input_renames[func_arg_name] = global_var_name

    if input_renames:
        if "renames" in current_options and isinstance(
            current_options["renames"], dict
        ):
            current_options["renames"] = {**current_options["renames"], **input_renames}
        else:
            current_options["renames"] = input_renames

    if step_model.parameters:
        if "defaults" not in current_options:
            current_options["defaults"] = {}
        for param_key, param_value in step_model.parameters.items():
            if param_key not in current_options["defaults"]:
                current_options["defaults"][param_key] = param_value

    try:
        pf_func = pipefunc.PipeFunc(
            func=_callable,
            **current_options,  # Pass the fully prepared options
        )
    except Exception as e:
        raise PipelineBuildError(
            f"Failed to instantiate PipeFunc for step '{step_model.name}' "
            f"(function: '{step_model.function}') with effective options: {current_options}. Error: {e}"
        ) from e
    return pf_func
