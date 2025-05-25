import logging

import pipefunc

from pipeflow.utils.python import import_callable
from pipeflow.workflow.exceptions import PipelineBuildError
from pipeflow.workflow.schema import StepModel

logger = logging.getLogger(__name__)


def from_model(step_model: StepModel) -> pipefunc.PipeFunc:
    """Create a `pipefunc.PipeFunc` from a `StepModel` instance."""
    if not step_model.function:  # Ensure function FQN is present
        raise ValueError(f"Function FQN is missing for step '{step_model.name}'.")

    _callable = import_callable(step_model.function)
    options = (
        step_model.options.model_dump(exclude_none=True) if step_model.options else {}
    )

    if step_model.parameters:
        if "defaults" not in options:
            options["defaults"] = {}
        options["defaults"].update(step_model.parameters)

    try:
        pf_func = pipefunc.PipeFunc(
            func=_callable,
            **options,
        )
    except Exception as e:
        raise PipelineBuildError(
            f"Failed to instantiate PipeFunc for '{step_model.function}' {step_model.name} "
            f"with func='{_callable.__name__}' and options={options}. Error: \n{e}"
        ) from e
    return pf_func
