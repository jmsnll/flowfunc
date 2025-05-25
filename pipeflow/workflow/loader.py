from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from pipeflow.core.exceptions import PipeflowCoreError
from pipeflow.workflow.exceptions import WorkflowLoadError
from pipeflow.workflow.exceptions import WorkflowSchemaValidationError
from pipeflow.workflow.schema import PipeflowPipelineModel


def load_from_dict(raw_data: dict) -> PipeflowPipelineModel:
    if raw_data is None:
        raise WorkflowLoadError(
            "Workflow file is empty or contains no parsable content."
        )

    if not isinstance(raw_data, dict):
        raise WorkflowLoadError(
            f"Invalid YAML structure: "
            f"Top level must be a mapping (object), but found type '{type(raw_data).__name__}'."
        )

    try:
        return PipeflowPipelineModel.model_validate(raw_data)
    except ValidationError as e:
        error_details = e.errors()
        formatted_errors = "\n".join(
            [
                f"  - {err['loc']}: {err['msg']} (type: {err['type']})"
                for err in error_details
            ]
        )
        raise WorkflowSchemaValidationError(
            f"Workflow YAML schema validation failed:\n{formatted_errors}"
        ) from e


def load_from_path(path: Path) -> PipeflowPipelineModel:
    try:
        with path.open("rb") as f:
            raw_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        msg = (
            f"{path.as_posix()} is not a valid YAML file.\n{e.__class__.__name__}: {e}"
        )
        raise WorkflowLoadError(msg) from e
    except Exception as e:
        raise WorkflowLoadError(f"Could not read file '{path.as_posix()}': {e}") from e

    try:
        return load_from_dict(raw_data)
    except PipeflowCoreError as e:
        raise WorkflowLoadError(f"Failed to load '{path.as_posix()}': {e}") from e
