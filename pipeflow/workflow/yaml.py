from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from pipeflow.workflow.exceptions import WorkflowLoadError
from pipeflow.workflow.exceptions import WorkflowSchemaValidationError
from pipeflow.workflow.schema import PipeflowPipelineModel


class WorkflowYAML:
    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        if not self._path.exists():
            raise FileNotFoundError(self._path.as_posix())
        self._data: dict[str, Any] | None = None
        self._model_instance: PipeflowPipelineModel | None = None

    @property
    def path(self) -> Path:
        return self._path

    @property
    def model(self) -> PipeflowPipelineModel:
        if self._model_instance is None:
            try:
                with self.path.open("rb") as f:
                    raw_data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                msg = (
                    f"{self._path.as_posix()} is not a valid YAML file.\n"
                    f"{e.__class__.__name__}: {e}"
                )

                raise WorkflowLoadError(msg) from e
            except Exception as e:
                raise WorkflowLoadError(
                    f"Could not read file '{self.path.as_posix()}': {e}"
                ) from e

            if raw_data is None:
                raise WorkflowLoadError(
                    f"Workflow file is empty or contains no parsable content: {self.path.as_posix()}"
                )

            if not isinstance(raw_data, dict):
                raise WorkflowLoadError(
                    f"Invalid YAML structure in '{self.path.as_posix()}': "
                    f"Top level must be a mapping (object), but found type '{type(raw_data).__name__}'."
                )

            try:
                self._model_instance = PipeflowPipelineModel.model_validate(raw_data)
            except ValidationError as e:
                error_details = e.errors()
                formatted_errors = "\n".join(
                    [
                        f"  - {err['loc']}: {err['msg']} (type: {err['type']})"
                        for err in error_details
                    ]
                )
                raise WorkflowSchemaValidationError(
                    f"Workflow YAML schema validation failed for '{self.path.as_posix()}':\n{formatted_errors}"
                ) from e

        return self._model_instance
