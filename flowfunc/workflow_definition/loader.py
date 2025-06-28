import json
import logging
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from flowfunc.exceptions import WorkflowDefinitionLoaderError
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class WorkflowDefinitionLoader:
    """Loads and validates a workflow definition from a file or dict."""

    def from_path(self, file_path: Path) -> WorkflowDefinition:
        """Loads a workflow definition from a .yaml/.yml or .json file."""
        logger.info(f"Loading workflow definition from: {file_path}")

        if not file_path.exists():
            raise WorkflowDefinitionLoaderError(f"File not found: {file_path}")

        try:
            with file_path.open(encoding="utf-8") as f:
                match file_path.suffix:
                    case ".yaml" | ".yml":
                        data = yaml.safe_load(f)
                    case ".json":
                        data = json.load(f)
                    case ext:
                        raise WorkflowDefinitionLoaderError(
                            f"Unsupported file type: {ext}. Must be .yaml, .yml, or .json"
                        )
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise WorkflowDefinitionLoaderError(
                f"Failed to parse workflow file {file_path}: {e}"
            ) from e
        except Exception as e:
            raise WorkflowDefinitionLoaderError(
                f"Error reading workflow file {file_path}: {e}"
            ) from e

        if not isinstance(data, dict):
            raise WorkflowDefinitionLoaderError(
                f"Workflow file must deserialize to a dictionary. Got: {type(data)}"
            )

        return self.from_dict(data, file_path)

    def from_dict(
        self, data: dict[str, Any], file_path: Path | None = None
    ) -> WorkflowDefinition:
        """Parses raw dict into WorkflowDefinition."""
        try:
            model = WorkflowDefinition.model_validate(data)
            source = f" from {file_path}" if file_path else ""
            logger.info(f"Workflow definition{source} validated successfully.")
            return model
        except ValidationError as e:
            logger.exception("Workflow validation failed.")
            raise WorkflowDefinitionLoaderError(
                f"Workflow validation error: {e}"
            ) from e
