# flowfunc/workflow_definition/loader.py

import logging
from pathlib import Path
from typing import Any

import yaml  # Or your preferred YAML loader e.g., ruamel.yaml
from pydantic import ValidationError

from flowfunc.workflow_definition.schema import (
    WorkflowDefinition,  # Assuming schema.py is in the same directory
)

logger = logging.getLogger(__name__)


class WorkflowDefinitionLoaderError(Exception):
    """Custom exception for workflow definition loading errors."""


class WorkflowDefinitionLoader:
    """
    Loads and validates a workflow definition from a file or dictionary.
    """

    def from_path(self, file_path: Path) -> WorkflowDefinition:
        """
        Loads a workflow definition from a YAML or JSON file.
        """
        logger.info(f"Loading workflow definition from: {file_path}")
        if not file_path.exists():
            raise WorkflowDefinitionLoaderError(f"Workflow file not found: {file_path}")

        try:
            with file_path.open("r", encoding="utf-8") as f:
                if file_path.suffix in (".yaml", ".yml"):
                    data = yaml.safe_load(f)
                elif file_path.suffix == ".json":
                    import json

                    data = json.load(f)
                else:
                    raise WorkflowDefinitionLoaderError(
                        f"Unsupported file type: {file_path.suffix}. Must be .yaml, .yml, or .json"
                    )
        except yaml.YAMLError as e:
            raise WorkflowDefinitionLoaderError(
                f"Error parsing YAML file {file_path}: {e}"
            ) from e
        except Exception as e:
            raise WorkflowDefinitionLoaderError(
                f"Could not read or parse workflow file {file_path}: {e}"
            ) from e

        if not isinstance(data, dict):
            raise WorkflowDefinitionLoaderError(
                "Workflow definition must be a dictionary (JSON object)."
            )

        return self.from_dict(data, file_path=file_path)

    def from_dict(
        self, data: dict[str, Any], file_path: Path | None = None
    ) -> WorkflowDefinition:
        """
        Parses a dictionary into a Workflow model.
        """
        try:
            workflow_model = WorkflowDefinition.model_validate(data)
            logger.info(
                f"Workflow definition {'from ' + str(file_path) if file_path else ''} validated successfully."
            )
            return workflow_model
        except ValidationError as e:
            # Add more context to validation errors if possible
            error_details = e.errors()
            # You could format error_details for better readability here
            logger.error(f"Workflow validation failed: {error_details}")
            raise WorkflowDefinitionLoaderError(
                f"Workflow validation error: {e}"
            ) from e
