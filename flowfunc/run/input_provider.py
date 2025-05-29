# flowfunc/run/input_provider.py

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class InputProviderError(Exception):
    """Custom exception for input providing errors."""


class InputProvider:
    """
    Loads user-provided inputs from various sources (file, JSON string).
    """

    def load_from_file(self, file_path: Path) -> dict[str, Any]:
        """
        Loads inputs from a JSON file.
        (Adapted from flowfunc.workflow.inputs.from_file)
        """
        logger.info(f"Loading inputs from file: {file_path}")

        if not file_path.exists() or not file_path.is_file():
            raise InputProviderError(
                f"Input file not found or not a valid file: {file_path}"
            )

        try:
            with file_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise InputProviderError(
                f"Invalid JSON in input file {file_path}: {e}"
            ) from e
        except OSError as e:
            raise InputProviderError(
                f"Could not read input file {file_path}: {e}"
            ) from e

        if not isinstance(data, dict):
            raise InputProviderError(
                f"Inputs from file {file_path} must be a JSON object (dictionary)."
            )

        logger.info(f"Successfully loaded inputs from file {file_path}.")
        return data

    def load_from_json_string(self, json_string: str) -> dict[str, Any]:
        """
        Loads inputs from a JSON string.
        (Adapted from flowfunc.workflow.inputs.from_json)
        """
        logger.info("Loading inputs from JSON string.")
        if not json_string:
            logger.warning(
                "Empty JSON string provided for inputs. Returning empty dictionary."
            )
            return {}
        try:
            data = json.loads(json_string)
        except json.JSONDecodeError as e:
            raise InputProviderError(
                f"Invalid JSON string provided for inputs: {e}"
            ) from e

        if not isinstance(data, dict):
            raise InputProviderError(
                "Inputs from JSON string must be a JSON object (dictionary)."
            )

        logger.info("Successfully loaded inputs from JSON string.")
        return data
