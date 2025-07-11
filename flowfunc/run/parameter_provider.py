import json
import logging
from pathlib import Path
from typing import Any

from flowfunc.exceptions import InputProviderError

logger = logging.getLogger(__name__)


class ParameterProvider:
    """Loads user params from a JSON file or string."""

    def load_from_file(self, file_path: Path) -> dict[str, Any]:
        logger.info(f"Loading params from: {file_path}")

        if not file_path.is_file():
            raise InputProviderError(f"Invalid input file: {file_path}")

        try:
            with file_path.open(encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            raise InputProviderError(
                f"Failed to read/parse input file {file_path}: {e}"
            ) from e

        if not isinstance(data, dict):
            raise InputProviderError(
                f"Input file {file_path} must contain a JSON object."
            )

        logger.info("Inputs loaded successfully.")
        return data

    def load_from_json_string(self, json_string: str) -> dict[str, Any]:
        logger.info("Loading params from JSON string.")

        if not json_string:
            logger.warning("Empty JSON string provided. Returning empty dict.")
            return {}

        try:
            data = json.loads(json_string)
        except json.JSONDecodeError as e:
            raise InputProviderError(f"Invalid JSON string: {e}") from e

        if not isinstance(data, dict):
            raise InputProviderError("JSON string must contain a JSON object.")

        logger.info("Inputs loaded successfully.")
        return data
