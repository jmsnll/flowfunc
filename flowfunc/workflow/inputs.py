import json
import logging
from pathlib import Path
from typing import Any

from flowfunc.workflow.schema import InputItem

logger = logging.getLogger(__name__)


def from_file(file_path: Path) -> dict[str, Any]:
    logger.info(f"Loading inputs from file: {file_path}")

    if not file_path.exists() or not file_path.is_file():
        raise ValueError(f"Input file not found or not a valid file: {file_path}")

    try:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in input file {file_path}") from e
    except OSError as e:
        raise ValueError(f"Could not read input file {file_path}") from e

    if not isinstance(data, dict):
        raise ValueError(
            f"Inputs from file {file_path} must be a JSON object (dictionary)."
        )

    logger.info(f"Successfully loaded inputs from file {file_path}.")
    return data


def from_json(json_str: str) -> dict[str, Any]:
    logger.info("Loading inputs from JSON string option.")
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError("Invalid JSON in --inputs parameter") from e

    if not isinstance(data, dict):
        raise ValueError(
            "Inputs from --inputs parameter must be a JSON object (dictionary)."
        )

    logger.info("Successfully loaded inputs from JSON string option.")
    return data


def input_has_scope(input_name: str) -> bool:
    return len(input_name.split(".")) > 1


def resolve(
    user_inputs: dict[str, Any],
    global_input_definitions: dict[str, InputItem] | None,
    pipeline_expected_input_names: tuple[str, ...],
    pipeline_required_input_names: list[str],
    scope: str | None,
) -> dict[str, Any]:
    """
    Applies defaults to initial inputs and validates against required inputs.
    Returns the final effective input dictionary.
    Raises EnvironmentError if validation fails.
    """
    logger.debug("Resolving effective inputs...")
    resolved = {}
    for name, definition in user_inputs.items():
        resolved_name = name
        if not input_has_scope(name):
            resolved_name = f"{scope}.{name}"
            logger.info(
                f"Input name '{name}' missing scope; "
                f"automatically prefixed with pipeline scope '{scope}' -> '{resolved_name}'"
            )
        resolved[resolved_name] = definition

    expected_set = set(pipeline_expected_input_names)

    if global_input_definitions:
        for name, definition in global_input_definitions.items():
            if (
                name not in resolved
                and definition.value is not None
                and name in expected_set
            ):
                resolved[name] = definition.value
                logger.debug(
                    f"Applied default for global input '{name}': {definition.value!r}"
                )
            elif name not in expected_set:
                logger.debug(
                    f"Skipping default for '{name}'; not expected by pipefunc."
                )
    else:
        logger.debug("No global input definitions found in workflow model.")

    if missing := sorted(set(pipeline_required_input_names) - resolved.keys()):
        raise ValueError(f"Validation failed. Missing required inputs: {missing}")

    logger.info("Effective inputs resolved and validated successfully.")
    return resolved
