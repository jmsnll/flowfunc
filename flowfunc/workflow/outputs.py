import logging
from pathlib import Path
from typing import Any

from flowfunc import locations
from flowfunc.io import serializer

logger = logging.getLogger(__name__)


def persist(
    results: dict[str, Any],
    output_definitions: dict[str, str] | None,
    output_directory: Path,
) -> dict[str, str]:
    """
    Saves declared workflow outputs to disk based on their definitions.
    Returns a manifest of {output_name: relative_saved_path}.
    Individual serialization errors are logged but do not stop other outputs from being processed.
    """
    logger.info("Persisting workflow outputs...")
    manifest: dict[str, str] = {}

    if not output_definitions or not results:
        logger.debug(
            "No output definitions or no pipefunc results; nothing to persist."
        )
        return manifest

    for output_name, output_path in output_definitions.items():
        if not output_path:
            logger.debug(
                f"Output '{output_name}' has no 'path' defined; skipping persistence."
            )
            continue
        if output_name not in results:
            logger.warning(
                f"Output '{output_name}' defined with path '{output_path}' but not found in pipefunc results; skipping."
            )
            continue

        # Ensure path is not absolute, resolve relative to target_output_directory
        target_save_path = (output_directory / output_path).resolve()

        try:
            relative_path_str = _serialize_output(
                results[output_name].output,
                output_name=output_name,
                target_path=target_save_path,
            )
            manifest[output_name] = relative_path_str
        except Exception as e:
            raise OSError(
                f"Serialization/DiskIO failed for output '{output_name}' to '{target_save_path}': {e}"
            ) from e

    logger.info(f"Output persistence complete. Manifest: {manifest}")
    return manifest


def _serialize_output(
    data_to_save: Any,
    output_name: str,
    target_path: Path,
) -> str:
    """
    Internal logic to serialize and save a single output.
    Returns the relative path string of the saved file.
    Raises OutputSerializationError or underlying OS/IO errors.
    """
    file_suffix = target_path.suffix.lower()
    project_root = locations.project_root()

    if not (serializer_func := serializer.lookup(file_suffix)):
        msg = (
            f"Unsupported extension '{file_suffix}' for output '{output_name}' at '{target_path}'. "
            "No serializer found."
        )
        logger.error(msg)
        raise OSError(msg)

    target_path.parent.mkdir(parents=True, exist_ok=True)
    relative_saved_path = str(target_path.relative_to(project_root))

    logger.debug(
        f"Attempting to save output '{output_name}' to '{relative_saved_path}'"
    )
    serializer_func(data_to_save, target_path)
    logger.info(f"Successfully saved output '{output_name}' to '{relative_saved_path}'")

    return relative_saved_path
