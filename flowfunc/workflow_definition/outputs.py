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
    Saves declared workflow outputs to disk based on definitions.
    Returns a manifest: {output_name: relative_saved_path}.
    """
    logger.info("Persisting workflow outputs...")
    manifest: dict[str, str] = {}

    if not output_definitions or not results:
        logger.debug("No output definitions or results; skipping persistence.")
        return manifest

    for name, rel_path in output_definitions.items():
        if not rel_path:
            logger.debug(f"Output '{name}' missing path; skipping.")
            continue
        if name not in results:
            logger.warning(f"Output '{name}' not found in results; skipping.")
            continue

        target_path = (output_directory / rel_path).resolve()

        try:
            manifest[name] = _serialize_output(results[name].output, name, target_path)
        except Exception as e:
            raise OSError(
                f"Failed to persist output '{name}' to '{target_path}': {e}"
            ) from e

    logger.info(f"Output persistence complete. Manifest: {manifest}")
    return manifest


def _serialize_output(data: Any, name: str, path: Path) -> str:
    """
    Serializes and saves one output. Returns relative path from project root.
    """
    suffix = path.suffix.lower()
    project_root = locations.project_root()

    if not (serialize := serializer.lookup(suffix)):
        msg = f"No serializer for '{suffix}' (output '{name}' at '{path}')"
        logger.error(msg)
        raise OSError(msg)

    path.parent.mkdir(parents=True, exist_ok=True)
    rel_path = str(path.relative_to(project_root))

    logger.debug(f"Saving output '{name}' to '{rel_path}'")
    serialize(data, path)
    logger.info(f"Output '{name}' saved to '{rel_path}'")

    return rel_path
