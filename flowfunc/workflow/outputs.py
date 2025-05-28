from pathlib import Path
from typing import Any
from typing import cast

from flowfunc import locations
from flowfunc.io import serializer
from flowfunc.workflow.run import logger
from flowfunc.workflow.schema import PipelineOutputItem


def persist_workflow_outputs(
    workflow_results: dict[str, Any],
    workflow_output_definitions: list[PipelineOutputItem | str] | None,
    target_output_directory: Path,
) -> dict[str, str]:
    """
    Saves declared workflow outputs to disk based on their definitions.
    Returns a manifest of {output_name: relative_saved_path}.
    Individual serialization errors are logged but do not stop other outputs from being processed.
    """
    logger.info("Persisting workflow outputs...")
    manifest: dict[str, str] = {}

    if not workflow_output_definitions or not workflow_results:
        logger.debug(
            "No output definitions or no pipefunc results; nothing to persist."
        )
        return manifest

    for output_definition in workflow_output_definitions:
        if isinstance(output_definition, str):
            # Output declared by name only, not explicitly saved by this function
            if output_definition in workflow_results:
                logger.warning(
                    f"Output '{output_definition}' declared by name only: Value present in results, but not saved."
                )
            continue

        output_item = cast("PipelineOutputItem", output_definition)
        if not output_item.path:
            logger.debug(
                f"Output '{output_item.name}' has no 'path' defined; skipping persistence."
            )
            continue
        if output_item.name not in workflow_results:
            logger.warning(
                f"Output '{output_item.name}' defined with path '{output_item.path}' but not found in pipefunc results; skipping."
            )
            continue

        # Ensure path is not absolute, resolve relative to target_output_directory
        target_save_path = (target_output_directory / output_item.path).resolve()

        try:
            relative_path_str = _serialize_output(
                workflow_results[output_item.name].output,
                output_name=output_item.name,
                target_path=target_save_path,
            )
            manifest[output_item.name] = relative_path_str
        except Exception as e:
            raise OSError(
                f"Serialization/DiskIO failed for output '{output_item.name}' to '{target_save_path}': {e}"
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
