import logging
from pathlib import Path
from typing import Any

from flowfunc.exceptions import OutputPersisterError
from flowfunc.exceptions import SerializerError as IOSerializerError
from flowfunc.io.serializer import Serializer as IOSerializer
from flowfunc.io.serializer import lookup_serializer
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class OutputPersister:
    """
    Persists pipeline results to disk according to workflow output definitions,
    handling global workflow scope and using the new serializer lookup.
    """

    def persist(
        self,
        results: dict[str, Any],
        workflow_model: WorkflowDefinition,
        output_dir: Path,
    ) -> dict[str, str]:
        """
        Saves declared workflow outputs to the output directory.
        Returns a manifest of {declared_output_name: absolute_file_path_string}.
        """
        # Assuming workflow_model.spec.outputs is Optional[Dict[str, Path]]
        declared_outputs: dict[str, Path] | None = workflow_model.spec.outputs
        if not declared_outputs:
            logger.info(
                "No output specifications provided in workflow. Skipping output persistence."
            )
            return {}

        if not output_dir.exists():
            logger.warning(
                f"Base output directory {output_dir} does not exist. Creating it."
            )
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                raise OutputPersisterError(
                    f"Could not create base output directory {output_dir}: {e}"
                ) from e

        persisted_outputs_manifest: dict[str, str] = {}
        logger.info(f"Persisting workflow outputs to base directory: {output_dir}")

        # Determine global scope for looking up results
        global_scope: str | None = None
        if workflow_model.spec.options and workflow_model.spec.options.scope:
            global_scope = workflow_model.spec.options.scope

        if global_scope:
            logger.debug(
                f"Using global output scope for result lookup: '{global_scope}'"
            )

        for declared_output_name, defined_path_spec in declared_outputs.items():
            # Construct the key to look for in results, considering the global scope
            actual_result_key = (
                f"{global_scope}.{declared_output_name}"
                if global_scope
                else declared_output_name
            )

            if actual_result_key not in results:
                logger.warning(
                    f"Output '{declared_output_name}' (expected as '{actual_result_key}' in pipeline results) "
                    "not found. Skipping."
                )
                continue

            data_to_persist = results[actual_result_key].output

            # Determine the final absolute target path
            target_file_path: Path
            if defined_path_spec.is_absolute():
                target_file_path = defined_path_spec
            else:
                target_file_path = (output_dir / defined_path_spec).resolve()

            try:
                logger.debug(
                    f"Attempting to persist output '{declared_output_name}' (from result key '{actual_result_key}') to '{target_file_path}'."
                )

                # Ensure parent directory exists before serialization
                target_file_path.parent.mkdir(parents=True, exist_ok=True)

                self._serialize_output(data_to_persist, target_file_path)

                # Use the originally declared output name for the manifest key
                persisted_outputs_manifest[declared_output_name] = str(target_file_path)
                logger.info(
                    f"Successfully persisted '{declared_output_name}' to '{target_file_path}'."
                )

            except OutputPersisterError:  # Re-raise errors from _serialize_output
                # Error already logged in _serialize_output or by the serializer itself
                # Decide if we want to log again or just let it propagate
                logger.error(
                    f"Serialization failed for output '{declared_output_name}' to path '{target_file_path}'."
                )
                # Continue to next output, or re-raise if one failure should stop all
            except OSError as e:  # Catch errors from mkdir
                logger.error(
                    f"Could not create directory for output '{declared_output_name}' at '{target_file_path.parent}': {e}",
                    exc_info=True,
                )
            except Exception as e:
                # Catch any other unexpected errors for this specific output, re-raising would stop ALL persistence.
                logger.error(
                    f"Unexpected error persisting output '{declared_output_name}' to '{target_file_path}': {e}",
                    exc_info=True,
                )

        return persisted_outputs_manifest

    def _serialize_output(
        self,
        data: Any,
        file_path: Path,
    ) -> None:
        """
        Serializes and writes data to a file using a looked-up serializer.
        """
        serializer_handler: IOSerializer | None = lookup_serializer(file_path)

        if serializer_handler and serializer_handler.can_dump:
            try:
                logger.debug(
                    f"Using serializer '{serializer_handler.name}' for path '{file_path}'"
                )
                serializer_handler.dump(data, file_path)
            except IOSerializerError as e:
                raise OutputPersisterError(
                    f"Serialization failed for {file_path}: {e}"
                ) from e
            except Exception as e:
                raise OutputPersisterError(
                    f"Unexpected error during serialization of {file_path} with {serializer_handler.name}: {e}"
                ) from e
        else:
            raise OutputPersisterError(
                f"No suitable serializer found or dumping not supported for file type: '{file_path.suffix}' (path: {file_path})"
            )
