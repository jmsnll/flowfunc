import logging
from pathlib import Path
from typing import Any

import numpy as np

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

        global_scope: str | None = None
        if workflow_model.spec.options and workflow_model.spec.options.scope:
            global_scope = workflow_model.spec.options.scope

        if global_scope:
            logger.debug(
                f"Using global output scope for result lookup: '{global_scope}'"
            )

        for declared_output_name, defined_path_spec in declared_outputs.items():
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

            data_to_persist = self._prepare_data_for_serialization(
                results[actual_result_key].output
            )
            target_file_path: Path
            if defined_path_spec.is_absolute():
                target_file_path = defined_path_spec
            else:
                target_file_path = (output_dir / defined_path_spec).resolve()

            try:
                logger.debug(
                    f"Attempting to persist output '{declared_output_name}' (from result key '{actual_result_key}') to '{target_file_path}'."
                )

                target_file_path.parent.mkdir(parents=True, exist_ok=True)

                self._serialize_output(data_to_persist, target_file_path)

                persisted_outputs_manifest[declared_output_name] = str(target_file_path)
                logger.info(
                    f"Successfully persisted '{declared_output_name}' to '{target_file_path}'."
                )

            # Only log the exceptions, as re-raising would stop persisting all remaining outputs too
            except OutputPersisterError:
                logger.exception(
                    f"Serialization failed for output '{declared_output_name}' to path '{target_file_path}'."
                )

            except OSError as e:
                logger.error(
                    f"Could not create directory for output '{declared_output_name}' at '{target_file_path.parent}': {e}",
                    exc_info=True,
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error persisting output '{declared_output_name}' to '{target_file_path}': {e}",
                    exc_info=True,
                )

        return persisted_outputs_manifest

    def _prepare_data_for_serialization(self, data: Any) -> Any:
        """
        Recursively converts known non-standard types into serializable Python objects.
        This is a universal cleanup step that runs for all data before serialization.
        """
        if isinstance(data, np.ndarray):
            return data.tolist()
        if isinstance(data, np.integer):
            return int(data)
        if isinstance(data, np.floating):
            return float(data)
        if isinstance(data, np.bool_):
            return bool(data)

        if isinstance(data, list):
            return [self._prepare_data_for_serialization(item) for item in data]
        if isinstance(data, dict):
            return {
                key: self._prepare_data_for_serialization(value)
                for key, value in data.items()
            }

        return data

    def _serialize_output(
        self,
        data: Any,
        file_path: Path,
    ) -> None:
        """Serializes and writes data to a file using a looked-up serializer."""
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
