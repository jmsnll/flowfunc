# flowfunc/run/output_persister.py

import logging
from pathlib import Path
from typing import Any

from flowfunc.exceptions import SerializerError
from flowfunc.io.serializer import lookup as lookup_serializer  # Ensure this exists
from flowfunc.workflow_definition import WorkflowDefinition

logger = logging.getLogger(__name__)


class OutputPersisterError(Exception):
    """Custom exception for output persisting errors."""


class OutputPersister:
    """
    Persists pipeline results to disk according to workflow output definitions.
    (Adapted from flowfunc.workflow.outputs.persist)
    """

    def persist(
        self,
        results: dict[str, Any],
        workflow_model: WorkflowDefinition,  # Pass the whole model to get scope and output specs
        output_dir: Path,
    ) -> dict[str, str]:
        """
        Saves declared workflow outputs to the output directory.
        Returns a manifest of {output_name: file_path_string}.
        """
        if not workflow_model.spec.outputs:
            logger.info(
                "No output specifications provided in workflow. Skipping output persistence."
            )
            return {}

        if not output_dir.exists():
            logger.warning(
                f"Output directory {output_dir} does not exist. Creating it."
            )
            output_dir.mkdir(parents=True, exist_ok=True)

        persisted_outputs_manifest: dict[str, str] = {}
        logger.info(f"Persisting workflow outputs to: {output_dir}")
        workflow_scope = (
            workflow_model.spec.options.scope if workflow_model.spec.options else None
        )

        for output_name, output_path in workflow_model.spec.outputs.items():
            scoped_name = (
                f"{workflow_scope}.{output_name}" if workflow_scope else output_name
            )
            if scoped_name not in results:
                logger.warning(
                    f"Output '{scoped_name}' defined in spec but not found in pipeline results. Skipping."
                )
                continue

            data_to_persist = results[scoped_name]
            file_path = (output_dir / output_path).resolve()

            try:
                logger.debug(f"Persisting output '{scoped_name}' to '{file_path}'.")
                self._serialize_output(data_to_persist, file_path)
                persisted_outputs_manifest[scoped_name] = str(file_path.resolve())
                logger.info(f"Successfully persisted '{scoped_name}' to '{file_path}'.")
            except Exception as e:
                # Log error but continue persisting other outputs if possible
                logger.error(
                    f"Failed to persist output '{scoped_name}' to '{file_path}': {e}",
                    exc_info=True,
                )
                # Optionally, re-raise if one failure should stop all:
                # raise OutputPersisterError(f"Failed to persist output '{output_name}': {e}") from e

        return persisted_outputs_manifest

    def _get_default_extension(self, serializer_name: str | None) -> str:
        # Try to get a default extension from the serializer itself if possible
        # This is a placeholder; your serializer lookup might provide this.
        if serializer_name:
            try:
                serializer_instance = lookup_serializer(
                    f".{serializer_name}"
                )  # Assuming lookup by extension
                if hasattr(serializer_instance, "DEFAULT_EXTENSION"):
                    return serializer_instance.DEFAULT_EXTENSION.lstrip(".")
                # Fallback based on common names
                if "json" in serializer_name:
                    return "json"
                if "pickle" in serializer_name:
                    return "pkl"
                if "yaml" in serializer_name:
                    return "yaml"
                if "parquet" in serializer_name:
                    return "parquet"
                if "csv" in serializer_name:
                    return "csv"
            except SerializerError:
                pass  # Serializer not found by this guess
        return "dat"  # Generic default

    def _serialize_output(
        self,
        data: Any,
        file_path: Path,
    ) -> None:
        """
        Serializes and writes data to a file using the specified serializer.
        (Adapted from flowfunc.workflow.outputs._serialize_output)
        """
        try:
            # `lookup_serializer` should ideally take the name directly, or adapt suffix
            # e.g. if name is "json", it looks up ".json"
            target_serializer_key = file_path.suffix
            serializer = lookup_serializer(target_serializer_key)
            serializer(data, file_path)
        except SerializerError as e:
            raise OutputPersisterError(
                f"Serializer for '{file_path.suffix}' not found for {file_path}: {e}"
            ) from e
        except Exception as e:
            raise OutputPersisterError(
                f"Error during serialization of {file_path} with '{file_path.suffix}': {e}"
            ) from e
