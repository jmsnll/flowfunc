# flowfunc/run/output_persister.py

import logging
from pathlib import Path
from typing import Any

from flowfunc.io.serializer import SerializerError
from flowfunc.io.serializer import lookup as lookup_serializer  # Ensure this exists

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
        output_specs: dict[str, Path] | None,  # From Workflow.spec.outputs
        output_dir: Path,
    ) -> dict[str, str]:
        """
        Saves declared workflow outputs to the output directory.
        Returns a manifest of {output_name: file_path_string}.
        """
        if not output_specs:
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

        for output_name, spec in output_specs.items():
            if output_name not in results:
                logger.warning(
                    f"Output '{output_name}' defined in spec but not found in pipeline results. Skipping."
                )
                continue

            data_to_persist = results[output_name]
            file_name = (
                spec.file
                or f"{output_name}.{self._get_default_extension(spec.serializer)}"
            )
            file_path = output_dir / file_name

            try:
                logger.debug(
                    f"Persisting output '{output_name}' to '{file_path}' using serializer '{spec.serializer}'."
                )
                self._serialize_output(
                    data_to_persist, file_path, spec.serializer, spec.kwargs
                )
                persisted_outputs_manifest[output_name] = str(file_path.resolve())
                logger.info(f"Successfully persisted '{output_name}' to '{file_path}'.")
            except Exception as e:
                # Log error but continue persisting other outputs if possible
                logger.error(
                    f"Failed to persist output '{output_name}' to '{file_path}': {e}",
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
        serializer_name: str | None,  # Name of the serializer (e.g., "json", "pickle")
        serializer_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """
        Serializes and writes data to a file using the specified serializer.
        (Adapted from flowfunc.workflow.outputs._serialize_output)
        """
        serializer_kwargs = serializer_kwargs or {}
        effective_serializer_name = serializer_name

        if not effective_serializer_name:
            # Attempt to infer from file extension if serializer name is not provided
            try:
                lookup_serializer(
                    file_path.suffix
                )  # Check if one exists for the suffix
                effective_serializer_name = (
                    file_path.suffix
                )  # Use suffix as name (e.g. ".json")
            except SerializerError:
                # If no serializer for suffix and none specified, fallback or error
                logger.warning(
                    f"No serializer specified for {file_path} and suffix '{file_path.suffix}' is not recognized. Attempting pickle."
                )
                effective_serializer_name = ".pkl"  # Default fallback

        try:
            # `lookup_serializer` should ideally take the name directly, or adapt suffix
            # e.g. if name is "json", it looks up ".json"
            target_serializer_key = (
                effective_serializer_name
                if effective_serializer_name.startswith(".")
                else f".{effective_serializer_name}"
            )
            serializer = lookup_serializer(target_serializer_key)
            serializer.dump(data, file_path, **serializer_kwargs)
        except SerializerError as e:
            raise OutputPersisterError(
                f"Serializer '{effective_serializer_name}' not found for {file_path}: {e}"
            ) from e
        except Exception as e:
            raise OutputPersisterError(
                f"Error during serialization of {file_path} with '{effective_serializer_name}': {e}"
            ) from e
