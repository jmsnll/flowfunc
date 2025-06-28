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
    """Persists pipeline results to disk based on workflow output definitions."""

    def persist(
        self,
        results: dict[str, Any],
        workflow_model: WorkflowDefinition,
        output_dir: Path,
    ) -> dict[str, str]:
        declared_outputs: dict[str, Path] | None = workflow_model.spec.outputs
        if not declared_outputs:
            logger.info("No outputs defined; skipping persistence.")
            return {}

        if not output_dir.exists():
            try:
                output_dir.mkdir(parents=True, exist_ok=True)
                logger.warning(f"Created missing output directory: {output_dir}")
            except OSError as e:
                raise OutputPersisterError(
                    f"Cannot create output dir {output_dir}: {e}"
                ) from e

        scope = getattr(workflow_model.spec.options, "scope", None)
        manifest: dict[str, str] = {}

        logger.info(f"Persisting outputs to: {output_dir}")

        for name, rel_path in declared_outputs.items():
            result_key = f"{scope}.{name}" if scope else name
            if result_key not in results:
                logger.warning(f"Result key '{result_key}' missing; skipping '{name}'.")
                continue

            target_path = (
                rel_path
                if rel_path.is_absolute()
                else (output_dir / rel_path).resolve()
            )
            data = self._prepare_data_for_serialization(results[result_key].output)

            try:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Persisting '{name}' to: {target_path}")
                self._serialize_output(data, target_path)
                manifest[name] = str(target_path)
                logger.info(f"Persisted '{name}' → {target_path}")
            except OutputPersisterError:
                logger.exception(f"Failed to serialize '{name}' → {target_path}")
            except OSError as e:
                logger.error(
                    f"Dir creation failed for '{name}' at {target_path.parent}: {e}",
                    exc_info=True,
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error persisting '{name}' → {target_path}: {e}",
                    exc_info=True,
                )

        return manifest

    def _prepare_data_for_serialization(self, data: Any) -> Any:
        """Converts non-standard types (e.g., NumPy) into Python-native types."""
        if isinstance(data, np.ndarray):
            return data.tolist()
        if isinstance(data, (np.integer, np.floating, np.bool_)):
            return data.item()
        if isinstance(data, list):
            return [self._prepare_data_for_serialization(d) for d in data]
        if isinstance(data, dict):
            return {k: self._prepare_data_for_serialization(v) for k, v in data.items()}
        return data

    def _serialize_output(self, data: Any, file_path: Path) -> None:
        """Serializes data to disk using appropriate serializer."""
        serializer: IOSerializer | None = lookup_serializer(file_path)
        if not serializer or not serializer.can_dump:
            raise OutputPersisterError(
                f"No serializer for '{file_path.suffix}' at {file_path}"
            )

        try:
            logger.debug(f"Using serializer '{serializer.name}' for {file_path}")
            serializer.dump(data, file_path)
        except IOSerializerError as e:
            raise OutputPersisterError(
                f"Serialization failed for {file_path}: {e}"
            ) from e
        except Exception as e:
            raise OutputPersisterError(
                f"Error during serialization of {file_path} with '{serializer.name}': {e}"
            ) from e
