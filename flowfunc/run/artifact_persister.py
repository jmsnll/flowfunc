import logging
from pathlib import Path
from typing import Any

import numpy as np

from flowfunc.exceptions import ArtifactPersistenceError
from flowfunc.exceptions import SerializerError as IOSerializerError
from flowfunc.io.serializer import Serializer as IOSerializer
from flowfunc.io.serializer import lookup_serializer
from flowfunc.workflow_definition.schema import WorkflowDefinition

logger = logging.getLogger(__name__)


class ArtifactPersister:
    """Persists pipeline results to disk based on workflow output definitions."""

    def persist(
        self,
        results: dict[str, Any],
        workflow_model: WorkflowDefinition,
        artifact_dir: Path,
    ) -> dict[str, str]:
        declared_artifacts: dict[str, Path] | None = workflow_model.spec.artifacts
        if not declared_artifacts:
            logger.info("No artifacts defined; skipping persistence.")
            return {}

        if not artifact_dir.exists():
            try:
                artifact_dir.mkdir(parents=True, exist_ok=True)
                logger.warning(f"Created missing output directory: {artifact_dir}")
            except OSError as e:
                raise ArtifactPersistenceError(
                    f"Cannot create output dir {artifact_dir}: {e}"
                ) from e

        scope = getattr(workflow_model.spec.options, "scope", None)
        manifest: dict[str, str] = {}

        logger.info(f"Persisting artifacts to: {artifact_dir}")

        for name, rel_path in declared_artifacts.items():
            result_key = f"{scope}.{name}" if scope else name
            if result_key not in results:
                logger.warning(f"Result key '{result_key}' missing; skipping '{name}'.")
                continue

            target_path = (
                rel_path
                if rel_path.is_absolute()
                else (artifact_dir / rel_path).resolve()
            )
            data = self._prepare_data_for_serialization(results[result_key].output)

            try:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                logger.debug(f"Persisting '{name}' to: {target_path}")
                self._serialize_artifact(data, target_path)
                manifest[name] = str(target_path)
                logger.info(f"Persisted '{name}' → {target_path}")
            except ArtifactPersistenceError:
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
        if isinstance(data, np.integer | np.floating | np.bool_):
            return data.item()
        if isinstance(data, list):
            return [self._prepare_data_for_serialization(d) for d in data]
        if isinstance(data, dict):
            return {k: self._prepare_data_for_serialization(v) for k, v in data.items()}
        return data

    def _serialize_artifact(self, data: Any, file_path: Path) -> None:
        """Serializes data to disk using appropriate serializer."""
        serializer: IOSerializer | None = lookup_serializer(file_path)
        if not serializer or not serializer.can_dump:
            raise ArtifactPersistenceError(
                f"No serializer for '{file_path.suffix}' at {file_path}"
            )

        try:
            logger.debug(f"Using serializer '{serializer.name}' for {file_path}")
            serializer.dump(data, file_path)
        except IOSerializerError as e:
            raise ArtifactPersistenceError(
                f"Serialization failed for {file_path}: {e}"
            ) from e
        except Exception as e:
            raise ArtifactPersistenceError(
                f"Error during serialization of {file_path} with '{serializer.name}': {e}"
            ) from e
