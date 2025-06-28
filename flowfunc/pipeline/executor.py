import logging
from typing import Any

from pipefunc import Pipeline

from flowfunc.exceptions import PipelineExecutionError

logger = logging.getLogger(__name__)


class PipelineExecutor:
    """Executes a `pipefunc.Pipeline` with provided inputs."""

    def execute(
        self, pipeline: Pipeline, inputs: dict[str, Any], name: str
    ) -> dict[str, Any]:
        name = name or "Unnamed Pipeline"
        logger.info(f"Executing pipeline: {name}")
        logger.debug(f"Pipeline inputs for execution: {inputs}")

        try:
            results = pipeline.map(inputs)
            logger.info(f"Pipeline '{name}' execution completed.")
            return results
        except Exception as e:
            logger.error(
                f"Error during pipeline execution '{name}': {e}", exc_info=True
            )
            raise PipelineExecutionError(
                f"Pipeline execution failed for '{name}': {e}"
            ) from e
