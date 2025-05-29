# flowfunc/pipeline/executor.py

import logging
from typing import Any

from pipefunc import Pipeline

logger = logging.getLogger(__name__)


class PipelineExecutionError(Exception):
    """Custom exception for pipeline execution errors."""


class PipelineExecutor:
    """
    Executes a `pipefunc.Pipeline` with resolved inputs.
    """

    def execute(
        self, pipeline: Pipeline, resolved_inputs: dict[str, Any], name: str
    ) -> dict[str, Any]:
        """
        Executes the pipeline using the .map() method.
        """
        pipeline_name = name or "Unnamed Pipeline"
        logger.info(f"Executing pipeline: {pipeline_name}")
        logger.debug(f"Pipeline inputs for execution: {resolved_inputs}")

        try:
            # Consider if you need to pass an executor, profile_path, etc., based on your app's capabilities
            results = pipeline.map(resolved_inputs)
            logger.info(f"Pipeline '{pipeline_name}' execution completed.")
            return results
        except Exception as e:  # Catch specific pipefunc execution errors
            logger.error(
                f"Error during pipeline execution '{pipeline_name}': {e}", exc_info=True
            )
            raise PipelineExecutionError(
                f"Pipeline execution failed for '{pipeline_name}': {e}"
            ) from e
