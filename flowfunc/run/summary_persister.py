# flowfunc/run/summary_persister.py

import logging
from pathlib import Path

from .summary_model import Summary  # Assuming summary_model.py is in the same directory

logger = logging.getLogger(__name__)


class SummaryPersistenceError(Exception):
    """Custom exception for summary persistence errors."""


class SummaryPersister:
    """
    Saves the final run summary to a file.
    """

    def save(self, summary_data: Summary, file_name: str = "summary.json") -> Path:
        """
        Saves the Summary object to a JSON file in the summary_data.run_dir.
        Returns the path to the saved summary file.
        """
        if not summary_data.run_dir:
            raise SummaryPersistenceError(
                "Run directory not set in Summary data. Cannot save summary."
            )

        summary_file_path = summary_data.run_dir / file_name
        logger.info(f"Saving run summary to: {summary_file_path}")

        try:
            summary_file_path.parent.mkdir(parents=True, exist_ok=True)
            summary_file_path.write_text(summary_data.model_dump_json(indent=2))
            logger.info(f"Run summary saved successfully to {summary_file_path}.")
            return summary_file_path
        except Exception as e:
            logger.error(
                f"Failed to save summary to {summary_file_path}: {e}", exc_info=True
            )
            raise SummaryPersistenceError(
                f"Could not save summary to {summary_file_path}: {e}"
            ) from e
