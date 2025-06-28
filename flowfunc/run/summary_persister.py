import logging
from pathlib import Path

from flowfunc.exceptions import SummaryPersistenceError
from flowfunc.run.summary_model import Summary

logger = logging.getLogger(__name__)


class SummaryPersister:
    """Persists a run summary to disk as JSON."""

    def save(self, summary: Summary, file_name: str = "summary.json") -> Path:
        if not summary.run_dir:
            raise SummaryPersistenceError("Missing run directory in Summary.")

        path = summary.run_dir / file_name
        logger.info(f"Saving run summary: {path}")

        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(summary.model_dump_json(indent=2), encoding="utf-8")
        except Exception as e:
            logger.exception(f"Failed to write summary: {path}")
            raise SummaryPersistenceError(f"Could not save summary to {path}") from e

        logger.info(f"Summary saved: {path}")
        return path
