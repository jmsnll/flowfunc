# flowfunc/run/state_tracker.py

import logging
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from flowfunc.run.summary_model import Status
from flowfunc.run.summary_model import Summary
from flowfunc.workflow_definition.utils import (
    generate_unique_id,  # Assuming this utility
)

logger = logging.getLogger(__name__)


class RunStateTracker:
    """
    Manages the lifecycle state of a single workflow run.
    It creates and updates a Summary object.
    """

    def __init__(self, run_id: str | None = None, custom_run_name: str | None = None):
        self._run_id = run_id or generate_unique_id(custom_run_name)
        self._summary_data: Summary | None = None
        self._initial_custom_run_name = custom_run_name  # For logging or if needed

        logger.info(f"RunStateTracker initialized for run_id: {self._run_id}")

    def start_run(
        self,
        workflow_name: str,
        run_dir: Path,
        workflow_file: Path | None = None,
    ) -> None:
        """Initializes the summary data when the run officially starts."""
        if self._summary_data is not None:
            logger.warning(
                f"Run {self._run_id} already started. Re-initializing summary might lose data."
            )

        self._summary_data = Summary(
            run_id=self._run_id,
            workflow_name=workflow_name,
            workflow_file=workflow_file,
            run_dir=run_dir,
            status=Status.RUNNING,
            start_time=datetime.now(UTC),
        )
        logger.info(
            f"Run '{self._run_id}' for workflow '{workflow_name}' started at {self._summary_data.start_time}."
        )
        logger.info(f"Run directory: {run_dir}")
        if not self._summary_data.output_dir.exists():
            self._summary_data.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {self._summary_data.output_dir}")

    def get_summary(self) -> Summary:
        if self._summary_data is None:
            raise ValueError("Run has not been started. Call start_run() first.")
        return self._summary_data

    def update_user_inputs(self, user_inputs: dict[str, Any]) -> None:
        summary = self.get_summary()
        summary.user_inputs = user_inputs
        logger.debug(f"Run {self._run_id}: User inputs updated.")

    def update_resolved_inputs(self, resolved_inputs: dict[str, Any]) -> None:
        summary = self.get_summary()
        summary.resolved_inputs = resolved_inputs
        logger.debug(f"Run {self._run_id}: Resolved inputs updated.")

    def update_persisted_outputs(self, persisted_outputs: dict[str, str]) -> None:
        summary = self.get_summary()
        summary.persisted_outputs = persisted_outputs
        logger.info(f"Run {self._run_id}: Persisted outputs updated.")

    def complete_run(self, status: Status, error_message: str | None = None) -> None:
        summary = self.get_summary()
        summary.status = status
        summary.end_time = datetime.now(UTC)
        if error_message:
            summary.error_message = error_message
        duration = summary.duration_seconds
        logger.info(
            f"Run {self._run_id} completed with status '{status.value}'. "
            f"Duration: {duration:.2f}s"
            if duration is not None
            else ""
        )

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def output_dir(self) -> Path:
        return self.get_summary().output_dir
