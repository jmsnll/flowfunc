import logging
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from flowfunc.run.summary_model import Status
from flowfunc.run.summary_model import Summary
from flowfunc.workflow_definition.utils import generate_unique_id

logger = logging.getLogger(__name__)


class RunStateTracker:
    """Tracks state and metadata for a single workflow run."""

    def __init__(self, run_id: str | None = None, name: str | None = None) -> None:
        self._run_id = run_id or generate_unique_id(name)
        self._summary: Summary | None = None
        self._initial_name = name
        logger.info(f"RunStateTracker initialized: {self._run_id}")

    def start_run(
        self,
        workflow_name: str,
        run_dir: Path,
        workflow_file: Path | None = None,
    ) -> None:
        if self._summary:
            logger.warning(f"Run '{self._run_id}' already started; reinitializing.")

        self._summary = Summary(
            run_id=self._run_id,
            workflow_name=workflow_name,
            workflow_file=workflow_file,
            run_dir=run_dir,
            status=Status.RUNNING,
            start_time=datetime.now(UTC),
        )

        logger.info(f"Run started: {self._run_id} ({workflow_name})")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def complete_run(self, status: Status, error: str | None = None) -> None:
        summary = self._require_summary()
        summary.status = status
        summary.end_time = datetime.now(UTC)
        summary.error_message = error

        duration = summary.duration_seconds
        msg = f"Run '{self._run_id}' finished with {status.value}"
        if duration is not None:
            msg += f" in {duration:.2f}s"
        logger.info(msg)

    def update_user_params(self, inputs: dict[str, Any]) -> None:
        self._require_summary().user_params = inputs
        logger.debug(f"User params set for {self._run_id}")

    def update_resolved_params(self, inputs: dict[str, Any]) -> None:
        self._require_summary().resolved_params = inputs
        logger.debug(f"Resolved params set for {self._run_id}")

    def update_artifacts(self, artifacts: dict[str, str]) -> None:
        self._require_summary().artifacts = artifacts
        logger.info(f"Artifacts recorded for {self._run_id}")

    def get_summary(self) -> Summary:
        return self._require_summary()

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def output_dir(self) -> Path:
        return self._require_summary().output_dir

    def _require_summary(self) -> Summary:
        if not self._summary:
            raise RuntimeError("Run not started. Call start_run() first.")
        return self._summary
