# flowfunc/run/summary_model.py (Modified from flowfunc/workflow/summary.py)

import enum
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import computed_field


class Status(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class Summary(BaseModel):
    run_id: str = Field(..., description="Unique identifier for the run.")
    workflow_name: str = Field(
        ..., description="Name of the executed workflow from its metadata."
    )
    workflow_file: Path | None = Field(
        None, description="Path to the workflow file, if applicable."
    )
    status: Status = Field(
        default=Status.PENDING,
        description="Final status of the workflow run (e.g., SUCCESS, FAILED).",
    )
    start_time: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Start time of the run (UTC).",
    )
    end_time: datetime | None = Field(None, description="End time of the run (UTC).")
    user_inputs: dict[str, Any] = Field(
        default_factory=dict, description="Raw inputs provided by the user."
    )
    resolved_inputs: dict[str, Any] = Field(
        default_factory=dict, description="Inputs resolved for the pipeline."
    )
    persisted_outputs: dict[str, str] = Field(
        default_factory=dict, description="Output manifest (name: path_to_output)."
    )
    run_dir: Path = Field(..., description="Path to the directory for the current run.")
    error_message: str | None = Field(
        None, description="Error message if the run failed."
    )

    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
    }

    @computed_field(return_type=float)  # type: ignore[misc]
    @property
    def duration_seconds(self) -> float | None:
        """Total duration of the run in seconds, computed from start and end times."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def output_dir(self) -> Path:
        """Directory for persisted outputs within the run directory."""
        return self.run_dir / "outputs"
