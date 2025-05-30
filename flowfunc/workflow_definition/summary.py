import enum
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import computed_field


class Status(enum.Enum):
    FAILED = "failed"
    SUCCESS = "success"


class Summary(BaseModel):
    run_id: str = Field(description="Unique identifier for the run.")
    workflow_name: str = Field(
        description="Name of the executed workflow from its metadata."
    )
    workflow_file: Path = Field(description="Path to the workflow file.")
    status: Status = Field(
        description="Final status of the workflow run (e.g., SUCCESS, FAILED).",
        default=Status.FAILED,
    )
    start_time: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Start time of the run (UTC), automatically set on creation.",
    )
    end_time: datetime = Field(description="End time of the run.", default=None)
    user_inputs: dict[str, Any] = Field(
        default_factory=dict, description="Raw inputs provided by the user."
    )
    resolved_inputs: dict[str, Any] = Field(
        default_factory=dict, description="Inputs resolved for the pipeline."
    )
    persisted_outputs: dict[str, str] = Field(
        default_factory=dict, description="Output manifest."
    )
    run_dir: Path = Field(
        description="Path to the directory for the current run to store outputs & logs."
    )

    @computed_field
    @property
    def duration_seconds(self) -> float:
        """Total duration of the run in seconds, computed from start and end times."""
        return (self.end_time - self.start_time).total_seconds()

    @computed_field
    @property
    def output_dir(self) -> Path:
        """Total duration of the run in seconds, computed from start and end times."""
        return self.run_dir / "outputs"

    def finished(self) -> None:
        self.end_time = datetime.now(UTC)
        self.status = Status.SUCCESS

    def save(self) -> None:
        summary_file = self.run_dir / "summary.json"
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        summary_file.write_text(self.model_dump_json(indent=2))
