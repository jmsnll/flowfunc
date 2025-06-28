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
    workflow_name: str = Field(..., description="Name of the executed workflow.")
    workflow_file: Path | None = Field(None, description="Path to the workflow file.")
    status: Status = Field(default=Status.PENDING, description="Final run status.")
    start_time: datetime = Field(default_factory=lambda: datetime.now(UTC))
    end_time: datetime | None = None
    user_inputs: dict[str, Any] = Field(default_factory=dict)
    resolved_inputs: dict[str, Any] = Field(default_factory=dict)
    persisted_outputs: dict[str, str] = Field(default_factory=dict)
    run_dir: Path = Field(..., description="Directory of the current run.")
    error_message: str | None = None

    model_config = {
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
    }

    @computed_field(return_type=float)
    @property
    def duration_seconds(self) -> float | None:
        """Run duration in seconds."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    @property
    def output_dir(self) -> Path:
        """Directory where outputs are stored."""
        return self.run_dir / "outputs"
