import dataclasses
import enum
from datetime import datetime
from pathlib import Path
from typing import Any

from pipefunc import Pipeline
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic.dataclasses import dataclass

from flowfunc.locations import ensure
from flowfunc.workflow.runs import generate_unique_id
from flowfunc.workflow.schema import Workflow


class Status(enum.Enum):
    FAILED = "failed"
    SUCCESS = "success"


class Summary(BaseModel):
    run_id: str = Field(..., description="Unique identifier for the run.")
    workflow_name: str = Field(
        ..., description="Name of the executed workflow from its metadata."
    )
    file_path: Path = Field(..., description="Path to the workflow file.")
    status: Status = Field(
        ..., description="Final status of the workflow run (e.g., SUCCESS, FAILED)."
    )
    start_time: datetime = Field(..., description="Start time of the run.")
    end_time: datetime = Field(..., description="End time of the run.")
    duration_seconds: float = Field(
        ..., description="Total duration of the run in seconds."
    )
    user_inputs: dict[str, Any] = Field(
        default_factory=dict, description="Raw inputs provided by the user."
    )
    resolved_inputs: dict[str, Any] = Field(
        default_factory=dict, description="Inputs resolved for the pipeline."
    )
    persisted_outputs: dict[str, str] = Field(
        default_factory=dict, description="Output manifest."
    )


@dataclass
class Metadata:
    run_id: str = dataclasses.field(default_factory=generate_unique_id)
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_seconds: float = 0.0
    status: Status = Status.FAILED

    def start(self) -> None:
        self.start_time = datetime.now()

    def stop(self) -> None:
        self.end_time = datetime.now()
        if self.start_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()


@dataclass
class PathsContext:
    run_dir: Path | None = None
    output_dir: Path | None = None

    @classmethod
    def build(cls, workflow_name, run_id, toml_config):
        from flowfunc.locations import workflow_output_dir
        from flowfunc.locations import workflow_run_dir_actual

        run_dir = workflow_run_dir_actual(toml_config, workflow_name, run_id)
        output_dir = workflow_output_dir(run_dir)
        return cls(
            run_dir=ensure(run_dir),
            output_dir=ensure(output_dir),
        )


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class WorkflowContext:
    pipeline: Pipeline | None = None
    model: Workflow | None = None
    file_path: Path | None = None


@dataclass
class InputsContext:
    user_inputs: dict[str, Any] = dataclasses.field(default_factory=dict)
    resolved_inputs: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclass
class OutputsContext:
    results: dict[str, Any] = dataclasses.field(default_factory=dict)
    persisted_outputs: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclass
class RunContext:
    metadata: Metadata = dataclasses.field(default_factory=Metadata)
    workflow: WorkflowContext = dataclasses.field(default_factory=WorkflowContext)
    inputs: InputsContext = dataclasses.field(default_factory=InputsContext)
    outputs: OutputsContext = dataclasses.field(default_factory=OutputsContext)
    paths: PathsContext = dataclasses.field(default_factory=PathsContext)

    def summarize(self) -> Summary:
        self.metadata.stop()
        return Summary(
            run_id=self.metadata.run_id,
            workflow_name=self.workflow.model.metadata.name,
            file_path=self.workflow.file_path,
            status=self.metadata.status,
            start_time=self.metadata.start_time,
            end_time=self.metadata.end_time,
            duration_seconds=self.metadata.duration_seconds,
            user_inputs=self.inputs.user_inputs,
            resolved_inputs=self.inputs.resolved_inputs,
            persisted_outputs=self.outputs.persisted_outputs,
        )

    def save_summary(self) -> None:
        """Saves the Summary to a 'summary.json' file in the run_directory."""
        summary_file = self.paths.run_dir / "summary.json"
        summary = self.summarize()
        summary_file.parent.mkdir(parents=True, exist_ok=True)
        summary_file.write_text(summary.model_dump_json(indent=2))
