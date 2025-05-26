from __future__ import annotations  # Important for forward references in type hints

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import Field


class KindEnum(str, Enum):
    PIPELINE = "Pipeline"


class GlobalInputTypeEnum(str, Enum):
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"


class ResourcesScopeEnum(str, Enum):
    MAP = "map"
    ELEMENT = "element"


class ParallelizationModeEnum(str, Enum):  # From all_docs.md (slurm)
    INTERNAL = "internal"


class CacheTypeEnum(str, Enum):  # From all_docs.md (caching)
    LRU = "lru"
    HYBRID = "hybrid"
    DISK = "disk"
    SIMPLE = "simple"


class MetadataModel(BaseModel):
    name: str = Field(..., pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
    version: str | None = None
    description: str | None = None
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None


class PipelineOutputItem(BaseModel):  # New model for detailed output definition
    name: str  # Name of the output from pipefunc results (e.g., "step_name.output_key")
    path: Path | None = (
        None  # Optional: path to save this output, relative to run's output dir
    )
    # format: str | None = None # Optional: if flowfunc needs to handle serialization (e.g., "csv", "json", "parquet")
    # persist: bool = True # Implicitly true if path is given

    class Config:
        extra = "forbid"


class PipelineConfigModel(BaseModel):
    validate_type_annotations: bool | None = None
    cache_type: CacheTypeEnum | None = None
    cache_kwargs: dict[str, Any] | None = None
    default_variant: str | dict[str, str] | None = None

    lazy: bool | None = None
    debug: bool | None = None
    profile: bool | None = None
    scope: str | None = None
    default_resources: ResourcesModel | None = None

    class Config:
        extra = "forbid"  # Corresponds to additionalProperties: false


class GlobalInputItem(BaseModel):
    description: str
    type: GlobalInputTypeEnum | None = None
    default: Any | None = None  # Value can be any valid JSON/YAML type


class ResourcesModel(BaseModel):  # From pipefunc_options.resources
    cpus: int | None = None
    gpus: int | None = None
    memory: str | None = None  # e.g., "8GB"
    wall_time: str | None = None  # e.g., "1:00:00"
    queue: str | None = None
    partition: str | None = None
    parallelization_mode: ParallelizationModeEnum | None = None
    extra_job_scheduler_args: list[str] | None = None

    class Config:
        extra = "allow"  # Corresponds to additionalProperties: true in schema


class PipefuncOptionsModel(BaseModel):
    output_name: str | list[str] | None = None
    output_picker: str | None = None  # FQN string
    renames: dict[str, str] | None = None
    defaults: dict[str, Any] | None = None
    bound: dict[str, Any] | None = None
    profile: bool | None = None
    debug: bool | None = None
    cache: bool | None = None
    mapspec: str | None = None
    internal_shape: int | Literal["?"] | list[int | Literal["?"]] | None = None
    post_execution_hook: str | None = None  # FQN string
    resources: ResourcesModel | None = None  # Using the nested model
    resources_variable: str | None = None
    resources_scope: ResourcesScopeEnum | None = None
    scope: str | None = None
    # For variant, string or dict. Keys in dict are strings.
    # Convention for default group ('_default_') handled in processing logic.
    variant: str | dict[str, str] | None = None

    class Config:
        extra = "forbid"


class StepModel(BaseModel):
    name: str
    function: str | None = None  # FQN string, or simple name if default_module is used
    description: str | None = None
    inputs: dict[str, str] | None = Field(default_factory=dict)  # Default to empty dict
    parameters: dict[str, Any] | None = Field(
        default_factory=dict
    )  # Default to empty dict
    options: PipefuncOptionsModel | None = Field(
        default_factory=PipefuncOptionsModel
    )  # Default to empty options

    class Config:
        extra = "forbid"


class PipelineSpecModel(BaseModel):
    default_module: str | None = None
    pipeline_config: PipelineConfigModel | None = Field(
        default_factory=PipelineConfigModel
    )
    global_inputs: dict[str, GlobalInputItem] | None = Field(default_factory=dict)
    steps: list[StepModel] = Field(..., min_length=1)  # Pydantic v2 uses min_length
    pipeline_outputs: list[str | PipelineOutputItem] = Field(default_factory=list)

    class Config:
        extra = "forbid"


class FlowFuncPipelineModel(BaseModel):
    apiVersion: str = Field(
        ..., pattern=r"^flowfunc\.dev\/v[0-9]+(?:alpha[0-9]+|beta[0-9]+)?$"
    )
    kind: KindEnum
    metadata: MetadataModel
    spec: PipelineSpecModel

    class Config:
        extra = "forbid"


class PipefuncCacheConfigUsed(BaseModel):
    cache_type: str | None = Field(
        default=None,
        description="The type of cache used by pipefunc (e.g., 'disk', 'memory').",
    )
    cache_kwargs: dict[str, Any] = Field(
        default_factory=dict,
        description="The keyword arguments passed to the pipefunc cache.",
    )


class RunInfoModel(BaseModel):
    run_id: str = Field(..., description="Unique identifier for the run.")
    flowfunc_version: str = Field(
        ..., description="Version of FlowFunc used for this run."
    )
    workflow_metadata_name: str = Field(
        ..., description="Name of the executed workflow from its metadata."
    )
    workflow_metadata_version: str = Field(
        ..., description="Version of the executed workflow from its metadata."
    )
    workflow_file_relative_path: str = Field(
        ..., description="Path to the workflow file, relative to the project root."
    )
    status: str = Field(
        ..., description="Final status of the workflow run (e.g., SUCCESS, FAILED)."
    )
    start_time_utc: datetime = Field(..., description="Start time of the run in UTC.")
    end_time_utc: datetime = Field(..., description="End time of the run in UTC.")
    duration_seconds: float = Field(
        ..., description="Total duration of the run in seconds."
    )
    user_provided_inputs: dict[str, Any] = Field(
        default_factory=dict,
        description="Inputs provided by the user for the workflow run.",
    )
    effective_inputs_used_by_pipefunc: dict[str, Any] = Field(
        default_factory=dict,
        description="The actual inputs resolved and used by the pipefunc pipeline.",
    )
    flowfunc_persisted_outputs: dict[str, str] = Field(
        default_factory=dict,
        description="Manifest of outputs persisted by FlowFunc, mapping output name to its relative path.",
    )
    pipefunc_cache_config_used: PipefuncCacheConfigUsed = Field(
        ..., description="Configuration of the pipefunc cache used for this run."
    )
    run_artifacts_base_dir_relative: str = Field(
        ...,
        description="Path to the base directory for this run's artifacts, relative to the project root.",
    )

    class Config:
        pass

    def save_run_info(self, run_dir: Path) -> None:
        """Saves the RunInfoModel to a 'run_info.json' file in the run_directory."""
        run_info_file_path = run_dir / "run_info.json"
        try:
            run_info_file_path.parent.mkdir(parents=True, exist_ok=True)
            run_info_file_path.write_text(self.model_dump_json(indent=2))
        except Exception as e:
            run_id = run_dir.parts[-1]
            raise Exception(
                f"Failed to write {run_info_file_path.name} for run {run_id}"
            ) from e
