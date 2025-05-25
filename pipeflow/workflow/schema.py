from __future__ import annotations  # Important for forward references in type hints

from enum import Enum
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


class GlobalInputDetailModel(BaseModel):
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
    global_inputs: dict[str, GlobalInputDetailModel] | None = Field(
        default_factory=dict
    )
    steps: list[StepModel] = Field(..., min_length=1)  # Pydantic v2 uses min_length
    pipeline_outputs: list[str] | None = Field(default_factory=list)

    class Config:
        extra = "forbid"


class PipeflowPipelineModel(BaseModel):
    apiVersion: str = Field(
        ..., pattern=r"^pipeflow\.dev\/v[0-9]+(?:alpha[0-9]+|beta[0-9]+)?$"
    )
    kind: KindEnum
    metadata: MetadataModel
    spec: PipelineSpecModel

    class Config:
        extra = "forbid"
