from __future__ import annotations  # Important for forward references in type hints

from enum import Enum
from pathlib import Path
from typing import Any
from collections.abc import Callable

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator


class KindEnum(str, Enum):
    PIPELINE = "Pipeline"


class GlobalInputTypeEnum(str, Enum):
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    LIST = "list"
    OBJECT = "object"


class ResourcesScopeEnum(str, Enum):
    MAP = "map"
    ELEMENT = "element"


class ParallelizationModeEnum(str, Enum):  # From all_docs.md (slurm)
    INTERNAL = "internal"


class Metadata(BaseModel):
    name: str = Field(..., pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
    version: str | None = None
    description: str | None = None
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None


class OutputItem(BaseModel):
    name: str
    path: Path | None = None

    class Config:
        extra = "forbid"


class PipelineConfig(BaseModel):
    validate_type_annotations: bool | None = None
    default_variant: str | dict[str, str] | None = None

    lazy: bool | None = None
    debug: bool | None = None
    profile: bool | None = None
    scope: str | None = None
    resources: Resources | None = None

    class Config:
        extra = "forbid"


class GlobalInputItem(BaseModel):
    description: str
    type: GlobalInputTypeEnum | None = None
    default: Any | None = None  # Value can be any valid JSON/YAML type


class Resources(BaseModel):
    cpus: int | None = None
    memory: str | None = None  # e.g., "8GB"
    advanced_options: dict[str, Any] | None = None

    class Config:
        extra = "allow"


class StepOptions(BaseModel):
    func: Callable | None = None
    output_name: str | list[str] | None = None
    renames: dict[str, str] | None = None
    defaults: dict[str, Any] | None = None
    bound: dict[str, Any] | None = None
    profile: bool | None = None
    debug: bool | None = None
    cache: bool | None = None
    mapspec: str | None = None
    scope: str | None = None
    advanced_options: dict[str, Any] | None = None

    class Config:
        extra = "forbid"


class Step(BaseModel):
    name: str
    func: str | None = None  # FQN string, or simple name if default_module is used
    description: str | None = None
    inputs: dict[str, str] | None = Field(default_factory=dict)
    parameters: dict[str, Any] | None = Field(default_factory=dict)
    options: StepOptions | None = Field(default_factory=StepOptions)
    resources: Resources | None = Field(default_factory=Resources)

    class Config:
        extra = "forbid"


class Pipeline(BaseModel):
    default_module: str | None = None
    config: PipelineConfig | None = Field(default_factory=PipelineConfig)
    global_inputs: dict[str, GlobalInputItem] | None = Field(default_factory=dict)
    steps: list[Step] = Field(..., min_length=1)  # Pydantic v2 uses min_length
    pipeline_outputs: list[str | OutputItem] = Field(default_factory=list)

    class Config:
        extra = "forbid"


class Workflow(BaseModel):
    apiVersion: str = Field(
        ..., pattern=r"^flowfunc\.dev\/v[0-9]+(?:alpha[0-9]+|beta[0-9]+)?$"
    )
    kind: KindEnum
    metadata: Metadata
    spec: Pipeline

    class Config:
        extra = "forbid"
