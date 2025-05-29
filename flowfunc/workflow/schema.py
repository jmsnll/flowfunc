from __future__ import annotations  # Important for forward references in type hints

from collections.abc import Callable
from enum import Enum
from typing import TYPE_CHECKING
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

if TYPE_CHECKING:
    from pathlib import Path


class KindEnum(str, Enum):
    PIPELINE = "Pipeline"


class InputTypeEnum(str, Enum):
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    LIST = "list"
    OBJECT = "object"
    OUTPUT = "output"


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


class Resources(BaseModel):
    cpus: int | None = None
    memory: str | None = None  # e.g., "8GB"
    advanced_options: dict[str, Any] | None = None

    @field_validator("advanced_options")
    @classmethod
    def prevent_reserved_keys_in_advanced_options(
        cls, v: dict[str, Any] | None
    ) -> dict[str, Any] | None:
        if v is None:
            return None

        reserved_keys = {"cpus", "memory"}
        for key in v:
            if key in reserved_keys:
                raise ValueError(
                    f"Key '{key}' is a reserved field and cannot be used in 'advanced_options'."
                    f"Please set '{key}' at the top level of resources."
                )
        return v


class PipelineOptions(BaseModel):
    validate_type_annotations: bool | None = None
    default_variant: str | dict[str, str] | None = None

    lazy: bool | None = None
    debug: bool | None = None
    profile: bool | None = None
    scope: str | None = None
    default_resources: Resources | None = Field(default_factory=Resources)

    class Config:
        extra = "forbid"


class InputItem(BaseModel):
    description: str | None = None
    type: InputTypeEnum | None = None
    value: Any | None = None


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


class StepDefinition(BaseModel):
    name: str
    func: str | None = None  # FQN string, or simple name if default_module is used
    description: str | None = None
    inputs: dict[str, InputItem | str] | None = Field(default_factory=dict)
    parameters: dict[str, Any] | None = Field(default_factory=dict)
    options: StepOptions | None = Field(default_factory=StepOptions)
    resources: Resources | None = Field(default_factory=Resources)

    class Config:
        extra = "forbid"

    @field_validator("inputs", mode="before")
    @classmethod
    def normalize_inputs(cls, v: Any) -> dict[str, InputItem]:
        if not isinstance(v, dict):
            raise ValueError("inputs must be a dictionary")

        new = {}
        for key, val in v.items():
            if isinstance(val, InputItem):
                new[key] = val
            elif isinstance(val, dict):
                new[key] = InputItem(**val)
            else:
                new[key] = InputItem(value=val)
        return new

    def model_dump(self, **kwargs: dict[str, Any] | None) -> dict[str, Any]:
        base = super().model_dump(**kwargs)
        base["inputs"] = {k: v.value for k, v in self.inputs.items()}
        return base


class PipelineDefinition(BaseModel):
    default_module: str | None = None
    options: PipelineOptions | None = Field(default_factory=PipelineOptions)
    inputs: dict[str, InputItem | str] = Field(default_factory=dict)
    steps: list[StepDefinition] = Field(..., min_length=1)
    outputs: dict[str, str] = Field(default_factory=dict)

    class Config:
        extra = "forbid"

    @field_validator("inputs", mode="before")
    @classmethod
    def normalize_inputs(cls, v: Any) -> dict[str, InputItem]:
        if not isinstance(v, dict):
            raise ValueError("inputs must be a dictionary")

        new = {}
        for key, val in v.items():
            if isinstance(val, InputItem):
                new[key] = val
            elif isinstance(val, dict):
                new[key] = InputItem(**val)
            else:
                new[key] = InputItem(value=val)
        return new

    def model_dump(self, **kwargs: dict[str, Any] | None) -> dict[str, Any]:
        base = super().model_dump(**kwargs)
        base["inputs"] = {k: v.value for k, v in self.inputs.items()}
        return base


class WorkflowDefinition(BaseModel):
    apiVersion: str = Field(
        ..., pattern=r"^flowfunc\.dev\/v[0-9]+(?:alpha[0-9]+|beta[0-9]+)?$"
    )
    kind: KindEnum
    metadata: Metadata
    spec: PipelineDefinition

    class Config:
        extra = "forbid"
