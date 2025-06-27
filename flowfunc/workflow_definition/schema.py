from __future__ import annotations

import logging
from collections.abc import Callable
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator
from pydantic.main import IncEx

logger = logging.getLogger(__name__)


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


class Metadata(BaseModel):
    name: str = Field(pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
    version: str = None
    description: str | None = None
    labels: dict[str, str] | None = None
    annotations: dict[str, str] | None = None


class Resources(BaseModel):
    cpus: int | None = None
    memory: str | None = None
    advanced_options: dict[str, Any] | None = None

    @classmethod
    @field_validator("advanced_options")
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


class WorkflowSpecOptions(BaseModel):
    lazy: bool = False
    debug: bool = False
    profile: bool = False
    validate_type_annotations: bool = True
    default_resources: Resources | None = None
    scope: str | None = None

    model_config = {"extra": "forbid"}


class InputItem(BaseModel):
    description: str | None = None
    type: InputTypeEnum | None = None
    value: Any | None = None

    @classmethod
    @model_validator(mode="before")
    def coerce_from_shorthand(cls, data):
        if isinstance(data, str):
            return {"value": data}
        return data

    def model_dump(
        self,
        *,
        mode: Literal["json", "python"] | str = "python",
        include: IncEx | None = None,
        exclude: IncEx | None = None,
        context: Any | None = None,
        by_alias: bool | None = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool | Literal["none", "warn", "error"] = True,
        fallback: Callable[[Any], Any] | None = None,
        serialize_as_any: bool = False,
    ) -> dict[str, Any]:
        return self.value


class MapMode(str, Enum):
    NONE = "none"
    BROADCAST = "broadcast"
    ZIP = "zip"
    AGGREGATE = "aggregate"


class StepOptions(BaseModel):
    func: Callable | None = None
    output_name: str | list[str] | None = None
    renames: dict[str, str] | None = Field(default_factory=dict)
    defaults: dict[str, Any] | None = Field(default_factory=dict)
    bound: dict[str, Any] | None = Field(default_factory=dict)
    profile: bool | None = None
    debug: bool | None = None
    cache: bool | None = None
    mapspec: str | None = None
    scope: str | None = None
    advanced_options: dict[str, Any] | None = None
    map_mode: MapMode | None = Field(default=MapMode.BROADCAST, exclude=True)

    model_config = {"extra": "forbid"}


class StepDefinition(BaseModel):
    name: str
    func: str | None = None
    description: str | None = None
    inputs: dict[str, InputItem | str | int | float] | None = Field(
        default_factory=dict
    )
    parameters: dict[str, Any] | None = Field(default_factory=dict)
    outputs: str | list[str] | None = None
    resources: Resources | None = None
    options: StepOptions | None = Field(default_factory=StepOptions)


class WorkflowSpec(BaseModel):
    default_module: str | None = None
    options: WorkflowSpecOptions | None | None = Field(
        default_factory=WorkflowSpecOptions
    )
    inputs: dict[str, InputItem | str] | None = Field(default_factory=dict)
    outputs: dict[str, Path] | None = Field(default_factory=dict)
    steps: list[StepDefinition] = Field(min_length=1)

    model_config = {"extra": "forbid"}

    def model_dump(self, **kwargs: dict[str, Any] | None) -> dict[str, Any]:
        base = super().model_dump(**kwargs)
        base["inputs"] = {k: v.value for k, v in self.inputs.items()}
        return base


class WorkflowDefinition(BaseModel):
    apiVersion: str = Field(
        pattern=r"^flowfunc\.dev\/v[0-9]+(?:alpha[0-9]+|beta[0-9]+)?$"
    )
    kind: KindEnum
    metadata: Metadata
    spec: WorkflowSpec

    model_config = {"extra": "forbid"}
