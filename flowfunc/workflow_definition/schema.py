from __future__ import annotations

import logging
import re
from enum import Enum
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from pydantic.main import IncEx
from collections.abc import Callable

logger = logging.getLogger(__name__)

DEPENDENCY_STRING_PATTERN = re.compile(
    r"^{{\s*steps\.([a-z0-9_]+)\.produces\.(\w+)\s*}}$"
)


class KindEnum(str, Enum):
    PIPELINE = "Pipeline"
    WORKFLOW = "Workflow"


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
    resources: dict[str, Any] = Field(default_factory=dict)
    advanced_options: dict[str, Any] | None = Field(default_factory=dict, exclude=True)
    map_mode: MapMode | None = Field(default=MapMode.BROADCAST, exclude=True)

    model_config = {"extra": "forbid"}


class StepDefinition(BaseModel):
    name: str
    func: str | None = None
    description: str | None = None
    params: dict[str, Any] | None = Field(default_factory=dict)
    consumes: dict[str, str] | None = Field(default_factory=dict)
    produces: str | list[str] | None = None
    resources: Resources | None = None
    options: StepOptions | None = Field(default_factory=StepOptions)

    @field_validator("consumes")
    @classmethod
    def validate_consumes_format(
        cls, v: dict[str, str] | None
    ) -> dict[str, str] | None:
        """Ensures that dependency strings are correctly formatted."""
        if v is None:
            return None
        for arg_name, dependency_str in v.items():
            if not DEPENDENCY_STRING_PATTERN.match(dependency_str):
                raise ValueError(
                    f"Invalid format for 'consumes' argument '{arg_name}'. "
                    f"Expected '{{{{ steps.<step_name>.produces.<output_name> }}}}', "
                    f"but got '{dependency_str}'."
                )
        return v


class WorkflowSpec(BaseModel):
    default_module: str | None = None
    options: WorkflowSpecOptions | None | None = Field(
        default_factory=WorkflowSpecOptions
    )
    params: dict[str, Any] | None = Field(default_factory=dict)
    artifacts: dict[str, str] | None = Field(default_factory=dict)
    steps: list[StepDefinition] = Field()

    model_config = {"extra": "forbid"}

    @field_validator("artifacts")
    @classmethod
    def validate_artifacts_format(
        cls, v: dict[str, str] | None
    ) -> dict[str, str] | None:
        """Ensures that artifact source strings are correctly formatted."""
        if v is None:
            return None
        for artifact_path, source_str in v.items():
            print(f"{artifact_path=} -> {source_str=}")
            if not DEPENDENCY_STRING_PATTERN.match(source_str):
                raise ValueError(
                    f"Invalid source format for artifact '{artifact_path}'. "
                    f"Expected '{{{{ steps.<step_name>.produces.<output_name> }}}}', "
                    f"but got '{source_str}'."
                )
        return v


class WorkflowDefinition(BaseModel):
    apiVersion: str = Field(
        pattern=r"^flowfunc\.dev\/v[0-9]+(?:alpha[0-9]+|beta[0-9]+)?$"
    )
    kind: KindEnum
    metadata: Metadata
    spec: WorkflowSpec

    model_config = {"extra": "forbid"}
