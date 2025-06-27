from __future__ import annotations

import logging
import string
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

from flowfunc.pipeline.builder import PipelineBuildError
from flowfunc.utils.python import import_callable

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
    BROADCAST = "broadcast"
    ZIP = "zip"
    AGGREGATE = "aggregate"


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
    map_mode: MapMode | None = MapMode.BROADCAST

    model_config = {"extra": "forbid"}


class StepDefinition(BaseModel):
    name: str
    func: str | None = None
    description: str | None = None
    inputs: dict[str, InputItem | str | int | float] | None = Field(
        default_factory=dict
    )
    parameters: dict[str, Any] | None = Field(default_factory=dict)
    output_name: str | None = None
    resources: Resources | None = None
    options: StepOptions | None = Field(default_factory=StepOptions)

    def _get_callable(self, default_module: str | None) -> Callable:
        function_path_str = self.func
        if not function_path_str:
            if default_module and self.name:
                function_path_str = f"{default_module}.{self.name}"
                logger.debug(
                    f"Step '{self.name}': 'func' not specified. Defaulting to '{function_path_str}' "
                    f"using default_module and step name."
                )
            else:
                error_msg_parts = []
                if not default_module:
                    error_msg_parts.append(
                        "no 'default_module' is specified in the workflow"
                    )
                if not self.name:
                    error_msg_parts.append("'name' is missing for the step")
                detail = (
                    " and ".join(error_msg_parts)
                    if error_msg_parts
                    else "required information missing"
                )
                raise PipelineBuildError(
                    f"Step '{self.name or '(unnamed step)'}': 'func' is not specified and "
                    f"cannot be defaulted because {detail}."
                )
        try:
            if not isinstance(function_path_str, str):
                raise TypeError(
                    f"'func' path must be a string, got {type(function_path_str)} for step '{self.name}'"
                )
            return import_callable(function_path_str)
        except (ImportError, AttributeError, TypeError) as e:
            raise PipelineBuildError(
                f"Could not import callable '{function_path_str}' for step '{self.name}': {e}"
            ) from e

    def _get_pipefunc_renames(self) -> dict[str, str]:
        renames: dict[str, str] = {}
        if self.inputs:
            for name, input_item in self.inputs.items():
                input_value = (
                    input_item.value
                    if isinstance(input_item, InputItem)
                    else input_item
                )
                if input_value.startswith("$global."):
                    global_var_name = ".".join(input_value.split(".", 1)[1:])
                    if name != global_var_name:
                        renames[name] = global_var_name
        return renames

    def _get_pipefunc_defaults(self) -> dict[str, Any]:
        return self.parameters.copy() if self.parameters else {}

    def _get_pipefunc_mapspec(self) -> str | None:
        """
        Generates a `mapspec` string based on step inputs, outputs, and a `map_mode`.

        This function makes it easier to handle common mapping patterns (broadcasting,
        zipping, aggregation) without writing a full `mapspec` string manually.
        The generation is controlled by `options.map_mode`.

        The function will NOT generate a `mapspec` if:
        - One is already defined in `options.mapspec`.
        - No iterable inputs are found.
        - The step has no `output_name` defined.

        The `map_mode` option in the step's `options` block dictates the pattern:
        - `broadcast` (Default): Creates a cartesian product of all iterable inputs.
          Each iterable gets a unique index (`i`, `j`, ...).
          e.g., `inputs: {a: "$global.x", b: "$global.y"}` -> `x[i], y[j] -> out[i,j]`

        - `zip`: Iterates over all iterable inputs in parallel. All iterables
          share the same index (`i`). This is for 1-to-1 processing of multiple lists.
          e.g., `inputs: {a: "$global.x", b: "$global.y"}` -> `x[i], y[i] -> out[i]`

        - `aggregate`: Consumes an iterable input to produce a single, aggregated output.
          The input has an index, but the output does not.
          e.g., `inputs: {a: "step.results"}` -> `results[i] -> summary`

        Iterable vs. Constant Inputs:
        - An input is "iterable" if its value is a string reference like `"$global.my_list"`
          or `"previous_step.output_list"`.
        - An input is "constant" if its value is a literal (e.g., `10`, `"foo"`, `true`).
          Constants are passed as-is to every function call.
        """
        if self.options and self.options.mapspec:
            return self.options.mapspec

        if not self.inputs or not self.output_name:
            return None

        map_mode = self.options.map_mode if self.options else MapMode.BROADCAST

        iterable_inputs = {}
        constant_inputs = []
        for input_name, input_value in self.inputs.items():
            if isinstance(input_value, str) and (
                input_value.startswith("$global.") or "." in input_value
            ):
                source_name = input_value.split(".", 1)[1]
                iterable_inputs[input_name] = source_name
            else:
                constant_inputs.append(input_name)

        if not iterable_inputs:
            return None

        indices = list(string.ascii_lowercase[8:])
        input_parts = []
        output_names = (
            [self.output_name]
            if isinstance(self.output_name, str)
            else self.output_name
        )

        match map_mode:
            case MapMode.BROADCAST:
                output_indices = []
                for i, source_name in enumerate(iterable_inputs.values()):
                    if i >= len(indices):
                        raise PipelineBuildError(
                            "Too many iterable inputs for `broadcast`."
                        )
                    index = indices[i]
                    input_parts.append(f"{source_name}[{index}]")
                    output_indices.append(index)

                output_index_str = ",".join(output_indices)
                output_parts = [f"{name}[{output_index_str}]" for name in output_names]
                outputs_str = ", ".join(output_parts)

            case MapMode.ZIP:
                index = indices[0]
                for source_name in iterable_inputs.values():
                    input_parts.append(f"{source_name}[{index}]")

                output_parts = [f"{name}[{index}]" for name in output_names]
                outputs_str = ", ".join(output_parts)

            case MapMode.AGGREGATE:
                index = indices[0]
                for source_name in iterable_inputs.values():
                    input_parts.append(f"{source_name}[{index}]")

                # For aggregation, the output has no indices
                outputs_str = ", ".join(output_names)

            case _:
                raise ValueError(
                    f"Unhandled MapMode: {map_mode}. This should not happen."
                )

        input_parts.extend(constant_inputs)
        inputs_str = ", ".join(input_parts)
        mapspec_string = f"{inputs_str} -> {outputs_str}"

        logger.info(
            f"Step '{self.name}': Automatically generated mapspec "
            f"using mode '{map_mode.value}': '{mapspec_string}'"
        )

        return mapspec_string

    def _get_pipefunc_resources(
        self, global_resources: Resources | None
    ) -> dict[str, Any]:
        global_res_dict = (
            global_resources.model_dump(exclude_none=True) if global_resources else {}
        )
        step_res_dict = (
            self.resources.model_dump(exclude_none=True) if self.resources else {}
        )
        merged_resources = {**global_res_dict, **step_res_dict}
        advanced_opts_dict = merged_resources.pop("advanced_options", {}) or {}
        flattened_resources: dict[str, Any] = {}
        if merged_resources.get("cpus") is not None:
            flattened_resources["cpus"] = merged_resources.get("cpus")
        if merged_resources.get("memory") is not None:
            flattened_resources["memory"] = merged_resources.get("memory")
        flattened_resources.update(advanced_opts_dict)
        return flattened_resources

    def _get_pipefunc_scope(self) -> str | None:
        return self.options.scope if self.options else None

    def to_pipefunc_options(
        self,
        workflow_default_module: str | None,
        workflow_global_resources: Resources | None,
    ) -> dict[str, Any]:
        """Generates the keyword arguments dictionary for instantiating a pipefunc.PipeFunc."""
        pf_options: dict[str, Any] = (
            self.options.model_dump(
                exclude_none=True, by_alias=True, exclude={"map_mode"}
            )
            if self.options
            else {}
        )

        if self.output_name:
            pf_options["output_name"] = self.output_name
        elif self.name:  # Default to step name if no explicit outputs list
            pf_options["output_name"] = self.name
        else:  # Should be caught by Step validation if name is required
            raise PipelineBuildError(
                "Step must have a name to determine default PipeFunc output."
            )

        pf_options["func"] = self._get_callable(workflow_default_module)

        renames = self._get_pipefunc_renames()
        if renames:
            pf_options["renames"] = {**pf_options.get("renames", {}), **renames}

        defaults = self._get_pipefunc_defaults()
        if defaults:
            pf_options["defaults"] = {**pf_options.get("defaults", {}), **defaults}

        mapspec = self._get_pipefunc_mapspec()
        if mapspec:
            pf_options["mapspec"] = mapspec

        resources = self._get_pipefunc_resources(workflow_global_resources)
        if resources:
            pf_options["resources"] = resources

        scope = self._get_pipefunc_scope()
        if scope is not None:
            # Important to only set if not None, to allow PipeFunc default behavior
            pf_options["scope"] = scope

        return pf_options


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

    def get_pipeline_constructor_kwargs(self) -> dict[str, Any]:
        """Generates the keyword arguments for instantiating a pipefunc.Pipeline."""
        kwargs: dict[str, Any] = {}
        if self.spec.options:
            options_data = self.spec.options.model_dump(
                exclude_none=True, by_alias=True
            )
            # These are direct kwargs for pipefunc.Pipeline constructor
            valid_args = {
                "lazy",
                "debug",
                "profile",
                "cache_type",
                "cache_kwargs",
                "validate_type_annotations",
                # "scope" and "default_resources" are handled specifically below for clarity/precedence
            }
            for opt_name, opt_value in options_data.items():
                if opt_name in valid_args:
                    kwargs[opt_name] = opt_value

            # Handle default_resources specifically
            if self.spec.options.default_resources:
                # Pass the Pydantic model instance; pipefunc will use Resources.maybe_from_dict
                kwargs["default_resources"] = self.spec.options.default_resources

            # Handle scope specifically
            if self.spec.options.scope is not None:
                kwargs["scope"] = self.spec.options.scope

        # Override or set pipeline scope from spec.options.scope if defined
        # This gives spec.options.scope higher precedence for the pipeline's scope if both are set.
        if self.spec.options and self.spec.options.scope is not None:
            kwargs["scope"] = self.spec.options.scope

        return kwargs
