from typing import Any

import pytest

from flowfunc.composition.step import resolve_mapspec
from flowfunc.exceptions import PipelineBuildError
from flowfunc.workflow_definition import StepDefinition
from flowfunc.workflow_definition import StepOptions


class TestGetPipefuncMapspec:
    @pytest.mark.parametrize(
        ("step_data", "expected_spec"),
        [
            (
                {"inputs": {"items": "$global.my_list"}, "outputs": "results"},
                "my_list[i] -> results[i]",
            ),
            (
                {"inputs": {"items": "step1.output"}, "outputs": "results"},
                "output[i] -> results[i]",
            ),
            (
                {
                    "inputs": {
                        "items_a": "$global.list_a",
                        "items_b": "$global.list_b",
                    },
                    "outputs": "pairs",
                    "options": {"map_mode": "broadcast"},
                },
                "list_a[i], list_b[j] -> pairs[i,j]",
            ),
            (
                {
                    "inputs": {
                        "item": "$global.my_list",
                        "multiplier": 10,
                        "prefix": "p",
                    },
                    "outputs": "results",
                },
                "my_list[i], multiplier, prefix -> results[i]",
            ),
        ],
        ids=[
            "broadcast_single_iterable",
            "broadcast_single_iterable_from_step",
            "broadcast_single_iterable_broadcast_two_iterables_explicit",
            "broadcast_with_constants",
        ],
    )
    def test_mapspec_broadcast_mode(
        self, step_data: dict[str, Any], expected_spec: str
    ) -> None:
        """Tests various scenarios for 'broadcast' map_mode."""
        step = StepDefinition(name="test_step", **step_data)
        options = StepOptions(output_name=step.outputs)
        options_with_mapsec = resolve_mapspec(options, step)

        assert options_with_mapsec.mapspec == expected_spec

    @pytest.mark.parametrize(
        ("step_data", "expected_spec"),
        [
            (
                {
                    "inputs": {
                        "files": "$global.file_list",
                        "configs": "$global.config_list",
                    },
                    "outputs": "processed",
                    "options": {"map_mode": "zip"},
                },
                "file_list[i], config_list[i] -> processed[i]",
            ),
            (
                {
                    "inputs": {
                        "a": "$global.x",
                        "b": "step1.y",
                        "c": "$global.z",
                        "factor": 2,
                    },
                    "outputs": "results",
                    "options": {"map_mode": "zip"},
                },
                "x[i], y[i], z[i], factor -> results[i]",
            ),
        ],
        ids=[
            "zip_two_iterables",
            "zip_three_iterables_with_constant",
        ],
    )
    def test_mapspec_zip_mode(
        self, step_data: dict[str, Any], expected_spec: str
    ) -> None:
        """Tests various scenarios for 'zip' map_mode."""
        step = StepDefinition(name="test_step", **step_data)
        options = StepOptions(output_name=step.outputs)
        options_with_mapsec = resolve_mapspec(options, step)

        assert options_with_mapsec.mapspec == expected_spec

    @pytest.mark.parametrize(
        ("step_data", "expected_spec"),
        [
            (
                {
                    "inputs": {"items": "step1.results"},
                    "outputs": "summary",
                    "options": {"map_mode": "aggregate"},
                },
                "results[i] -> summary",
            ),
            (
                {
                    "inputs": {"items": "$global.data", "method": "average"},
                    "outputs": "result",
                    "options": {"map_mode": "aggregate"},
                },
                "data[i], method -> result",
            ),
        ],
        ids=[
            "aggregate_single_iterable",
            "aggregate_with_constant",
        ],
    )
    def test_mapspec_aggregate_mode(
        self, step_data: dict[str, Any], expected_spec: str
    ) -> None:
        """Tests various scenarios for 'aggregate' map_mode."""
        step = StepDefinition(name="test_step", **step_data)
        options = StepOptions(output_name=step.outputs)
        options_with_mapsec = resolve_mapspec(options, step)

        assert options_with_mapsec.mapspec == expected_spec

    @pytest.mark.parametrize(
        "step_data",
        [
            ({"options": {"mapspec": "explicit[i] -> user_defined[i]"}}),
            ({"inputs": {"a": 1, "b": "hello"}}),
            ({"inputs": {}}),
            ({"inputs": None}),
            ({"inputs": {"items": "$global.data"}, "outputs": None}),
        ],
        ids=[
            "return_none_if_mapspec_is_explicit",
            "return_none_for_all_constant_inputs",
            "return_none_for_empty_inputs",
            "return_none_for_null_inputs",
            "return_none_if_outputs_is_missing",
        ],
    )
    def test_mapspec_should_return_none(self, step_data: dict[str, Any]) -> None:
        """Tests scenarios where mapspec generation should be skipped."""
        step = StepDefinition(name="test_step", **step_data)
        options = StepOptions(**(step_data.get("options", {})))
        options_with_mapsec = resolve_mapspec(options, step)

        if step_data.get("options", {}).get("mapspec"):
            assert options_with_mapsec.mapspec == step_data["options"]["mapspec"]
        else:
            assert options_with_mapsec.mapspec is None

    def test_mapspec_raises_error_on_too_many_broadcast_inputs(self) -> None:
        """Tests that an error is raised when too many iterables are used in broadcast mode."""
        too_many_inputs = {f"in_{i}": f"$global.list_{i}" for i in range(30)}
        step_data = {
            "inputs": too_many_inputs,
            "outputs": "results",
            "options": {"map_mode": "broadcast", "output_name": "results"},
        }
        step = StepDefinition(name="test_too_many", **step_data)
        options = StepOptions(**(step_data.get("options", {})))
        with pytest.raises(PipelineBuildError):
            resolve_mapspec(options, step)
