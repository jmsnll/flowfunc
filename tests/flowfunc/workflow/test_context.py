import json
from datetime import datetime

import freezegun
import pytest


@pytest.fixture(scope="module", autouse=True)
def freeze_all_time():
    frozen_time = freezegun.freeze_time(datetime(2025, 1, 1, 1, 0, 0))
    frozen = frozen_time.start()
    yield
    frozen_time.stop()


def test_summarize_with_populated_run_context_check_expected_summary_fields(
    run_context,
):
    summary = run_context.summarize()

    assert summary.run_id == "test-run"
    assert summary.workflow_name == "text-analysis-sentiment-pipeline"
    assert summary.file_path == "/path/to/workflow.yaml"
    assert summary.status == "success"
    assert summary.duration_seconds == 3600
    assert summary.user_inputs["param"] == "value"
    assert summary.resolved_inputs["param"] == "resolved"
    assert summary.persisted_outputs["output"] == "result"


@pytest.mark.parametrize(
    "workflow_context", [{"with_example": "nlp_text_summarization"}], indirect=True
)
def test_save_summary_with_valid_summary_data_check_json_written_to_expected_path(
    run_context,
):
    summary_file = run_context.paths.run_dir / "summary.json"

    run_context.save_summary()

    summary_data = json.load(open(summary_file))
    assert summary_data["run_id"] == "test-run"
    assert summary_data["workflow_name"] == "text-analysis-sentiment-pipeline"
    assert summary_data["file_path"] == "/path/to/workflow.yaml"
    assert summary_data["status"] == "success"
    assert summary_data["duration_seconds"] == 3600
    assert summary_data["user_inputs"] == {
        "text": [
            "The movie was excellent! The performances were outstanding, and the plot was captivating.",
            "The movie was bad and boring. I found it dull and slow with no gripping moments.",
            "An alright film with a good sense of humor but lacking depth in character development.",
        ]
    }
    assert summary_data["resolved_inputs"] == {
        "text": [
            "The movie was excellent! The performances were outstanding, and the plot was captivating.",
            "The movie was bad and boring. I found it dull and slow with no gripping moments.",
            "An alright film with a good sense of humor but lacking depth in character development.",
        ]
    }
    assert (
        summary_data["persisted_outputs"]["result_summary"]
        == "/tmp/test-run/run/outputs/result/final_sentiment_report.json"
    )


@pytest.mark.parametrize("inputs_context", [{"user_inputs": {"x": 1}}], indirect=True)
@pytest.mark.parametrize("metadata_context", [{"run_id": "override-id"}], indirect=True)
def test_summarize_with_overrides(run_context, inputs_context, metadata_context):
    summary = run_context.summarize()
    assert summary.run_id == "override-id"
    assert summary.user_inputs == {"x": 1}
