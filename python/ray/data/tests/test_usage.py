"""Unit tests for the Ray Data usage-stats collector."""

import json
import sys

import pytest

import ray
from ray.data._internal.usage import collector


@pytest.fixture
def mock_record(monkeypatch):
    recorded = []
    monkeypatch.setattr(
        collector,
        "record_extra_usage_tag",
        lambda key, value: recorded.append((key, value)),
    )
    return recorded


@pytest.fixture
def reset_collector(monkeypatch):
    collector.reset_for_testing()
    monkeypatch.setattr(collector, "_cluster_spilled_bytes", lambda: 0)
    monkeypatch.delenv("RAY_DATA_USAGE_DISABLED", raising=False)
    yield
    collector.reset_for_testing()


def test_round_trip_payload_shape(reset_collector, mock_record):
    """End-to-end: record_workload, record_execution_result yields a valid
    payload with anonymized plan, env, and performance filled in."""
    ds = ray.data.range(1).map_batches(lambda b: b)
    collector.record_workload("exec-1", ds._logical_plan)
    collector.record_execution_result("exec-1")

    _, payload_json = mock_record[-1]
    payload = json.loads(payload_json)
    entry = payload["executions"][0]
    assert entry["id"] == "exec-1"
    assert entry["workload"]["plan"] == "ReadRange->MapBatches"
    assert "pyarrow" in entry["env"]


def test_unknown_operators_anonymized(reset_collector):
    """Privacy: operators not on the whitelist must collapse to ``Unknown``
    so user-defined names never leak into the payload."""

    class FakeOp:
        name = "MyCompanySecretOp"
        input_dependencies = []

    assert collector.anonymize_op_name(FakeOp()) == "Unknown"


def test_limit_anonymized_to_class_name(reset_collector):
    """Limit's runtime name embeds the row count (e.g. ``limit=10``); telemetry
    must collapse it back to ``Limit`` so the value isn't recorded."""
    ds = ray.data.range(100).limit(10)
    collector.record_workload("exec-limit", ds._logical_plan)
    entry = collector.get_executions()["exec-limit"]
    plan_ops = [op.name for op in entry.workload.ops]
    assert "Limit" in plan_ops
    assert not any(op.startswith("limit=") for op in plan_ops)


def test_read_files_anonymized_to_class_name(reset_collector):
    """ReadFiles' runtime name embeds the datasource (e.g. ``ReadFilesParquet``);
    telemetry collapses it to a single ``ReadFiles`` bucket so the datasource
    name (which may be user-defined) isn't recorded."""
    from ray.data._internal.logical.operators import ReadFiles

    class _StubReadFiles(ReadFiles):
        def __init__(self):
            pass

        @property
        def name(self):
            return "ReadFilesMyCompanyDatasource"

        @property
        def input_dependencies(self):
            return []

    assert collector._anonymize_op_name(_StubReadFiles()) == "ReadFiles"


def test_does_not_record_when_disabled_via_env_var(
    reset_collector, mock_record, monkeypatch
):
    """Privacy gate: RAY_DATA_USAGE_DISABLED=1 must produce zero side effects."""
    monkeypatch.setenv("RAY_DATA_USAGE_DISABLED", "1")
    ds = ray.data.range(10)
    collector.record_workload("exec-1", ds._logical_plan)
    collector.record_execution_result("exec-1")

    assert mock_record == []
    assert "exec-1" not in collector.get_executions()


def test_does_not_raise_on_internal_errors(reset_collector, mock_record, monkeypatch):
    """Safety: a bug in collection must never break user execution."""
    monkeypatch.setattr(
        collector,
        "_collect_workload",
        lambda *_: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    ds = ray.data.range(10)
    collector.record_workload("exec-1", ds._logical_plan)  # must not raise
    assert mock_record == []


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
