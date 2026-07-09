"""Unit tests for the Ray Data usage-stats collector."""

import json
import sys
from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.issue_detection.issue_detector import IssueType
from ray.data._internal.usage import collector
from ray.data._internal.usage.execution_callback import UsageCallback


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
def executor():
    # A fake executor with no issue detection registered, standing in for the
    # StreamingExecutor the callback receives at runtime.
    executor = MagicMock()
    executor.issue_detector_manager = None
    return executor


@pytest.fixture
def reset_collector(monkeypatch):
    collector.reset_for_testing()
    monkeypatch.delenv("RAY_DATA_USAGE_DISABLED", raising=False)
    # ``ray.init()`` force-sets # RAY_USAGE_STATS_ENABLED=0 for driver-created clusters, so the env var can't
    # keep the collector's opt-out gate open. Patch the gate directly instead.
    monkeypatch.setattr(collector, "usage_stats_enabled", lambda: True)
    # The callback reads these module-level cluster metric readers; pin them to
    # zero so tests never touch the real cluster and deltas are deterministic.
    monkeypatch.setattr(collector, "cluster_spilled_bytes", lambda: 0)
    monkeypatch.setattr(collector, "cluster_dead_node_count", lambda: 0)
    yield
    collector.reset_for_testing()


def test_round_trip_payload_shape(reset_collector, mock_record, executor):
    """End-to-end: a full callback lifecycle yields a valid payload with
    anonymized plan tree, plan_str, env, and performance filled in."""
    ds = ray.data.range(1).map_batches(lambda b: b)
    callback = UsageCallback(ds._logical_plan)
    callback.before_execution_starts(executor)
    callback.after_execution_succeeds(executor)

    _, payload_json = mock_record[-1]
    payload = json.loads(payload_json)
    entry = payload["executions"][0]
    assert entry["id"] == callback._execution_id
    read_usage_id = collector.make_usage_op_id(0, "ReadRange")
    map_batches_usage_id = collector.make_usage_op_id(1, "MapBatches")
    assert entry["workload"]["plan"] == {
        "usage_id": map_batches_usage_id,
        "op": "MapBatches",
        "inputs": [{"usage_id": read_usage_id, "op": "ReadRange", "inputs": []}],
    }
    assert [(op["usage_id"], op["name"]) for op in entry["workload"]["ops"]] == [
        (read_usage_id, "ReadRange"),
        (map_batches_usage_id, "MapBatches"),
    ]
    assert entry["workload"]["plan_str"] == "MapBatches\n+- ReadRange\n"
    assert "pyarrow" in entry["env"]
    # Performance deltas are populated from the injected readers; both readers
    # return 0 at start and end, so the clamped deltas are 0.
    assert entry["performance"]["bytes_spilled"] == 0
    assert entry["performance"]["node_deaths"] == 0
    # No issues detected in this run; the key is present and empty.
    assert entry["detected_issues"] == []


def test_detected_issues_in_payload(reset_collector, mock_record, monkeypatch):
    """Detected issues are recorded as (issue_type, operator) pairs, serialized
    as a list of ``{"issue_type", "operator"}`` objects in the payload."""
    monkeypatch.setattr(
        collector,
        "physical_op_name_with_id",
        lambda operator, usage_id_map=None, op_name_fn=None: operator,
    )
    executor = MagicMock()
    executor.issue_detector_manager.get_detected_issues.return_value = [
        (IssueType.HANGING, "MapBatches"),
        (IssueType.HIGH_MEMORY, "ReadRange"),
    ]
    ds = ray.data.range(1).map_batches(lambda b: b)
    callback = UsageCallback(ds._logical_plan)
    callback.before_execution_starts(executor)
    callback.after_execution_succeeds(executor)

    _, payload_json = mock_record[-1]
    entry = json.loads(payload_json)["executions"][0]
    assert entry["detected_issues"] == [
        {"issue_type": "hanging", "operator": "MapBatches"},
        {"issue_type": "high memory", "operator": "ReadRange"},
    ]


def test_build_usage_id_map(reset_collector, mock_record):
    ds = ray.data.range(1).map_batches(lambda b: b)
    usage_id_map = collector.build_usage_id_map(ds._logical_plan)

    map_batches_op = ds._logical_plan.dag
    read_op = map_batches_op.input_dependencies[0]
    assert usage_id_map[id(read_op)] == collector.make_usage_op_id(0, "ReadRange")
    assert usage_id_map[id(map_batches_op)] == collector.make_usage_op_id(
        1, "MapBatches"
    )


def test_self_zip_one_usage_id_per_operator(reset_collector, mock_record, executor):
    """``ds.zip(ds)`` reuses the same logical operator instances across both zip
    branches (a shared-node DAG). Each discrete operator must be assigned
    exactly one usage_id."""
    ds = ray.data.range(1).map_batches(lambda b: b)
    zipped = ds.zip(ds)

    callback = UsageCallback(zipped._logical_plan)
    callback.before_execution_starts(executor)
    usage_id_map = collector.build_usage_id_map(zipped._logical_plan)

    _, payload_json = mock_record[-1]
    entry = json.loads(payload_json)["executions"][0]

    recorded_ids = [op["usage_id"] for op in entry["workload"]["ops"]]
    # build_usage_id_map is keyed by operator id, so its length is the number
    # of discrete operators. Each should map to exactly one recorded ID.
    num_discrete_ops = len(usage_id_map)
    assert len(recorded_ids) == len(set(recorded_ids)) == num_discrete_ops


def test_detected_issues_absent_defaults_empty(reset_collector, mock_record, executor):
    """A run with no detected issues leaves detected_issues empty."""
    ds = ray.data.range(1)
    callback = UsageCallback(ds._logical_plan)
    callback.before_execution_starts(executor)
    callback.after_execution_succeeds(executor)

    _, payload_json = mock_record[-1]
    entry = json.loads(payload_json)["executions"][0]
    assert entry["detected_issues"] == []


def test_unknown_operators_anonymized(reset_collector):
    """Custom, user-defined operators / datasources / datasinks defined outside
    ``ray.data.*`` must collapse to sentinel names (``Unknown`` /
    ``ReadCustom`` / ``WriteCustom``) so user-defined class names never
    leak into the payload."""
    from ray.data._internal.logical.operators import Read, Write
    from ray.data.datasource.datasink import Datasink
    from ray.data.datasource.datasource import Datasource

    # Arbitrary LogicalOperator subclass defined in user code.
    class FakeOp:
        name = "FakeOp"
        input_dependencies = []

    assert (
        collector.anonymize_op_name(FakeOp())  # pyrefly: ignore[bad-argument-type]
        == "Unknown"
    )

    # User-defined Datasource: real class living outside ray.data.* should
    # appear as "ReadCustom", not "FakeDatasource".
    class FakeDatasource(Datasource):
        pass

    read_op = Read.__new__(Read)
    object.__setattr__(read_op, "datasource", FakeDatasource())
    assert collector.anonymize_op_name(read_op) == "ReadCustom"

    # User-defined Datasink: same guarantee on the write side.
    class FakeDatasink(Datasink):
        def write(self, blocks, ctx):
            pass

    write_op = Write.__new__(Write)
    object.__setattr__(write_op, "datasink_or_legacy_datasource", FakeDatasink())
    assert collector.anonymize_op_name(write_op) == "WriteCustom"


def test_limit_anonymized_to_class_name(reset_collector, executor):
    """Limit's runtime name embeds the row count (e.g. ``limit=10``); telemetry
    must collapse it back to ``Limit`` so the value isn't recorded."""
    ds = ray.data.range(100).limit(10)
    callback = UsageCallback(ds._logical_plan)
    callback.before_execution_starts(executor)
    entry = collector.get_executions()[callback._execution_id]
    plan_ops = [op.name for op in entry.workload.ops]
    assert "Limit" in plan_ops
    assert not any(op.startswith("limit=") for op in plan_ops)


def test_does_not_record_when_disabled_via_env_var(
    reset_collector, mock_record, monkeypatch, executor
):
    """Privacy gate: RAY_DATA_USAGE_DISABLED=1 must produce zero side effects."""
    monkeypatch.setenv("RAY_DATA_USAGE_DISABLED", "1")
    ds = ray.data.range(10)
    callback = UsageCallback(ds._logical_plan)
    callback.before_execution_starts(executor)
    callback.after_execution_succeeds(executor)

    assert mock_record == []
    assert collector.get_executions() == {}


def test_does_not_record_when_usage_stats_opted_out(
    reset_collector, mock_record, monkeypatch, executor
):
    """Privacy gate: opting out of Ray usage stats (RAY_USAGE_STATS_ENABLED=0,
    ``ray disable-usage-stats``, etc.) must also disable Ray Data collection."""
    monkeypatch.setattr(collector, "usage_stats_enabled", lambda: False)
    ds = ray.data.range(10)
    callback = UsageCallback(ds._logical_plan)
    callback.before_execution_starts(executor)
    callback.after_execution_succeeds(executor)

    assert mock_record == []
    assert collector.get_executions() == {}


def test_does_not_raise_on_internal_errors(
    reset_collector, mock_record, monkeypatch, executor
):
    """Safety: a bug in collection must never break user execution."""
    monkeypatch.setattr(
        collector,
        "collect_workload",
        lambda *_: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    ds = ray.data.range(10)
    callback = UsageCallback(ds._logical_plan)
    callback.before_execution_starts(executor)  # must not raise
    assert mock_record == []


def test_physical_op_name_joins_fused_logical_ops(monkeypatch):
    """A fused physical op maps to multiple logical ops; their anonymized names
    are joined with "->", matching operator fusion's naming."""
    monkeypatch.setattr(collector, "anonymize_op_name", lambda op: op)
    operator = MagicMock()
    operator._logical_operators = ["ReadParquet", "MapBatches", "Filter"]
    assert (
        collector.physical_op_name_with_id(operator)
        == "ReadParquet->MapBatches->Filter"
    )


def test_physical_op_name_includes_usage_ids(monkeypatch):
    monkeypatch.setattr(collector, "anonymize_op_name", lambda op: op)
    operator = MagicMock()
    operator._logical_operators = ["ReadParquet", "MapBatches", "Filter"]
    usage_id_map = {
        id(operator._logical_operators[0]): "aaaaaaaa",
        id(operator._logical_operators[1]): "bbbbbbbb",
        id(operator._logical_operators[2]): "cccccccc",
    }
    assert (
        collector.physical_op_name_with_id(operator, usage_id_map)
        == "ReadParquet-aaaaaaaa->MapBatches-bbbbbbbb->Filter-cccccccc"
    )


def test_physical_op_name_without_logical_ops():
    """An operator with no logical source collapses to "Unknown"."""
    operator = MagicMock()
    operator._logical_operators = []
    assert collector.physical_op_name_with_id(operator) == "Unknown"


def test_compute_delta():
    """Cluster metric deltas (bytes_spilled, node_deaths) are the non-negative
    increase between start and end samples, or None if either sample is missing."""
    assert collector.compute_delta(100, 250) == 150
    # Cumulative counters shouldn't go backwards, but clamp to 0 if they do.
    assert collector.compute_delta(250, 100) == 0
    assert collector.compute_delta(None, 100) is None
    assert collector.compute_delta(100, None) is None
    assert collector.compute_delta(None, None) is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
