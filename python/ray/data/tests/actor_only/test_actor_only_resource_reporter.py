"""Unit tests for ``ActorOnlyResourceReporter`` and its wiring into the cluster
autoscaler's utilization gauge. Fakes only; no cluster."""
import sys
from types import SimpleNamespace
from typing import Any, FrozenSet, List, cast

import pytest

from ray.data._internal.cluster_autoscaler import (
    default_cluster_autoscaler_v2 as autoscaler_module,
)
from ray.data._internal.cluster_autoscaler.default_cluster_autoscaler_v2 import (
    DefaultClusterAutoscalerV2,
    _NodeResourceSpec,
)
from ray.data._internal.cluster_autoscaler.fake_autoscaling_coordinator import (
    FakeAutoscalingCoordinator,
)
from ray.data._internal.cluster_autoscaler.resource_utilization_gauge import (
    ResourceReporter,
    RollingLogicalUtilizationGauge,
)
from ray.data._internal.execution.interfaces.execution_options import ExecutionResources
from ray.data._internal.execution.resource_bank import LiveObjStore
from ray.data._internal.experimental.execution.actor_only_resource_reporter import (
    ActorOnlyResourceReporter,
)


class _ReporterOp:
    """Duck-typed operator exposing only what the reporter reads."""

    def __init__(self, running: ExecutionResources, pending: ExecutionResources):
        self._running = running
        self._pending = pending

    def running_logical_usage(self) -> ExecutionResources:
        return self._running

    def pending_logical_usage(self) -> ExecutionResources:
        return self._pending


class _FakeResourceBank:
    """Per-op live object store as ``(input, pulled, prebuffered)`` bytes;
    missing ops report zero."""

    def __init__(self, per_op_bytes=None):
        self._per_op = per_op_bytes or {}

    def __new__(cls, *args, **kwargs) -> Any:
        return super().__new__(cls)

    def live_object_store(self, *, op=None, node_id=None, actor_id=None):
        in_b, pulled, prebuffered = self._per_op.get(op, (0, 0, 0))
        return LiveObjStore(
            num_input_bytes=in_b,
            num_pulled_output_bytes=pulled,
            num_prebuffered_output_bytes=prebuffered,
        )


def _topology(*ops) -> Any:
    # The reporter only iterates topology keys; the OpState value is unused.
    return cast(Any, {op: None for op in ops})


def _coordinator_with(bundles, by_node=None) -> FakeAutoscalingCoordinator:
    """A fake coordinator holding an unexpired allocation of ``bundles`` (and,
    optionally, the per-node view the fake serves for it)."""
    coordinator = FakeAutoscalingCoordinator(initial_cluster_resources_by_node=by_node)
    coordinator.request_resources(
        resources=bundles, expire_after_s=1e9, request_remaining=[]
    )
    return coordinator


def test_reporter_satisfies_the_gauge_protocol():
    reporter = ActorOnlyResourceReporter(
        _topology(), FakeAutoscalingCoordinator(), _FakeResourceBank()
    )
    assert isinstance(reporter, ResourceReporter)


def test_usage_pending_and_limits():
    op1 = _ReporterOp(
        running=ExecutionResources(cpu=2, gpu=0, memory=100),
        pending=ExecutionResources(cpu=4),
    )
    op2 = _ReporterOp(
        running=ExecutionResources(cpu=3, gpu=1, memory=200),
        pending=ExecutionResources(cpu=1, gpu=2, memory=50),
    )
    bank = _FakeResourceBank({op1: (10, 20, 5), op2: (5, 0, 0)})
    coordinator = _coordinator_with([{"CPU": 4, "GPU": 1}, {"CPU": 8}])
    reporter = ActorOnlyResourceReporter(_topology(op1, op2), coordinator, bank)

    usage = reporter.get_global_usage()
    assert (usage.cpu, usage.gpu, usage.memory) == (5, 1, 300)
    # Object store is the bank's live bytes: (10+20+5) + (5+0+0).
    assert usage.object_store_memory == 40

    pending = reporter.get_global_pending_usage()
    assert (pending.cpu, pending.gpu, pending.memory) == (5, 2, 50)

    limits = reporter.get_global_limits()
    assert (limits.cpu, limits.gpu) == (12, 1)


def test_empty_topology_and_no_allocation_report_zero():
    reporter = ActorOnlyResourceReporter(
        _topology(), FakeAutoscalingCoordinator(), _FakeResourceBank()
    )
    assert reporter.get_global_usage() == ExecutionResources.zero()
    assert reporter.get_global_pending_usage() == ExecutionResources.zero()
    assert reporter.get_global_limits() == ExecutionResources.zero()
    assert reporter.get_reserved_resources_by_node() == {}


def test_per_node_allocation_and_change_tracking():
    coordinator = _coordinator_with(
        [{"CPU": 1}],
        by_node={"node0": {"CPU": 4, "GPU": 1}, "node1": {"CPU": 8}},
    )
    reporter = ActorOnlyResourceReporter(_topology(), coordinator, _FakeResourceBank())

    by_node = reporter.get_reserved_resources_by_node()
    assert set(by_node) == {"node0", "node1"}
    assert (by_node["node0"].cpu, by_node["node0"].gpu) == (4, 1)
    assert by_node["node1"].cpu == 8
    assert all(isinstance(v, ExecutionResources) for v in by_node.values())

    # The first read establishes the allocation; an unchanged re-read does not
    # reset the change clock, a changed one does.
    age_after_first = reporter.seconds_since_allocation_change()
    reporter.get_reserved_resources_by_node()
    assert reporter.seconds_since_allocation_change() >= age_after_first

    coordinator._initial_cluster_resources_by_node["node1"] = {"CPU": 2}
    reporter.get_reserved_resources_by_node()
    assert reporter.seconds_since_allocation_change() < age_after_first + 1.0

    view = reporter.get_cluster_view()
    assert view.reserved_by_node["node1"].cpu == 2
    assert view.labels_by_node == {} and view.pg_bundles == {}


def _fake_pg(id_hex: str, bundle_specs=()) -> Any:
    return SimpleNamespace(
        id=SimpleNamespace(hex=lambda: id_hex), bundle_specs=list(bundle_specs)
    )


@pytest.mark.parametrize(
    "pg_ids, expected",
    [
        # Several ops sharing one group register it once.
        (["pg1", "pg1"], {"pg1"}),
        # Every group the dataset uses is registered.
        (["pg1", "pg2"], {"pg1", "pg2"}),
        ([], set()),
    ],
)
def test_register_placement_groups(pg_ids, expected):
    coordinator = FakeAutoscalingCoordinator()
    reporter = ActorOnlyResourceReporter(_topology(), coordinator, _FakeResourceBank())
    reporter.register_placement_groups([_fake_pg(pg_id) for pg_id in pg_ids])
    assert coordinator.placement_group_ids == expected


class _RecordingCoordinator(FakeAutoscalingCoordinator):
    """Records every placement-group registration it is sent."""

    def __init__(self):
        super().__init__()
        self.sent: List[FrozenSet[str]] = []

    def register_placement_groups(self, placement_group_ids):
        self.sent.append(frozenset(placement_group_ids))
        super().register_placement_groups(placement_group_ids)


def test_register_placement_groups_sends_only_changes():
    """Re-registering the same set sends nothing; a new group sends the whole
    set again."""
    coordinator = _RecordingCoordinator()
    reporter = ActorOnlyResourceReporter(_topology(), coordinator, _FakeResourceBank())

    reporter.register_placement_groups([_fake_pg("pg1")])
    reporter.register_placement_groups([_fake_pg("pg1")])
    reporter.register_placement_groups([_fake_pg("pg1"), _fake_pg("pg2")])
    assert coordinator.sent == [frozenset({"pg1"}), frozenset({"pg1", "pg2"})]


def test_cache_pg_bundle_shapes_reads_bundle_specs():
    """Shapes come from the group's own bundle specs, in bundle-index order."""
    reporter = ActorOnlyResourceReporter(
        _topology(), FakeAutoscalingCoordinator(), _FakeResourceBank()
    )
    reporter.cache_pg_bundle_shapes(
        [_fake_pg("pg1", [{"CPU": 1, "GPU": 1}, {"CPU": 2}])]
    )
    view = reporter.get_cluster_view(include_pg_bundles=True)
    assert view.pg_bundles == {
        "pg1": [ExecutionResources(cpu=1, gpu=1), ExecutionResources(cpu=2)]
    }


def test_cluster_view_carries_placement_group_part():
    """``reserved_by_node`` is the whole allocation; ``pg_reserved_by_node``
    is the part the coordinator says the registered groups hold."""
    coordinator = FakeAutoscalingCoordinator(
        initial_cluster_resources_by_node={"g1": {"CPU": 6}, "c1": {"CPU": 8}},
        placement_group_resources_by_node={"g1": {"CPU": 2, "GPU": 2}},
    )
    coordinator.request_resources([{"CPU": 1}], expire_after_s=60)
    reporter = ActorOnlyResourceReporter(_topology(), coordinator, _FakeResourceBank())

    view = reporter.get_cluster_view()
    assert view.reserved_by_node == {
        "g1": ExecutionResources(cpu=8, gpu=2),
        "c1": ExecutionResources(cpu=8),
    }
    assert view.pg_reserved_by_node == {"g1": ExecutionResources(cpu=2, gpu=2)}
    assert reporter.get_global_limits() == ExecutionResources(cpu=16, gpu=2)


@pytest.mark.parametrize("cpu_running,should_scale_up", [(4.0, False), (8.0, True)])
def test_reporter_drives_cluster_autoscaler_scale_up(
    monkeypatch, cpu_running, should_scale_up
):
    """reporter -> gauge -> DefaultClusterAutoscalerV2: limits come from the
    reporter's coordinator allocation (10 CPU), usage from the operator. 8/10
    crosses the 0.75 threshold and requests nodes; 4/10 does not."""
    # is_autoscaling_enabled() hits the GCS; it only affects log level.
    monkeypatch.setattr(autoscaler_module, "is_autoscaling_enabled", lambda: True)

    op = _ReporterOp(
        running=ExecutionResources(cpu=cpu_running), pending=ExecutionResources.zero()
    )
    reporter = ActorOnlyResourceReporter(
        _topology(op), _coordinator_with([{"CPU": 10}]), _FakeResourceBank()
    )
    # A separate coordinator receives the autoscaler's scale-up requests.
    request_coordinator = FakeAutoscalingCoordinator()
    node_spec = _NodeResourceSpec.of(cpu=10, gpu=0, mem=0)
    autoscaler = DefaultClusterAutoscalerV2(
        resource_manager=cast(Any, None),  # unused by the gauge path
        execution_id="test-actor-only",
        resource_utilization_calculator=RollingLogicalUtilizationGauge(reporter),
        cluster_scaling_up_util_threshold=0.75,
        cluster_scaling_up_delta=1,
        min_gap_between_autoscaling_requests_s=0,
        autoscaling_coordinator=request_coordinator,
        get_node_counts=lambda: {node_spec: 1},
    )

    autoscaler.try_trigger_scaling()

    allocated = autoscaler.get_total_resources()
    if should_scale_up:
        # 1 existing node + 1 scale-up delta = 20 CPU requested.
        assert allocated.cpu == 20
    else:
        assert allocated == ExecutionResources.zero()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
