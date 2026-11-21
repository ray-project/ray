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
    BundleKey,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (  # noqa: E501
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.placement_constraints import (  # noqa: E501
    PlacementConstraint,
)


def _reporter(topology, coordinator, bank, **kwargs) -> ActorOnlyResourceReporter:
    """A reporter with no cluster: no node labels, no draining nodes, unless
    a test injects its own."""
    kwargs.setdefault("get_node_labels", dict)
    kwargs.setdefault("get_draining_node_ids", list)
    return ActorOnlyResourceReporter(topology, coordinator, bank, **kwargs)


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
    reporter = _reporter(_topology(), FakeAutoscalingCoordinator(), _FakeResourceBank())
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
    reporter = _reporter(_topology(op1, op2), coordinator, bank)

    usage = reporter.get_global_usage()
    assert (usage.cpu, usage.gpu, usage.memory) == (5, 1, 300)
    # Object store is the bank's live bytes: (10+20+5) + (5+0+0).
    assert usage.object_store_memory == 40

    pending = reporter.get_global_pending_usage()
    assert (pending.cpu, pending.gpu, pending.memory) == (5, 2, 50)

    limits = reporter.get_global_limits()
    assert (limits.cpu, limits.gpu) == (12, 1)


def test_empty_topology_and_no_allocation_report_zero():
    reporter = _reporter(_topology(), FakeAutoscalingCoordinator(), _FakeResourceBank())
    assert reporter.get_global_usage() == ExecutionResources.zero()
    assert reporter.get_global_pending_usage() == ExecutionResources.zero()
    assert reporter.get_global_limits() == ExecutionResources.zero()
    assert reporter.get_reserved_resources_by_node() == {}


def test_per_node_allocation_and_change_tracking():
    coordinator = _coordinator_with(
        [{"CPU": 1}],
        by_node={"node0": {"CPU": 4, "GPU": 1}, "node1": {"CPU": 8}},
    )
    reporter = _reporter(_topology(), coordinator, _FakeResourceBank())

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


def _fake_pg(id_hex: str, bundle_specs=()) -> Any:
    return SimpleNamespace(
        id=SimpleNamespace(hex=lambda: id_hex), bundle_specs=list(bundle_specs)
    )


class _PGOp(ExperimentalActorPoolMapOperator):
    """An actor-pool op exposing only what the reporter reads: its static
    placement group, the ones its ``ray_remote_args_fn`` created, and its
    pool's max size. Skips the operator's ``__init__``."""

    def __init__(self, pg=None, *, fn_pgs=(), uses_fn=False, max_size=4):
        self._constraint = PlacementConstraint(
            placement_group=pg, mints_pg_per_actor=uses_fn
        )
        self.fn_pgs = list(fn_pgs)
        self._pool = SimpleNamespace(bounded_max_size=lambda: max_size)

    def placement_constraint(self) -> PlacementConstraint:
        return self._constraint

    def fn_placement_groups(self) -> List[Any]:
        return list(self.fn_pgs)

    @property
    def actor_pool(self) -> Any:
        return self._pool


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
def test_cluster_view_registers_placement_groups(pg_ids, expected):
    coordinator = FakeAutoscalingCoordinator()
    ops = [_PGOp(_fake_pg(pg_id)) for pg_id in pg_ids]
    reporter = _reporter(_topology(*ops), coordinator, _FakeResourceBank())
    reporter.get_cluster_view()
    assert coordinator.placement_group_ids == expected


class _RecordingCoordinator(FakeAutoscalingCoordinator):
    """Records every placement-group registration it is sent."""

    def __init__(self):
        super().__init__()
        self.sent: List[FrozenSet[str]] = []

    def register_placement_groups(self, placement_group_ids):
        self.sent.append(frozenset(placement_group_ids))
        super().register_placement_groups(placement_group_ids)


def test_fn_placement_groups_are_registered_as_they_appear():
    """Placement groups a ray_remote_args_fn creates per actor are registered
    alongside the static ones, including for an op with no static group, so
    the coordinator takes their bundles out of the non-PG slice. Each view
    re-registers, but an unchanged set sends nothing."""
    coordinator = _RecordingCoordinator()
    infer = _PGOp(_fake_pg("static"))
    embed = _PGOp(uses_fn=True)
    reporter = _reporter(_topology(infer, embed), coordinator, _FakeResourceBank())

    reporter.get_cluster_view()
    reporter.get_cluster_view()
    embed.fn_pgs.append(_fake_pg("fn"))
    reporter.get_cluster_view()
    assert coordinator.sent == [
        frozenset({"static"}),
        frozenset({"static", "fn"}),
    ]


def test_cluster_view_measures_placement_group_slots():
    """Slots come from each static group's own bundle specs, in bundle-index
    order; an op whose group has no known bundles (one minted per actor) is
    capped at its pool's max size instead."""
    pinned = _PGOp(_fake_pg("pg1", [{"CPU": 1, "GPU": 1}, {"CPU": 2}]))
    minted = _PGOp(uses_fn=True, max_size=3)
    reporter = _reporter(
        _topology(pinned, minted), FakeAutoscalingCoordinator(), _FakeResourceBank()
    )

    slots = reporter.get_cluster_view().pg_slots
    assert slots.shapes == {
        BundleKey("pg1", 0): ExecutionResources(cpu=1, gpu=1),
        BundleKey("pg1", 1): ExecutionResources(cpu=2),
    }
    assert slots.static_bundles_by_op == {
        pinned: (BundleKey("pg1", 0), BundleKey("pg1", 1))
    }
    assert slots.unknown_cap_by_op == {minted: 3}


def test_cluster_view_carries_placement_group_part():
    """``reserved_by_node`` is the whole allocation; ``pg_reserved_by_node``
    is the part the coordinator says the registered groups hold."""
    coordinator = FakeAutoscalingCoordinator(
        initial_cluster_resources_by_node={"g1": {"CPU": 6}, "c1": {"CPU": 8}},
        placement_group_resources_by_node={"g1": {"CPU": 2, "GPU": 2}},
    )
    coordinator.request_resources([{"CPU": 1}], expire_after_s=60)
    reporter = _reporter(_topology(), coordinator, _FakeResourceBank())

    view = reporter.get_cluster_view()
    assert view.reserved_by_node == {
        "g1": ExecutionResources(cpu=8, gpu=2),
        "c1": ExecutionResources(cpu=8),
    }
    assert view.pg_reserved_by_node == {"g1": ExecutionResources(cpu=2, gpu=2)}
    assert reporter.get_global_limits() == ExecutionResources(cpu=16, gpu=2)


def test_cluster_view_reads_labels_and_draining_nodes_through_seams():
    """Node labels and draining nodes come from the injected readers (Ray's
    state API in production), read fresh on every view."""
    labels = {"g1": {"accelerator": "A100"}, "c1": {}}
    draining = {"c1": 1_700_000_000_000}  # node id -> drain deadline (ms)
    reporter = _reporter(
        _topology(),
        FakeAutoscalingCoordinator(),
        _FakeResourceBank(),
        get_node_labels=lambda: labels,
        get_draining_node_ids=lambda: draining,
    )

    view = reporter.get_cluster_view()
    assert view.labels_by_node == labels
    assert view.draining_nodes == frozenset({"c1"})

    draining.clear()
    assert reporter.get_cluster_view().draining_nodes == frozenset()


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
    reporter = _reporter(
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
