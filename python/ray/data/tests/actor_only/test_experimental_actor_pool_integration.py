import time
from typing import Any, Dict, List, Optional, Tuple, cast
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from typing_extensions import override

import ray
from ray.actor import ActorHandle
from ray.data._internal.actor_autoscaler.autoscaling_actor_pool import (
    AutoscalingActorConfig,
)
from ray.data._internal.execution.interfaces import ExecutionResources, NodeIdStr
from ray.data._internal.execution.interfaces.common import ActorStatus
from ray.data._internal.execution.interfaces.ref_bundle import BlockEntry, RefBundle
from ray.data._internal.execution.resource_bank import (
    LiveObjStore,
    ResourceBank,
    ResourceBankBase,
)
from ray.data._internal.experimental.execution.operators import (
    actor_pool as pool_mod,
)
from ray.data._internal.experimental.execution.operators.actor_pool import (
    _ExperimentalActorPool,
)
from ray.data._internal.experimental.execution.operators.actor_pool_map_operator import (
    ExperimentalActorPoolMapOperator,
)
from ray.data._internal.experimental.execution.operators.actor_pool_models import (
    DrainReason,
    ScaleUpRequest,
    _ExperimentalActorState,
)
from ray.data.block import Block, BlockMetadata
from ray.types import ObjectRef


# ---------------------------------------------------------------------------
# Worker actors
# ---------------------------------------------------------------------------
@ray.remote(num_cpus=1)
class _ProbeWorker:
    """Reports the node it actually landed on (real node id)."""

    def get_location(self) -> str:
        return ray.get_runtime_context().get_node_id()

    def ping(self) -> str:
        return "ok"


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------
class _FakeResourceBank(ResourceBankBase):
    """Minimal ``ResourceBankBase`` stub for integration pool helpers.

    ``node_capacity`` returns a fixed per-node budget so ``pending_to_running``
    can derive a non-zero per-actor object-store limit.
    """

    def __init__(self, node_resources: Optional[ExecutionResources] = None):
        self._node_resources = node_resources or ExecutionResources(
            cpu=4, object_store_memory=1024**3
        )
        # Per-actor live object store the drain gate reads; tests set an
        # actor's outstanding output blocks here.
        self.live_by_actor: Dict[str, LiveObjStore] = {}

    @override
    def live_object_store(self, op=None, node_id=None, actor_id=None) -> LiveObjStore:
        if actor_id is not None:
            return self.live_by_actor.get(actor_id, LiveObjStore())
        return LiveObjStore()

    @override
    def on_block_consumed(self, input_ref: ObjectRef[Any]) -> None:
        pass

    @override
    def deregister_actor(self, actor_id) -> None:
        pass

    @override
    def node_capacity(self, node_id: NodeIdStr) -> ExecutionResources:
        return self._node_resources


def _make_bundle(size_bytes: int = 100, node_id: str = "node1") -> RefBundle:
    """A minimal RefBundle with controlled size_bytes (mirrors the unit test)."""
    block = pa.table({"col": [0] * max(1, size_bytes // 8)})
    block_ref = ray.put(block)
    metadata = BlockMetadata(
        num_rows=block.num_rows,
        size_bytes=size_bytes,
        exec_stats=None,
        node_id=node_id,
    )
    return RefBundle(
        blocks=(BlockEntry(cast("ObjectRef[Block]", block_ref), metadata),),
        owns_blocks=False,
        schema=block.schema,
    )


def _tid(n: int) -> Any:
    """Wrap a test task id so pyrefly accepts it as ``ray.TaskID``."""
    return n


class _FakeRef:
    def __init__(self, key: str):
        self._key = key

    def hex(self) -> str:
        return self._key


class _FakeDataTask:
    def __init__(self, task_idx: int, task_id):
        self._task_idx = task_idx
        self._task_id = task_id

    def get_task_id(self):
        return self._task_id

    def task_index(self) -> int:
        return self._task_idx


@pytest.fixture
def two_node_cluster(ray_start_cluster):
    """A head node (0 CPUs, so worker actors never land there) plus two worker
    nodes each tagged with a distinguishing custom resource for node lookup."""
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=0, resources={"head": 1})
    # A prior test file may have left an auto-inited local Ray behind.
    ray.shutdown()
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=4, resources={"node_a": 1})
    cluster.add_node(num_cpus=4, resources={"node_b": 1})
    cluster.wait_for_nodes()
    yield cluster
    ray.shutdown()


def _node_id(resource: str) -> str:
    """Resolve a distinguishing custom resource to its (alive) NodeID."""
    for node in ray.nodes():
        if node["Alive"] and resource in node["Resources"]:
            return node["NodeID"]
    raise AssertionError(f"No alive node with resource {resource!r}")


def _build_pool(
    worker_cls, max_size: int = 8
) -> Tuple[_ExperimentalActorPool, List[Tuple[ActorHandle, ray.ObjectRef]]]:
    """Build a pool whose ``create_actor_fn`` honors ``scheduling_strategy``.

    The real ``_create_actor`` passes ``scheduling_strategy`` as a kwarg, so the
    fn MUST accept and apply it (NodeAffinity for targeted placement). ``created``
    accumulates ``(actor, ready_ref)`` so the test can grab the newest actor.
    """
    created: List[Tuple[ActorHandle, ray.ObjectRef]] = []

    def create_actor_fn(labels, logical_actor_id, scheduling_strategy=None):
        actor = worker_cls.options(
            _labels=labels, scheduling_strategy=scheduling_strategy
        ).remote()
        ready_ref = actor.get_location.remote()
        created.append((actor, ready_ref))
        return actor, ready_ref, ExecutionResources(cpu=1)

    config = AutoscalingActorConfig(
        # min_size/initial_size only matter for start(), which we never call;
        # the config asserts both >= 1, so use 1. Actors are created explicitly
        # via targeted scale().
        min_size=1,
        max_size=max_size,
        initial_size=1,
        max_tasks_in_flight_per_actor=4,
        max_actor_concurrency=1,
        per_actor_resource_usage=ExecutionResources(cpu=1),
        max_num_output_bytes_per_actor=10_000_000,
    )
    pool = _ExperimentalActorPool(create_actor_fn=create_actor_fn, config=config)
    # pending_to_running derives a per-actor object-store budget from
    # node_capacity; the fake bank supplies a non-zero node capacity.
    pool.set_resource_bank(_FakeResourceBank())
    return pool, created


def _add_ready_actor_on(
    pool: _ExperimentalActorPool,
    created: List[Tuple[ActorHandle, ray.ObjectRef]],
    node_id: str,
) -> ActorHandle:
    """Scale one actor onto ``node_id`` and drive it pending -> running."""
    num = pool.scale(ScaleUpRequest.on_nodes((node_id,), reason="test"))
    assert num == 1
    actor, ready_ref = created[-1]
    ray.get(ready_ref)
    pool.pending_to_running(ready_ref)
    return actor


# ---------------------------------------------------------------------------
# Output-backpressure state is cached incrementally in ResourceBank.
# ---------------------------------------------------------------------------
def test_resource_bank_output_backpressure_cache_updates_incrementally():
    actor_id = "actor-1"
    node_id = "node-1"
    actor: Any = object()
    op = cast(Any, object.__new__(ExperimentalActorPoolMapOperator))
    op._ray_remote_args = {"_actor_generator_backpressure_num_objects": 0}
    op._ray_actor_task_remote_args = {}
    task_id_1 = _tid(1)
    task_id_2 = _tid(2)
    bank = ResourceBank()
    pool, _ = _build_pool(_ProbeWorker)
    pool._actor_to_id[actor] = actor_id
    pool._id_to_actor[actor_id] = actor
    pool._running_actors[actor] = _ExperimentalActorState(
        actor_id=actor_id,
        num_tasks_in_flight=1,
        num_input_bytes_in_flight=0,
        actor_location=node_id,
        max_num_output_bytes=250,
        max_num_outputs=2,
        max_tasks_in_flight=4,
        latest_task_submission_ts=0,
        status=ActorStatus.ACTIVE,
    )
    pool._node_to_actor_states[node_id] = {actor_id: pool._running_actors[actor]}
    pool.set_resource_bank(bank)
    op._actor_pool = pool
    op._resource_bank = bank
    op._task_ray_id = {1: task_id_1}
    op._data_tasks = {1: _FakeDataTask(task_idx=1, task_id=task_id_1)}

    bank.maybe_register_actor(op=op, actor_id=actor_id, node_id=node_id)
    bank.on_task_submitted(actor_id=actor_id, task_id=task_id_1, input_bms=[])

    assert op.output_backpressured_fraction() == (0, 1)

    bm = BlockMetadata(
        num_rows=1,
        size_bytes=100,
        exec_stats=None,
        node_id=node_id,
    )
    bank.on_new_output(
        op=op,
        ref=cast("ObjectRef[Block]", _FakeRef("output-1")),
        bm=bm,
        task_id=task_id_1,
    )
    assert op.output_backpressured_fraction() == (0, 1)

    bank.on_new_output(
        op=op,
        ref=cast("ObjectRef[Block]", _FakeRef("output-2")),
        bm=bm,
        task_id=task_id_1,
    )
    assert op.output_backpressured_fraction() == (1, 1)

    op._task_ray_id[2] = task_id_2
    op._data_tasks[2] = _FakeDataTask(task_idx=2, task_id=task_id_2)
    bank.on_task_submitted(actor_id=actor_id, task_id=task_id_2, input_bms=[])
    assert op.output_backpressured_fraction() == (2, 2)

    op._data_tasks.pop(1)
    bank.on_task_completed(op=op, task_id=task_id_1)
    assert op.output_backpressured_fraction() == (1, 1)

    pool._id_to_actor.pop(actor_id)
    bank.deregister_actor(actor_id=actor_id)
    assert op.output_backpressured_fraction() == (0, 0)


def test_upgrade_output_limit_even_without_object_store_headroom():
    op = cast(Any, object.__new__(ExperimentalActorPoolMapOperator))
    op._name = "MapBatches(Test)"
    op._additional_split_factor = None
    pool = cast(Any, _ExperimentalActorPool.__new__(_ExperimentalActorPool))

    upgrades = []
    pool.active_nodes = lambda: {"node-1"}
    pool.upgrade_output_limit = lambda **kwargs: upgrades.append(kwargs)
    op._actor_pool = pool

    bank = MagicMock()
    bank.live_object_store.return_value = LiveObjStore(num_pulled_output_bytes=900)
    bank.node_capacity.return_value = ExecutionResources(object_store_memory=1000)
    cumulative = MagicMock()
    cumulative.output_bytes.mean = 200
    bank.cumulative_object_store.return_value = cumulative
    op._resource_bank = bank

    # Only 100 object-store bytes free vs a ~200-byte mean block, but we still
    # upgrade (floored at 1MiB) to avoid deadlock.
    assert op.upgrade_output_limit() is True
    assert upgrades == [
        {"node_id": "node-1", "delta_bytes": 1 << 20, "delta_outputs": 1}
    ]


# ---------------------------------------------------------------------------
# Targeted upscale lands on the requested node
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("target_res", ["node_a", "node_b"])
def test_scale_up_pins_actor_to_node(two_node_cluster, target_res):
    pool, created = _build_pool(_ProbeWorker)
    try:
        target = _node_id(target_res)
        actor = _add_ready_actor_on(pool, created, target)

        # Pool bookkeeping says the actor is on the target node...
        assert target in pool.nodes()
        assert pool.committed_usage_by_node[target].cpu == 1
        assert pool._running_actors[actor].actor_location == target
        # ...and the actor itself reports the same real node id.
        assert ray.get(created[-1][1]) == target
    finally:
        pool.shutdown(force=True)


def _recompute_committed_usage_by_node(pool):
    """From-scratch committed-usage recompute (the pre-incremental reference):
    running actors at their location, pending actors at their target node."""
    usage = {}
    for actor, state in pool._running_actors.items():
        usage[state.actor_location] = usage.get(
            state.actor_location, ExecutionResources.zero()
        ).add(pool._actor_resource_usage[actor])
    for ready_ref, info in pool._pending_actor_info.items():
        if info.target_node_id is None:
            continue
        actor = pool._pending_actors[ready_ref]
        usage[info.target_node_id] = usage.get(
            info.target_node_id, ExecutionResources.zero()
        ).add(pool._actor_resource_usage[actor])
    return usage


def test_committed_usage_tracked_incrementally(two_node_cluster):
    """The incrementally-maintained committed-usage map matches a from-scratch
    recompute through add-pending -> ready -> downscale."""
    pool, created = _build_pool(_ProbeWorker)
    try:
        node_a = _node_id("node_a")
        node_b = _node_id("node_b")

        # (1) Pending (not yet ready): committed at the target node.
        assert pool.scale(ScaleUpRequest.on_nodes((node_a,), reason="test")) == 1
        assert pool.committed_usage_by_node == _recompute_committed_usage_by_node(pool)
        assert pool.committed_usage_by_node[node_a].cpu == 1

        # (2) Ready: re-attributed to the actual landing node.
        a_actor, a_ref = created[-1]
        ray.get(a_ref)
        pool.pending_to_running(a_ref)
        assert pool.committed_usage_by_node == _recompute_committed_usage_by_node(pool)

        # (3) A second actor on the other node.
        b_actor = _add_ready_actor_on(pool, created, node_b)
        assert pool.committed_usage_by_node == _recompute_committed_usage_by_node(pool)
        assert pool.committed_usage_by_node[node_b].cpu == 1

        # (4) Downscale one (idle) actor: it drains and is killed on the next
        # tick, dropping its node entry (no stale zero).
        id_b = pool.get_actor_logical_id(b_actor)
        assert pool._downscale_actor_by_id(id_b, DrainReason.RESIZE) is True
        pool.process_draining_actors()
        committed = pool.committed_usage_by_node
        assert committed == _recompute_committed_usage_by_node(pool)
        assert node_b not in committed
    finally:
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# Downscale a specific actor by logical id
# ---------------------------------------------------------------------------
def test_downscale_specific_actor_by_id(two_node_cluster):
    pool, created = _build_pool(_ProbeWorker)
    try:
        a = _add_ready_actor_on(pool, created, _node_id("node_a"))
        b = _add_ready_actor_on(pool, created, _node_id("node_b"))
        id_a = pool.get_actor_logical_id(a)
        id_b = pool.get_actor_logical_id(b)

        # Remove exactly actor `a` (idle: drains and is killed this tick);
        # `b` is untouched.
        assert pool._downscale_actor_by_id(id_a, DrainReason.RESIZE) is True
        pool.process_draining_actors()
        assert not pool.has_actor(id_a)
        assert pool.has_actor(id_b)
        assert pool.num_running_actors() == 1
        # The survivor is still schedulable.
        assert pool.select_actors(bundle=_make_bundle()) == b
    finally:
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# Drain one actor, the rest of the pool keeps serving
# ---------------------------------------------------------------------------
def test_drain_one_actor_pool_keeps_serving(two_node_cluster):
    pool, created = _build_pool(_ProbeWorker)
    try:
        a = _add_ready_actor_on(pool, created, _node_id("node_a"))
        b = _add_ready_actor_on(pool, created, _node_id("node_b"))

        pool.begin_draining(a, reason=DrainReason.RESIZE)
        assert pool.num_draining_actors() == 1
        assert pool._running_actors[a].status == ActorStatus.DRAINING
        # `a` is excluded from the serving count (out of the scheduling heaps,
        # accepts no new work) but is still physically present, holding its
        # node's resources until it actually dies.
        assert pool.num_running_actors() == 1
        assert a in pool._running_actors

        # Every selection goes to the survivor `b`; the draining actor `a` never
        # receives a new task. Stay under max_tasks_in_flight (4).
        for _ in range(3):
            selected = pool.select_actors(bundle=_make_bundle())
            assert selected == b
            assert selected is not None
            pool.on_task_submitted(selected, _make_bundle())
        assert pool._running_actors[a].num_tasks_in_flight == 0
    finally:
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# A draining actor is killed only once it is FULLY drained -- no
# in-flight tasks. While a task is still in flight it waits (no drain timeout).
# ---------------------------------------------------------------------------
def test_drain_waits_for_inflight_tasks(two_node_cluster):
    pool, created = _build_pool(_ProbeWorker)
    try:
        a = _add_ready_actor_on(pool, created, _node_id("node_a"))
        id_a = pool.get_actor_logical_id(a)

        bundle = _make_bundle()
        pool.on_task_submitted(a, bundle)
        pool.begin_draining(a, reason=DrainReason.RESIZE)

        # In-flight task -> not fully drained -> not killed (waits indefinitely).
        assert pool.process_draining_actors() == 0
        assert pool.has_actor(id_a)
        assert pool.num_draining_actors() == 1
        # Draining with work in flight: still ACTIVE (a pure runtime metric),
        # excluded from the running count, and idle never goes negative.
        assert pool.num_active_actors() == 1
        assert pool.num_running_actors() == 0
        assert pool.num_idle_actors() == 0

        # Task completes and no outputs are outstanding -> fully drained -> killed.
        pool.on_task_completed(a, bundle)
        # No longer active; still draining, so not idle either.
        assert pool.num_active_actors() == 0
        assert pool.num_idle_actors() == 0
        assert pool.process_draining_actors() == 1
        assert not pool.has_actor(id_a)
        assert pool.num_draining_actors() == 0
        assert pool.num_running_actors() == 0
    finally:
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# A drained actor is released by dropping the pool's references,
# never hard-killed -- an explicit kill would make the objects it produced
# unreconstructable. Along the way, ``draining_generation`` bumps on every
# draining-set change (add / reclaim / release) so the output-priority sync
# can cheaply skip when nothing changed.
# ---------------------------------------------------------------------------
def test_drained_actor_released_without_kill(two_node_cluster, monkeypatch):
    kills = []
    real_kill = ray.kill
    monkeypatch.setattr(ray, "kill", lambda actor, **kw: kills.append(actor))

    pool, created = _build_pool(_ProbeWorker)
    try:
        a = _add_ready_actor_on(pool, created, _node_id("node_a"))
        id_a = pool.get_actor_logical_id(a)

        g0 = pool.draining_generation
        assert pool.draining_generation == g0  # reading never bumps

        pool.begin_draining(a, reason=DrainReason.RESIZE)  # idle: fully drained at once
        g1 = pool.draining_generation
        assert g1 > g0  # add -> bump

        assert pool.reclaim_draining_actors(limit=1) == 1
        g2 = pool.draining_generation
        assert g2 > g1  # reclaim -> bump

        pool.begin_draining(a, reason=DrainReason.RESIZE)
        g3 = pool.draining_generation
        assert g3 > g2  # re-add -> bump

        # The actor is released from the pool without a hard kill.
        assert pool.process_draining_actors() == 1
        assert pool.draining_generation > g3  # release -> bump
        assert not pool.has_actor(id_a)
        assert a not in kills
    finally:
        monkeypatch.setattr(ray, "kill", real_kill)
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# A draining actor is NOT released while the ResourceBank still counts output
# blocks against it (produced, not yet consumed downstream), even with no
# in-flight tasks.
# ---------------------------------------------------------------------------
def test_drain_waits_for_outstanding_outputs(two_node_cluster):
    pool, created = _build_pool(_ProbeWorker)
    bank = cast(_FakeResourceBank, pool.resource_bank)
    try:
        a = _add_ready_actor_on(pool, created, _node_id("node_a"))
        id_a = pool.get_actor_logical_id(a)

        bundle = _make_bundle()
        pool.on_task_submitted(a, bundle)
        # The task streams two output blocks (the bank counts them against the
        # actor until downstream consumes them), then finishes.
        bank.live_by_actor[id_a] = LiveObjStore(num_pulled_output_blocks=2)
        pool.on_task_completed(a, bundle)
        pool.begin_draining(a, reason=DrainReason.RESIZE)

        # No in-flight tasks but 2 outstanding outputs -> NOT released.
        assert pool.process_draining_actors() == 0
        assert pool.has_actor(id_a)

        bank.live_by_actor[id_a] = LiveObjStore(num_pulled_output_blocks=1)
        assert pool.process_draining_actors() == 0
        assert pool.has_actor(id_a)

        # Last output consumed -> fully drained -> released.
        bank.live_by_actor[id_a] = LiveObjStore()
        assert pool.process_draining_actors() == 1
        assert not pool.has_actor(id_a)
    finally:
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# A pending (not-yet-running) victim is dropped + killed at once --
# it has no tasks or outputs to drain.
# ---------------------------------------------------------------------------
def test_drain_pending_victim_is_dropped(two_node_cluster):
    pool, created = _build_pool(_ProbeWorker)
    try:
        node_a = _node_id("node_a")
        assert pool.scale(ScaleUpRequest.on_nodes((node_a,), reason="test")) == 1
        assert pool.num_pending_actors() == 1
        pending_id = pool.pending_ids_by_target_node()[node_a][0]

        assert pool._downscale_actor_by_id(pending_id, DrainReason.RESIZE) is True
        assert pool.num_pending_actors() == 0
        assert not pool.has_actor(pending_id)
    finally:
        pool.shutdown(force=True)


class _FakeClock:
    """Controllable stand-in for the module's ``time`` so the drain-stall
    WARNING can be exercised deterministically; unknown attrs (sleep, etc.)
    fall through to the real module."""

    def __init__(self, t: float):
        self.t = t

    def time(self) -> float:
        return self.t

    def __getattr__(self, name):
        return getattr(time, name)


# ---------------------------------------------------------------------------
# An actor whose drain never completes (a stuck in-flight task)
# waits indefinitely (never killed) and emits a single debounced WARNING.
# ---------------------------------------------------------------------------
def test_stalled_drain_warns_debounced_and_never_kills(two_node_cluster, monkeypatch):
    clock = _FakeClock(1000.0)
    monkeypatch.setattr(pool_mod, "time", clock)
    monkeypatch.setattr(pool_mod, "DRAIN_STALL_WARN_S", 100)

    pool, created = _build_pool(_ProbeWorker)
    try:
        a = _add_ready_actor_on(pool, created, _node_id("node_a"))
        id_a = pool.get_actor_logical_id(a)
        # A task that never completes -> the drain can never finish.
        pool.on_task_submitted(a, _make_bundle())
        pool.begin_draining(a, reason=DrainReason.RESIZE)  # since == 1000.0

        # Below the stall threshold: no warning fired (debounce ts untouched).
        clock.t = 1050.0
        assert pool.process_draining_actors() == 0
        assert pool._last_drain_stall_warn_ts == 0.0

        # Past the threshold: warns once, stamping the debounce clock.
        clock.t = 1101.0
        pool.process_draining_actors()
        assert pool._last_drain_stall_warn_ts == 1101.0

        # Immediate re-tick is debounced: no new warning, ts unchanged.
        clock.t = 1102.0
        pool.process_draining_actors()
        assert pool._last_drain_stall_warn_ts == 1101.0

        # The stuck actor is never killed while its task is in flight.
        assert pool.has_actor(id_a)
    finally:
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# A task that completes after its actor was force-released does not
# corrupt the pool's in-flight accounting (the failure surfaces a tick later).
# ---------------------------------------------------------------------------
def test_on_task_completed_tolerates_released_actor(two_node_cluster):
    pool, created = _build_pool(_ProbeWorker)
    try:
        a = _add_ready_actor_on(pool, created, _node_id("node_a"))
        bundle = _make_bundle()
        pool.on_task_submitted(a, bundle)
        assert pool._total_num_tasks_in_flight == 1

        # Force-release the actor while a task is still in flight; releasing
        # reverses the in-flight accounting.
        pool._release_running_actor(a)
        assert pool._total_num_tasks_in_flight == 0

        # The in-flight task's (failed) completion arrives afterwards: it must be
        # a no-op, not a KeyError or a negative-count underflow.
        pool.on_task_completed(a, bundle)
        assert pool._total_num_tasks_in_flight == 0
        assert pool.num_running_actors() == 0
    finally:
        pool.shutdown(force=True)


# ---------------------------------------------------------------------------
# The output-backpressure path asks the pool for an actor's output
# limits even after the actor was released (a lingering data-task ref). A bare
# _id_to_actor[actor_id] lookup KeyError'd there and crashed the
# executor (relieve_output_limit_deadlock -> is_output_backpressured -> output_bytes_limit);
# a released / unknown actor must yield no constraint instead.
# ---------------------------------------------------------------------------
def test_output_limits_tolerate_released_actor(two_node_cluster):
    pool, created = _build_pool(_ProbeWorker)
    try:
        a = _add_ready_actor_on(pool, created, _node_id("node_a"))
        id_a = pool.get_actor_logical_id(a)

        # Live actor: real per-actor limits.
        assert pool.output_bytes_limit(id_a) == (
            pool._running_actors[a].max_num_output_bytes
        )
        assert pool.output_count_limit(id_a) == (
            pool._running_actors[a].max_num_outputs
        )

        # Released actor (force-kill / death) with a lingering task ref: no
        # constraint, no KeyError.
        pool._release_running_actor(a)
        assert not pool.has_actor(id_a)
        assert pool.output_bytes_limit(id_a) == float("inf")
        assert pool.output_count_limit(id_a) == float("inf")

        # Wholly unknown id is likewise tolerated.
        assert pool.output_bytes_limit("does-not-exist") == float("inf")
        assert pool.output_count_limit("does-not-exist") == float("inf")
    finally:
        pool.shutdown(force=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
