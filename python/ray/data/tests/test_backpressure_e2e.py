import time
from typing import Dict, List, Tuple
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import ray
from ray._private.internal_api import memory_summary
from ray.data._internal.execution.backpressure_policy.actor_pool_drain_policy import (
    ActorPoolDrainPolicy,
)
from ray.data._internal.execution.backpressure_policy.downstream_capacity_backpressure_policy import (
    DownstreamCapacityBackpressurePolicy,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    _ActorPool,
)
from ray.data._internal.execution.util import memory_string
from ray.data._internal.util import MiB
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask
from ray.data.tests.conftest import (
    CoreExecutionMetrics,
    assert_core_execution_metrics_equals,
    get_initial_core_execution_metrics_snapshot,
    restore_data_context,  # noqa: F401
)
from ray.tests.conftest import shutdown_only  # noqa: F401


def test_large_e2e_backpressure_no_spilling(
    shutdown_only, restore_data_context  # noqa: F811
):
    """Test backpressure can prevent object spilling on a synthetic large-scale
    workload."""
    # The cluster has 10 CPUs and 200MB object store memory.
    #
    # Each produce task generates 10 blocks, each of which has 10MB data.
    # In total, there will be 10 * 10 * 10MB = 1000MB intermediate data.
    #
    # `ReservationOpResourceAllocator` should dynamically allocate resources to each
    # operator and prevent object spilling.
    NUM_CPUS = 10
    NUM_ROWS_PER_TASK = 10
    NUM_TASKS = 20
    NUM_ROWS_TOTAL = NUM_ROWS_PER_TASK * NUM_TASKS
    BLOCK_SIZE = 10 * MiB
    object_store_memory = 200 * MiB

    print(f">>> Setting Object Store to {memory_string(object_store_memory)}")

    ray.init(num_cpus=NUM_CPUS, object_store_memory=object_store_memory)

    def produce(batch):
        print(">>> [Producer] Produce task started", batch["id"])
        time.sleep(0.1)
        for id in batch["id"]:
            print(f">>> [Producer] Producing row {id=}")
            yield {
                "id": [id],
                "image": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
            }

    def consume(batch):
        print(">>> [Consumer] Consume task started", batch["id"])
        time.sleep(0.01)
        return {"id": batch["id"], "result": [0 for _ in batch["id"]]}

    data_context = ray.data.DataContext.get_current()
    data_context.execution_options.verbose_progress = True
    data_context.target_max_block_size = BLOCK_SIZE

    last_snapshot = get_initial_core_execution_metrics_snapshot()

    ds = ray.data.range(NUM_ROWS_TOTAL, override_num_blocks=NUM_TASKS)
    ds = ds.map_batches(produce, batch_size=NUM_ROWS_PER_TASK)
    ds = ds.map_batches(consume, batch_size=None, num_cpus=0.9)
    # Check core execution metrics every 10 rows, because it's expensive.
    for _ in ds.iter_batches(batch_size=NUM_ROWS_PER_TASK):
        last_snapshot = assert_core_execution_metrics_equals(
            CoreExecutionMetrics(
                object_store_stats={
                    "spilled_bytes_total": 0,
                    "restored_bytes_total": 0,
                },
            ),
            last_snapshot,
        )


def _build_dataset(
    obj_store_limit,
    producer_num_cpus,
    consumer_num_cpus,
    num_blocks,
    block_size,
    insert_limit_op=False,
):
    # Create a dataset with 2 operators:
    # - The producer op has only 1 task, which produces `num_blocks` blocks, each
    #   of which has `block_size` data.
    # - The consumer op has `num_blocks` tasks, each of which consumes 1 block.
    ctx = ray.data.DataContext.get_current()
    ctx.target_max_block_size = block_size
    ctx.execution_options.resource_limits = ctx.execution_options.resource_limits.copy(
        object_store_memory=obj_store_limit
    )

    def producer(batch):
        for i in range(num_blocks):
            print(f"[{time.time()}] Producing block #{i} ({block_size=})")
            yield {
                "id": [i],
                "data": [np.zeros(block_size, dtype=np.uint8)],
            }

    def consumer(batch):
        assert len(batch["id"]) == 1
        print(f"[{time.time()}] Consuming block #{batch['id'][0]}")
        time.sleep(0.01)
        del batch["data"]
        return batch

    ds = ray.data.range(1, override_num_blocks=1).materialize()
    ds = ds.map_batches(producer, batch_size=None, num_cpus=producer_num_cpus)
    # Add a limit op in the middle, to test that ReservationOpResourceAllocator
    # will account limit op's resource usage to the previous producer map op.
    if insert_limit_op:
        ds = ds.limit(num_blocks)
    ds = ds.map_batches(consumer, batch_size=None, num_cpus=consumer_num_cpus)
    if insert_limit_op:
        ds = ds.limit(num_blocks)
    return ds


@pytest.mark.parametrize(
    "cluster_cpus, cluster_obj_store_mem_mb",
    [
        (3, 500),  # CPU not enough
        (4, 100),  # Object store memory not enough
        (3, 100),  # Both not enough
    ],
)
@pytest.mark.parametrize("insert_limit_op", [False, True])
def test_no_deadlock_on_small_cluster_resources(
    cluster_cpus,
    cluster_obj_store_mem_mb,
    insert_limit_op,
    shutdown_only,  # noqa: F811
    restore_data_context,  # noqa: F811
):
    """Test when cluster resources are not enough for launching one task per op,
    the execution can still proceed without deadlock.
    """
    cluster_obj_store_mem_mb *= 1024**2
    ray.init(num_cpus=cluster_cpus, object_store_memory=cluster_obj_store_mem_mb)
    num_blocks = 10
    block_size = 100 * 1024 * 1024
    ds = _build_dataset(
        obj_store_limit=cluster_obj_store_mem_mb // 2,
        producer_num_cpus=3,
        consumer_num_cpus=1,
        num_blocks=num_blocks,
        block_size=block_size,
        insert_limit_op=insert_limit_op,
    )
    assert len(ds.take_all()) == num_blocks


@pytest.mark.parametrize("insert_limit_op", [False, True])
def test_no_deadlock_on_resource_contention(
    insert_limit_op, shutdown_only, restore_data_context  # noqa: F811
):
    """Test when resources are preempted by non-Data code, the execution can
    still proceed without deadlock."""
    cluster_obj_store_mem = 1000 * 1024 * 1024
    ray.init(num_cpus=5, object_store_memory=cluster_obj_store_mem)
    # Create a non-Data actor that uses 4 CPUs, only 1 CPU
    # is left for Data. Currently Data StreamExecutor still
    # incorrectly assumes it has all the 5 CPUs.
    # Check that we don't deadlock in this case.

    @ray.remote(num_cpus=4)
    class DummyActor:
        def foo(self):
            return None

    dummy_actor = DummyActor.remote()
    ray.get(dummy_actor.foo.remote())

    num_blocks = 10
    block_size = 50 * 1024 * 1024
    ds = _build_dataset(
        obj_store_limit=cluster_obj_store_mem // 2,
        producer_num_cpus=1,
        consumer_num_cpus=0.9,
        num_blocks=num_blocks,
        block_size=block_size,
        insert_limit_op=insert_limit_op,
    )

    from ray.data._internal.execution.streaming_executor_state import IdleDetector

    with patch.object(IdleDetector, "DETECTION_INTERVAL_S", 0.1):
        assert len(ds.take_all()) == num_blocks


def test_no_deadlock_when_downstream_capacity_policy_zeros_limit(
    shutdown_only, restore_data_context  # noqa: F811
):
    """Test when DownstreamCapacityBackpressurePolicy zeros the output limit,
    the execution can still proceed without deadlock."""
    cluster_obj_store_mem = 100 * MiB
    ray.init(num_cpus=2, object_store_memory=cluster_obj_store_mem)

    num_blocks = 20
    block_size = 1 * MiB
    ds = _build_dataset(
        obj_store_limit=cluster_obj_store_mem // 2,
        producer_num_cpus=1,
        consumer_num_cpus=1,
        num_blocks=num_blocks,
        block_size=block_size,
    )

    # Force DownstreamCapacityBackpressurePolicy to always return 0 to trigger unblock
    with patch.object(
        DownstreamCapacityBackpressurePolicy,
        "max_task_output_bytes_to_read",
        lambda self, op: 0,
    ):
        # Without the escape hatch firing, this would hang.
        assert len(ds.take_all()) == num_blocks


def test_no_deadlock_with_preserve_order(
    restore_data_context, shutdown_only  # noqa: F811
):
    """Test backpressure won't cause deadlocks when `preserve_order=True`."""
    num_blocks = 20
    block_size = 10 * 1024 * 1024
    ray.init(num_cpus=num_blocks)
    data_context = ray.data.DataContext.get_current()
    data_context.target_max_block_size = block_size
    data_context._max_num_blocks_in_streaming_gen_buffer = 1
    data_context.execution_options.preserve_order = True
    data_context.execution_options.resource_limits = (
        data_context.execution_options.resource_limits.copy(
            object_store_memory=5 * block_size
        )
    )

    # Some tasks are slower than others.
    # The faster tasks will finish first and occupy Map op's internal output buffer.
    # Test that we won't backpressure the operator in this case.
    def map_fn(batch):
        idx = batch["id"][0]
        print("map_fn", idx, time.time())
        if idx % 2 == 0:
            time.sleep(3)
        batch["data"] = [np.zeros(block_size, dtype=np.uint8)]
        return batch

    ds = ray.data.range(num_blocks, override_num_blocks=num_blocks)
    ds = ds.map_batches(map_fn, batch_size=None, num_cpus=1)
    assert len(ds.take_all()) == num_blocks


def test_input_backpressure_e2e(restore_data_context, shutdown_only):  # noqa: F811
    # Tests that backpressure applies even when reading directly from the input
    # datasource. This relies on datasource metadata size estimation.

    @ray.remote
    class Counter:
        def __init__(self):
            self.count = 0

        def increment(self):
            self.count += 1

        def get(self):
            return self.count

        def reset(self):
            self.count = 0

    class CountingRangeDatasource(Datasource):
        def __init__(self):
            self.counter = Counter.remote()

        def prepare_read(self, parallelism):
            # Use 50 MiB blocks to exceed the 25 MiB output reservation
            # and trigger object store backpressure
            num_bytes = 50 * MiB

            def range_(i):
                print(f">>> Read task: {i=}")

                ray.get(self.counter.increment.remote())
                return [pd.DataFrame({"data": np.ones((num_bytes,), dtype=np.uint8)})]

            print(f">>> Block size: {num_bytes}")

            return [
                ReadTask(
                    lambda i=i: range_(i),
                    BlockMetadata(
                        num_rows=1,
                        size_bytes=num_bytes,
                        input_files=None,
                        exec_stats=None,
                    ),
                )
                for i in range(parallelism)
            ]

    source = CountingRangeDatasource()

    ctx = ray.data.DataContext.get_current()
    ctx.execution_options.resource_limits = ctx.execution_options.resource_limits.copy(
        object_store_memory=100 * MiB,
        cpu=1,
    )
    ctx.target_max_block_size = 50 * MiB

    # Create dataset with many blocks
    ds = ray.data.read_datasource(source, override_num_blocks=1000)
    it = iter(ds.iter_internal_ref_bundles())
    # Dequeue 1 block
    next(it)
    # Let it bake for some time
    time.sleep(3)

    launched = ray.get(source.counter.get.remote())

    # Clean up
    del it
    # With 50 MiB blocks and 100 MiB limit, backpressure should limit to ~2 tasks
    # because after 2 outputs (100 MiB), the budget is depleted
    assert launched == 2, launched


def test_streaming_backpressure_e2e(
    shutdown_only, monkeypatch, restore_data_context  # noqa: F811
):
    # This test case is particularly challenging since there is a large input->output
    # increase in data size: https://github.com/ray-project/ray/issues/34041

    # Increase the Ray Core spilling threshold to 100% to avoid flakiness.
    monkeypatch.setenv("RAY_object_spilling_threshold", "1")

    class TestSlow:
        def __call__(self, df: np.ndarray):
            time.sleep(2)
            return {"id": np.random.randn(1, 20, 1024, 1024)}

    class TestFast:
        def __call__(self, df: np.ndarray):
            time.sleep(0.5)
            return {"id": np.random.randn(1, 20, 1024, 1024)}

    ctx = ray.init(object_store_memory=4e9)
    ds = ray.data.range_tensor(20, shape=(3, 1024, 1024), override_num_blocks=20)

    pipe = ds.map_batches(
        TestFast,
        batch_size=1,
        num_cpus=0.5,
        compute=ray.data.ActorPoolStrategy(size=2),
    ).map_batches(
        TestSlow,
        batch_size=1,
        compute=ray.data.ActorPoolStrategy(size=1),
    )

    for batch in pipe.iter_batches(batch_size=1, prefetch_batches=2):
        ...

    # If backpressure is not working right, we will spill.
    meminfo = memory_summary(ctx.address_info["address"], stats_only=True)
    assert "Spilled" not in meminfo, meminfo


# === Actor pool drain (state machine covered by test_actor_pool_drain_policy.py) ===

# About 20 bottleneck batches (20s per test): enough time for the 2s drain window
# to engage and for the pool to be reclaimed, without the 50-batch, 50s-per-test
# cost of the original repro.
DRAIN_NUM_ROWS = 40_000
DRAIN_BATCH_SIZE = 2_000
# Small on purpose: this budget is what makes output backpressure engage.
DRAIN_OBJECT_STORE_BUDGET = 60 * 1024**2
# Deliberately more than the two actors the pipeline needs to run: with a wider
# budget the upstream pool grows past its floor, so the drain has several actors
# to reclaim and the walk-down is exercised end to end. At a budget of 4 the pool
# only ever reaches 2 against a min_size of 1, leaving exactly one actor to
# reclaim -- an assertion that then rests on a margin of one.
#
# CPUs rather than GPUs: the resource is only a budget that lets the pool grow,
# and nothing here depends on accelerators. Keeping it CPU-only means the drain
# path is covered on CPU-only CI instead of being skipped wherever there is no
# GPU.
DRAIN_NUM_CPUS = 12
# Reason `DefaultActorAutoscaler` attaches to the scale-down it issues for a
# draining operator. Used to tell a drain removal apart from the other paths that
# also shrink the pool -- notably "consumed all inputs", which fires at teardown
# whether or not the drain did anything.
DRAIN_REASON = "draining a stalled actor pool"


def _tune_context_for_drain(ctx):
    ctx.max_tasks_in_flight_per_actor = 4
    ctx.downstream_capacity_backpressure_ratio = 0.5
    ctx.execution_options.resource_limits = ctx.execution_options.resource_limits.copy(
        object_store_memory=DRAIN_OBJECT_STORE_BUDGET
    )


class _DrainObservation:
    """What a drain run did, sampled on the scheduling thread.

    ``ActorPoolDrainPolicy.can_add_input`` is consulted for every operator on
    every scheduling iteration, so spying on it gives a race-free view of which
    operators were withheld from, without a sampling thread of its own. The
    scale-down side is observed on ``_ActorPool.scale`` instead, where the
    *result* of each request is visible -- which is what shows whether an actor
    was actually handed back.
    """

    def __init__(self):
        # Ops the policy withheld from, in the order they first got withheld.
        self.withheld: List[str] = []
        # op name -> call index at which it was first withheld.
        self.first_withheld_at: Dict[str, int] = {}
        # (op name, delta, force, reason) for every request the autoscaler made.
        self.scale_requests: List[Tuple[str, int, bool, str]] = []
        # id(pool) -> owning op name. `_ActorPool` has no back-reference to the
        # operator that owns it, so the owning op is learned from the policy spy
        # (which is handed the operator) and used to attribute `scale` calls.
        self.pool_owner: Dict[int, str] = {}
        # (op name, size before, requested delta, result, size after) for drain
        # requests that were actually executed. `result` is what `_ActorPool.scale`
        # returned: negative means actors were really released, 0 means the request
        # was refused or found nothing to release.
        self.drain_results: List[Tuple[str, int, int, int, int]] = []
        self._calls = 0

    def forced_drain_requests(self) -> List[Tuple[str, int, bool, str]]:
        return [
            request
            for request in self.scale_requests
            if request[2] and request[3] == DRAIN_REASON
        ]

    def executed_drain_removals(self) -> List[Tuple[str, int, int, int, int]]:
        """Drain requests that really removed at least one actor.

        A request being *issued* proves nothing -- the pool refuses it when no
        actor is idle, and returns 0. Only a negative result shows an actor was
        handed back.
        """
        return [record for record in self.drain_results if record[3] < 0]


def _run_stalled_pipeline():
    """Run a fast upstream into a slow bottleneck, returning (rows, observation).

    The UDFs are nested so they get cloudpickled by value and the test module
    does not have to be importable in the workers.
    """

    class Upstream:
        def __call__(self, batch):
            time.sleep(0.01)
            ids = list(batch["id"])
            per = max(1, len(ids) // 4)
            payload = b"\0" * (512 * 1024 // per)
            for i in range(0, len(ids), per):
                # The last chunk may have fewer than `per` rows when a batch
                # crosses a block boundary, so size the payload from the actual
                # chunk length instead of hard-coding `per`.
                chunk = ids[i : i + per]
                yield {"id": chunk, "data": [payload] * len(chunk)}

    class Bottleneck:
        """The slow stage that sets the throughput."""

        def __call__(self, batch):
            time.sleep(1.0)
            return batch

    obs = _DrainObservation()
    original_can_add_input = ActorPoolDrainPolicy.can_add_input
    original_scale = _ActorPool.scale

    def _spy_can_add_input(self, op):
        result = original_can_add_input(self, op)
        obs._calls += 1
        name = str(op)
        # Learn which operator owns each pool, so `scale` calls can be attributed
        # without `_ActorPool` holding an operator reference.
        for pool in op.get_autoscaling_actor_pools():
            obs.pool_owner[id(pool)] = name
        if not result and name not in obs.first_withheld_at:
            obs.withheld.append(name)
            obs.first_withheld_at[name] = obs._calls
        return result

    def _spy_scale(self, req):
        owner = obs.pool_owner.get(id(self), self.map_worker_cls_name)
        before = self.current_size()
        result = original_scale(self, req)
        obs.scale_requests.append((owner, req.delta, req.force, req.reason))
        if req.reason == DRAIN_REASON:
            obs.drain_results.append(
                (owner, before, req.delta, result, self.current_size())
            )
        return result

    ds = (
        ray.data.range(DRAIN_NUM_ROWS, override_num_blocks=50).map_batches(
            Upstream, batch_size=DRAIN_BATCH_SIZE, num_cpus=1
        )
        # Pinned to a single actor on purpose: this stage has to *be* the
        # throughput ceiling, or the upstream never stalls and there is nothing
        # to drain. Under GPUs that fell out of GPU scarcity; on CPUs both pools
        # would simply grow in parallel and the upstream would keep up, so the
        # constraint is made explicit instead of implied by the resource budget.
        .map_batches(
            Bottleneck,
            batch_size=DRAIN_BATCH_SIZE,
            compute=ray.data.ActorPoolStrategy(size=1),
            num_cpus=1,
        )
    )
    with patch.object(
        ActorPoolDrainPolicy, "can_add_input", _spy_can_add_input
    ), patch.object(_ActorPool, "scale", _spy_scale):
        rows = sum(
            len(b["id"])
            for b in ds.iter_batches(batch_size=DRAIN_BATCH_SIZE, prefetch_batches=1)
        )
    return rows, obs


@pytest.mark.timeout(300)
def test_actor_pool_drain_disabled_by_default(
    shutdown_only, restore_data_context  # noqa: F811
):
    """With the default config the policy must never withhold a task."""
    ray.init(
        num_cpus=DRAIN_NUM_CPUS,
        object_store_memory=2 * 1024**3,
        include_dashboard=False,
    )

    ctx = ray.data.DataContext.get_current()
    assert ctx.actor_pool_drain_window_s is None
    _tune_context_for_drain(ctx)

    rows, obs = _run_stalled_pipeline()

    assert rows == DRAIN_NUM_ROWS
    assert not obs.withheld, "policy withheld a task while disabled"
    assert (
        not obs.forced_drain_requests()
    ), "the autoscaler force-shrank a pool while the policy was disabled"
    assert (
        not obs.executed_drain_removals()
    ), "the autoscaler released actors for a drain while the policy was disabled"


@pytest.mark.timeout(300)
def test_actor_pool_drain_engages(shutdown_only, restore_data_context):  # noqa: F811
    """The policy must withhold tasks from the stalled upstream, the autoscaler
    must turn that into a forced scale-down, the pool must actually shrink, and
    the pipeline must still run to completion.

    Asserting only that `can_add_input` returned False would prove the policy
    fires but not that anything came of it -- and the point of the change is the
    actors that get handed back.
    """
    ray.init(
        num_cpus=DRAIN_NUM_CPUS,
        object_store_memory=2 * 1024**3,
        include_dashboard=False,
    )

    ctx = ray.data.DataContext.get_current()
    # Opt in, with a short window so the test does not have to wait.
    ctx.actor_pool_drain_window_s = 2.0
    _tune_context_for_drain(ctx)

    rows, obs = _run_stalled_pipeline()

    assert rows == DRAIN_NUM_ROWS
    assert obs.withheld, "policy never withheld a task from a stalled op"

    # The policy only sets a flag; the drain branch in `DefaultActorAutoscaler`
    # is what turns it into a scale-down request, bypassing the debounce. Without
    # this link the withholding buys nothing.
    assert obs.forced_drain_requests(), (
        "policy withheld inputs but the autoscaler never issued a forced drain "
        f"downscale; requests seen: {obs.scale_requests}"
    )

    # ... and the request has to reclaim an actor. This is the payoff the commit
    # claims, and it is what the e2e test used to leave unverified.
    #
    # Two things make this discriminating, and both are load-bearing:
    #
    #   * Only an *executed* removal counts. `_ActorPool.scale` returns 0 when it
    #     finds no idle actor, so a request on its own proves nothing.
    #   * The removal has to be attributed by reason. Teardown shrinks the pool
    #     through "consumed all inputs", which happens whether or not the drain
    #     did anything, so a plain "the pool got smaller" check passes even when
    #     every drain request is ignored.
    drained = obs.withheld[0]
    removals = [
        record for record in obs.executed_drain_removals() if record[0] == drained
    ]
    assert removals, (
        f"{drained} was drained (first withheld at call "
        f"{obs.first_withheld_at[drained]}) but none of the "
        f"{len(obs.drain_results)} drain request(s) released an actor; "
        "last few seen (op, size before, delta, result, size after): "
        f"{obs.drain_results[-3:]}"
    )
    _, before, delta, result, after = removals[0]
    assert delta < 0, f"a drain removal must be a downscale, got delta={delta}"
    assert after == before + result, (
        f"drain request {delta} on {drained} reported result {result} but the pool "
        f"went {before} -> {after}; the reported release did not match the change"
    )
    assert after < before, f"{drained} did not shrink on its drain removal"

    # Deliberately NOT asserted: that the downstream pool grows. Measured against
    # a drain-disabled baseline, the bottleneck reaches its peak in both configs
    # (it only gets there at teardown), so such an assertion passes either way and
    # would not test this change. The attributed removal above is what
    # discriminates; the baseline's own teardown shrink is exactly why an
    # unattributed pool-size check does not.


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
