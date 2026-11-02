import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray._private.internal_api import memory_summary
from ray.data._internal.execution.backpressure_policy.downstream_capacity_backpressure_policy import (
    DownstreamCapacityBackpressurePolicy,
)
from ray.data._internal.execution.execution_callback import ExecutionCallback
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


_NUM_BLOCKS = 200
_ROWS_PER_BLOCK = 1024


def _make_payload_dataset():
    """A ~200 MiB dataset of 1 MiB blocks, larger than the test object stores."""

    def add_payload(batch):
        num_rows = len(batch["id"])
        return {"id": batch["id"], "payload": np.zeros((num_rows, 1024), np.uint8)}

    return ray.data.range(
        _NUM_BLOCKS * _ROWS_PER_BLOCK, override_num_blocks=_NUM_BLOCKS
    ).map_batches(add_payload)


def _executor_threads():
    return [
        thread
        for thread in threading.enumerate()
        if thread.name.startswith("StreamingExecutor-")
    ]


def _iterate_until_output_backpressured():
    """Start iterating a dataset much larger than the object store, take one
    batch, then stop consuming until the executor has been output-backpressured
    for several consecutive scheduling steps.

    Returns the iterator, the ids consumed so far, and the scheduling-step stats.
    """
    ray.init(num_cpus=4, object_store_memory=100 * MiB)
    stats = {"num_steps": 0, "backpressured_streak": 0}

    class SchedulingStepCounter(ExecutionCallback):
        def on_execution_step(self, executor):
            stats["num_steps"] += 1
            if any(op._in_task_output_backpressure for op in executor._topology):
                stats["backpressured_streak"] += 1
            else:
                stats["backpressured_streak"] = 0

    ray.data.DataContext.get_current().custom_execution_callback_classes.append(
        SchedulingStepCounter
    )

    it = iter(_make_payload_dataset().iter_batches(batch_size=_ROWS_PER_BLOCK))
    ids = list(next(it)["id"])
    wait_for_condition(lambda: stats["backpressured_streak"] >= 5)
    return it, ids, stats


def test_output_backpressure_does_not_busy_spin(
    shutdown_only, restore_data_context  # noqa: F811
):
    it, ids, stats = _iterate_until_output_backpressured()

    steps_before = stats["num_steps"]
    time.sleep(1)
    idle_steps = stats["num_steps"] - steps_before
    # Each blocked iteration waits WAIT_FOR_TASK_COMPLETION_TIMEOUT_S (0.1s), so
    # one second allows ~10. Busy-spinning runs thousands.
    assert idle_steps < 50, idle_steps

    for batch in it:
        ids.extend(batch["id"])
    assert sorted(ids) == list(range(_NUM_BLOCKS * _ROWS_PER_BLOCK))


def test_executor_shuts_down_while_output_backpressured(
    shutdown_only, restore_data_context  # noqa: F811
):
    it, _, _ = _iterate_until_output_backpressured()

    assert _executor_threads()
    it.close()
    assert not _executor_threads()


def test_streaming_split_consumers_get_every_row_once(
    shutdown_only, restore_data_context  # noqa: F811
):
    ray.init(num_cpus=4, object_store_memory=100 * MiB)

    def consume(split):
        return [
            row_id
            for batch in split.iter_batches(batch_size=_ROWS_PER_BLOCK)
            for row_id in batch["id"]
        ]

    with ThreadPoolExecutor(max_workers=2) as pool:
        ids_per_split = list(
            pool.map(consume, _make_payload_dataset().streaming_split(2))
        )

    assert sorted(ids_per_split[0] + ids_per_split[1]) == list(
        range(_NUM_BLOCKS * _ROWS_PER_BLOCK)
    )


class _DispatchGate:
    """Backpressure policy that blocks new tasks while closed, without limiting
    output reads, so running tasks drain and the executor is left with no active
    task. Stands in for downstream-capacity backpressure."""

    name = "DispatchGate"
    closed = False

    def can_add_input(self, op):
        return not _DispatchGate.closed

    def max_task_output_bytes_to_read(self, op):
        return None


def _iterate_until_idle_with_unread_output():
    """Start iterating, close ``_DispatchGate`` once a few outputs are queued, and
    wait until the executor has had no active task, while the consumer has output
    it isn't reading, for several consecutive scheduling steps.

    Returns the iterator, the ids consumed so far, and the scheduling-step stats.
    """
    ray.init(num_cpus=4, object_store_memory=100 * MiB)
    _DispatchGate.closed = False
    stats = {"num_steps": 0, "idle_streak": 0}

    class GateAndStepCounter(ExecutionCallback):
        def before_execution_starts(self, executor):
            executor._backpressure_policies.append(_DispatchGate())

        def on_execution_step(self, executor):
            stats["num_steps"] += 1
            if len(executor._output_node[1].output_queue) >= 4:
                _DispatchGate.closed = True
            if not executor._consumer_idling() and all(
                op.num_active_tasks() == 0 for op in executor._topology
            ):
                stats["idle_streak"] += 1
            else:
                stats["idle_streak"] = 0

    ray.data.DataContext.get_current().custom_execution_callback_classes.append(
        GateAndStepCounter
    )

    it = iter(_make_payload_dataset().iter_batches(batch_size=_ROWS_PER_BLOCK))
    ids = list(next(it)["id"])
    wait_for_condition(lambda: stats["idle_streak"] >= 5)
    return it, ids, stats


# Raised so that only consumer activity can end the executor's idle wait in time.
_patch_long_idle_wait_timeout = patch(
    "ray.data._internal.execution.streaming_executor."
    "WAIT_FOR_TASK_COMPLETION_TIMEOUT_S",
    60,
)


def _consume_rest(it, ids):
    _DispatchGate.closed = False
    for batch in it:
        ids.extend(batch["id"])
    assert sorted(ids) == list(range(_NUM_BLOCKS * _ROWS_PER_BLOCK))


def test_no_active_tasks_with_unread_output_does_not_busy_spin(
    shutdown_only, restore_data_context  # noqa: F811
):
    it, ids, stats = _iterate_until_idle_with_unread_output()

    steps_before = stats["num_steps"]
    time.sleep(1)
    idle_steps = stats["num_steps"] - steps_before
    assert idle_steps < 50, idle_steps

    _consume_rest(it, ids)


def test_consumer_read_wakes_executor_with_no_active_tasks(
    shutdown_only, restore_data_context  # noqa: F811
):
    it, ids, stats = _iterate_until_idle_with_unread_output()
    with _patch_long_idle_wait_timeout:
        # Let the executor enter a wait that only consumer activity can end soon.
        time.sleep(0.5)
        steps_before = stats["num_steps"]
        start = time.monotonic()
        # Batches the iterator already prefetched don't touch the executor, so
        # read until one does.
        while stats["num_steps"] == steps_before:
            ids.extend(next(it)["id"])
            time.sleep(0.1)
        assert time.monotonic() - start < 10

        start = time.monotonic()
        _consume_rest(it, ids)
        assert time.monotonic() - start < 30


def test_multi_operator_pipeline_never_waits_out_idle_timeout(
    shutdown_only, restore_data_context  # noqa: F811
):
    ray.init(num_cpus=4, object_store_memory=100 * MiB)
    dataset = _make_payload_dataset().map_batches(lambda batch: batch, num_cpus=0.5)

    with _patch_long_idle_wait_timeout:
        start = time.monotonic()
        ids = [
            row_id
            for batch in dataset.iter_batches(batch_size=_ROWS_PER_BLOCK)
            for row_id in batch["id"]
        ]
        elapsed = time.monotonic() - start

    assert elapsed < 30, elapsed
    assert sorted(ids) == list(range(_NUM_BLOCKS * _ROWS_PER_BLOCK))


def test_executor_shuts_down_while_idle_with_unread_output(
    shutdown_only, restore_data_context  # noqa: F811
):
    it, _, _ = _iterate_until_idle_with_unread_output()

    assert _executor_threads()
    it.close()
    assert not _executor_threads()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
