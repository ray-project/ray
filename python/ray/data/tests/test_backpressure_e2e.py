import threading
import time
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import ray
from ray._private.internal_api import memory_summary
from ray.data._internal.execution.backpressure_policy.downstream_capacity_backpressure_policy import (
    DownstreamCapacityBackpressurePolicy,
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


def _drain_split_and_measure(ds, num_consumers, watch_s):
    """Drain every shard on its own thread; return (rows, total_gap_seconds).

    Returns the largest gap between consecutive deliveries, with a trailing term
    so a pipeline that stops and never resumes does not look gap-free.
    """
    stamps = [time.time()]
    delivered = [0]
    lock = threading.Lock()

    def drain(split):
        try:
            for batch in split.iter_batches(batch_size=1):
                with lock:
                    stamps.append(time.time())
                    delivered[0] += len(batch["i"])
        except Exception:
            pass

    splits = ds.streaming_split(num_consumers, equal=True)
    threads = [
        threading.Thread(target=drain, args=(split,), daemon=True) for split in splits
    ]
    start = time.time()
    try:
        for t in threads:
            t.start()
        while time.time() - start < watch_s and any(t.is_alive() for t in threads):
            time.sleep(0.5)
        with lock:
            seen = sorted(stamps)
            rows = delivered[0]
    finally:
        # Release wedged consumers before Ray is torn down; `ray.shutdown()`
        # with threads blocked in the split coordinator aborts the process.
        ray.kill(splits[0]._coord_actor, no_restart=True)
        for t in threads:
            t.join(timeout=10)

    gaps = [b - a for a, b in zip(seen, seen[1:])] + [time.time() - seen[-1]]
    return rows, max(gaps)


@pytest.mark.timeout(180)
@pytest.mark.xfail(
    strict=True,
    reason=(
        "OutputSplitter buffers before it dispatches, and the amount scales with "
        "the consumer count while the object store budget does not. Past the "
        "point where that buffering exceeds the budget, the producer is throttled "
        "before it can fill the buffer, so the splitter never dispatches and "
        "nothing drains to free the budget."
    ),
)
def test_streaming_split_buffering_must_fit_object_store_budget(
    shutdown_only, restore_data_context  # noqa: F811
):
    """`OutputSplitter`'s buffering must fit the object store budget.

    Why: two of its buffering requirements scale with the consumer count `n`,
    and neither is accounted for in the budget:
      - the locality cap, `2 * n` bundles, counted not measured
        (output_splitter.py:99, :251), active when `locality_hints` are set;
      - the `equal=True` reserve, which needs enough buffered rows to equalize
        across `n` outputs (`_can_safely_dispatch` -> `_calculate_buffer_requirement`).
    Blocks held in that buffer are billed to their producer, so the producer needs
    headroom for the whole amount before the splitter will release anything.

    What: holds memory pressure and data volume constant and varies only `n`. The
    control leg (small `n`) is what rules out ordinary budget exhaustion; without
    it this would be indistinguishable from that. Both requirements are active
    here, so this shows the class of failure, not which of the two dominates.

    Fails when the large-`n` leg delivers zero rows while the control leg
    delivers everything.
    """
    store_mib = 300
    block_mib = 5
    # Enough data that the producer cannot simply finish -- `all_inputs_done`
    # force-flushes the buffer (output_splitter.py:185) and would mask the stall.
    num_blocks = 200
    rows_per_block = 4
    watch_s = 25.0

    # Data's object store budget is half the store, so ~150MiB here.
    #   n=2  -> 2*2*5MiB   =  20MiB, comfortably inside the budget
    #   n=32 -> 2*32*5MiB  = 320MiB, more than twice the budget
    control_consumers = 2
    subject_consumers = 32

    ray.init(num_cpus=4, object_store_memory=store_mib * MiB)

    def make_ds():
        return ray.data.range(
            num_blocks * rows_per_block, override_num_blocks=num_blocks
        ).map(
            lambda row: {
                "i": row["id"],
                "pad": np.zeros(block_mib * MiB // rows_per_block // 8),
            }
        )

    # Control leg: same pressure, priming volume inside the budget.
    control_rows, control_gap = _drain_split_and_measure(
        make_ds(), control_consumers, watch_s
    )
    if control_rows == 0:
        pytest.skip(
            f"control leg (n={control_consumers}) delivered nothing either, so "
            f"this sizing no longer isolates the priming requirement"
        )

    subject_rows, subject_gap = _drain_split_and_measure(
        make_ds(), subject_consumers, watch_s
    )

    assert subject_rows > 0, (
        f"n={subject_consumers} delivered {subject_rows} rows in {watch_s}s "
        f"(largest gap {subject_gap:.1f}s), while n={control_consumers} delivered "
        f"{control_rows} rows under identical memory pressure. The only "
        f"difference is the consumer count, which sets how much the splitter "
        f"buffers before dispatching ({2 * subject_consumers} bundles = "
        f"{2 * subject_consumers * block_mib}MiB) against a ~{store_mib // 2}MiB "
        f"budget."
    )


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
