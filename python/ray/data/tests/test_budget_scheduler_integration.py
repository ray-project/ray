"""Integration tests for BudgetScheduler with Ray Data.

  ┌───────────────────────┬─────────────────┬──────────────────┬─────────┐
  │         Test          │ Budget (median) │ Default (median) │ Speedup │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ two_stage[1x]         │ 3.43s           │ 3.88s            │ 1.13x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ two_stage[5x]         │ 8.35s           │ 9.65s            │ 1.16x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ two_stage[10x]        │ 14.37s          │ 16.38s           │ 1.14x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ equal_time (1:1)      │ 29.70s          │ 29.40s           │ 0.99x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ slow_producer (10:1)  │ 31.09s          │ 30.06s           │ 0.97x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ fractional_cpu (8cpu) │ 11.26s          │ 15.73s           │ 1.40x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ large_output[10x]     │ 1.27s           │ 1.53s            │ 1.20x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ large_output[100x]    │ 6.21s           │ 7.49s            │ 1.21x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ iterator_start_stop   │ 3.35s           │ 3.90s            │ 1.16x   │
  ├───────────────────────┼─────────────────┼──────────────────┼─────────┤
  │ variable_time         │ 6.16s           │ 6.29s            │ 1.02x   │
  └───────────────────────┴─────────────────┴──────────────────┴─────────┘

"""

import time

import numpy as np
import pytest

import ray

NUM_BLOCKS = 200
BLOCK_SIZE = 1024 * 1024  # 1 MiB
OBJECT_STORE_MEM = 100 * BLOCK_SIZE
NUM_RUNS = 3


@pytest.fixture(params=[True, False], ids=["budget", "default"])
def use_budget_scheduler(request):
    return request.param


def _warmup():
    """Run a small Ray Data job to warm up workers and object store."""
    ray.data.range(10).map_batches(lambda b: b).materialize()


def _benchmark(fn, num_runs=NUM_RUNS, label=""):
    """Run fn() num_runs times and print timing results. Returns list of times."""
    times = []
    for i in range(num_runs):
        elapsed = fn()
        times.append(elapsed)
        print(f"  {label} run={i + 1}/{num_runs} time={elapsed:.2f}s")
    s = sorted(times)
    median = s[len(s) // 2]
    print(
        f"  {label} median={median:.2f}s min={s[0]:.2f}s max={s[-1]:.2f}s"
        f" times={[f'{t:.2f}s' for t in times]}"
    )
    return times


# ---------------------------------------------------------------------------
# Test 1: Variable amplification (original)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("amplification_factor", [1, 5, 10])
def test_two_stage_pipeline(use_budget_scheduler, amplification_factor):
    """Produce -> consume pipeline with variable output amplification.

    Fusion is disabled by giving the consumer a different num_cpus.
    """
    ray.init(num_cpus=2, object_store_memory=OBJECT_STORE_MEM)
    try:
        _warmup()

        def run():
            ctx = ray.data.DataContext.get_current()
            ctx.use_budget_scheduler = use_budget_scheduler
            ctx.target_max_block_size = BLOCK_SIZE

            def produce(batch):
                for _ in batch["id"]:
                    for _ in range(amplification_factor):
                        yield {
                            "id": [1],
                            "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                        }

            def consume(batch):
                del batch["data"]
                return {"id": batch["id"]}

            ds = ray.data.range(NUM_BLOCKS, override_num_blocks=NUM_BLOCKS)
            ds = ds.map_batches(produce, batch_size=1)
            ds = ds.map_batches(consume, batch_size=None, num_cpus=0.99)

            start = time.perf_counter()
            result = ds.materialize()
            elapsed = time.perf_counter() - start

            assert result.count() == NUM_BLOCKS * amplification_factor
            return elapsed

        _benchmark(run, label=f"factor={amplification_factor}")
    finally:
        ray.shutdown()


# ---------------------------------------------------------------------------
# Test 2: Equal processing time (A:B = 1:1)
# ---------------------------------------------------------------------------


def test_equal_processing_time(use_budget_scheduler):
    """A and B both sleep the same amount per task."""
    ray.init(num_cpus=2, object_store_memory=OBJECT_STORE_MEM)
    try:
        _warmup()

        def run():
            ctx = ray.data.DataContext.get_current()
            ctx.use_budget_scheduler = use_budget_scheduler
            ctx.target_max_block_size = BLOCK_SIZE

            def slow_produce(batch):
                time.sleep(0.05)
                for _ in batch["id"]:
                    yield {
                        "id": [1],
                        "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                    }

            def slow_consume(batch):
                time.sleep(0.05)
                del batch["data"]
                return {"id": batch["id"]}

            ds = ray.data.range(NUM_BLOCKS, override_num_blocks=NUM_BLOCKS)
            ds = ds.map_batches(slow_produce, batch_size=1)
            ds = ds.map_batches(slow_consume, batch_size=None, num_cpus=0.99)

            start = time.perf_counter()
            result = ds.materialize()
            elapsed = time.perf_counter() - start

            assert result.count() == NUM_BLOCKS
            return elapsed

        _benchmark(run, label="A:B=1:1")
    finally:
        ray.shutdown()


# ---------------------------------------------------------------------------
# Test 3: Slow producer (A:B = 10:1)
# ---------------------------------------------------------------------------


def test_slow_producer(use_budget_scheduler):
    """Producer is 10x slower than consumer."""
    ray.init(num_cpus=2, object_store_memory=OBJECT_STORE_MEM)
    try:
        _warmup()

        def run():
            ctx = ray.data.DataContext.get_current()
            ctx.use_budget_scheduler = use_budget_scheduler
            ctx.target_max_block_size = BLOCK_SIZE

            def slow_produce(batch):
                time.sleep(0.1)
                for _ in batch["id"]:
                    yield {
                        "id": [1],
                        "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                    }

            def fast_consume(batch):
                time.sleep(0.01)
                del batch["data"]
                return {"id": batch["id"]}

            ds = ray.data.range(NUM_BLOCKS, override_num_blocks=NUM_BLOCKS)
            ds = ds.map_batches(slow_produce, batch_size=1)
            ds = ds.map_batches(fast_consume, batch_size=None, num_cpus=0.99)

            start = time.perf_counter()
            result = ds.materialize()
            elapsed = time.perf_counter() - start

            assert result.count() == NUM_BLOCKS
            return elapsed

        _benchmark(run, label="A:B=10:1")
    finally:
        ray.shutdown()


# ---------------------------------------------------------------------------
# Test 4: Optimal fractional CPU allocation (8 CPUs, A:B cost = 2:1)
# ---------------------------------------------------------------------------


def test_fractional_cpu_allocation(use_budget_scheduler):
    """With 8 CPUs and A twice as slow as B, the optimal split is ~5A + 3B.

    The scheduler should keep both stages busy without over-allocating to the
    faster consumer.
    """
    ray.init(num_cpus=8, object_store_memory=OBJECT_STORE_MEM)
    try:
        _warmup()

        def run():
            ctx = ray.data.DataContext.get_current()
            ctx.use_budget_scheduler = use_budget_scheduler
            ctx.target_max_block_size = BLOCK_SIZE

            def slow_produce(batch):
                time.sleep(0.1)
                for _ in batch["id"]:
                    yield {
                        "id": [1],
                        "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                    }

            def fast_consume(batch):
                time.sleep(0.05)
                del batch["data"]
                return {"id": batch["id"]}

            ds = ray.data.range(NUM_BLOCKS, override_num_blocks=NUM_BLOCKS)
            ds = ds.map_batches(slow_produce, batch_size=1)
            ds = ds.map_batches(fast_consume, batch_size=None, num_cpus=0.99)

            start = time.perf_counter()
            result = ds.materialize()
            elapsed = time.perf_counter() - start

            assert result.count() == NUM_BLOCKS
            return elapsed

        _benchmark(run, label="8cpu-A:B=2:1")
    finally:
        ray.shutdown()


# ---------------------------------------------------------------------------
# Test 5: Producer produces large output (10x and 100x amplification)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("amplification_factor", [10, 100])
def test_large_producer_output(use_budget_scheduler, amplification_factor):
    """A (producer) produces amplification_factor * BLOCK_SIZE per input.

    Tests that the scheduler handles a stage that greatly expands data volume,
    applying backpressure to avoid OOM.
    """
    # Amplification factor 10 means that object store is full after 10 tasks run.
    # Amplification factor 100 means that object store is full after 1 task runs.
    ray.init(num_cpus=8, object_store_memory=OBJECT_STORE_MEM)
    try:
        _warmup()

        def run():
            ctx = ray.data.DataContext.get_current()
            ctx.use_budget_scheduler = use_budget_scheduler
            ctx.target_max_block_size = BLOCK_SIZE
            num_blocks = 20  # Fewer blocks since output is large.

            def produce(batch):
                for _ in batch["id"]:
                    yield {
                        "id": [1],
                        "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                    }

            def amplify(batch):
                for _ in batch["id"]:
                    for _ in range(amplification_factor):
                        yield {
                            "id": [1],
                            "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                        }

            def consume(batch):
                del batch["data"]
                return {"id": batch["id"]}

            ds = ray.data.range(num_blocks, override_num_blocks=num_blocks)
            ds = ds.map_batches(produce, batch_size=1)
            ds = ds.map_batches(amplify, batch_size=1)
            ds = ds.map_batches(consume, batch_size=None, num_cpus=0.98)

            start = time.perf_counter()
            result = ds.materialize()
            elapsed = time.perf_counter() - start

            assert result.count() == num_blocks * amplification_factor
            return elapsed

        _benchmark(run, label=f"large-output-{amplification_factor}x")
    finally:
        ray.shutdown()


# ---------------------------------------------------------------------------
# Test 6: Dataset iterator (start/stop consumption)
# ---------------------------------------------------------------------------


def test_iterator_start_stop(use_budget_scheduler):
    """Consume via iter_batches with pauses to simulate bursty consumption.

    The scheduler must handle periods of no consumption (backpressure)
    followed by rapid draining.
    """
    ray.init(num_cpus=2, object_store_memory=OBJECT_STORE_MEM)
    try:
        _warmup()

        ctx = ray.data.DataContext.get_current()
        ctx.use_budget_scheduler = use_budget_scheduler
        ctx.target_max_block_size = BLOCK_SIZE

        def produce(batch):
            for _ in batch["id"]:
                yield {
                    "id": [1],
                    "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                }

        ds = ray.data.range(NUM_BLOCKS, override_num_blocks=NUM_BLOCKS)
        ds = ds.map_batches(produce, batch_size=1)

        start = time.perf_counter()
        total_rows = 0
        for i, batch in enumerate(ds.iter_batches(batch_size=1)):
            total_rows += len(batch["id"])
            # Pause every 10 batches to simulate bursty consumption.
            if i % 10 == 9:
                time.sleep(0.1)
        elapsed = time.perf_counter() - start

        assert total_rows == NUM_BLOCKS
        print(f"  iterator-start-stop time={elapsed:.2f}s rows={total_rows}")
    finally:
        ray.shutdown()


# ---------------------------------------------------------------------------
# Test 7: Variable processing time over time
# ---------------------------------------------------------------------------


def test_variable_processing_time(use_budget_scheduler):
    """A:B processing ratio changes over time.

    Producer starts slow and speeds up; consumer starts fast and slows down.
    The scheduler must adapt its EMA estimates to the changing ratios.
    """
    ray.init(num_cpus=2, object_store_memory=OBJECT_STORE_MEM)
    try:
        _warmup()

        def run():
            ctx = ray.data.DataContext.get_current()
            ctx.use_budget_scheduler = use_budget_scheduler
            ctx.target_max_block_size = BLOCK_SIZE

            produce_call = [0]
            consume_call = [0]

            def variable_produce(batch):
                # Starts at 100ms, decreases toward 10ms.
                n = produce_call[0]
                produce_call[0] += 1
                delay = max(0.01, 0.1 - n * 0.002)
                time.sleep(delay)
                for _ in batch["id"]:
                    yield {
                        "id": [1],
                        "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                    }

            def variable_consume(batch):
                # Starts at 10ms, increases toward 100ms.
                n = consume_call[0]
                consume_call[0] += 1
                delay = min(0.1, 0.01 + n * 0.002)
                time.sleep(delay)
                del batch["data"]
                return {"id": batch["id"]}

            num_blocks = 40
            ds = ray.data.range(num_blocks, override_num_blocks=num_blocks)
            ds = ds.map_batches(variable_produce, batch_size=1)
            ds = ds.map_batches(variable_consume, batch_size=None, num_cpus=0.99)

            start = time.perf_counter()
            result = ds.materialize()
            elapsed = time.perf_counter() - start

            assert result.count() == num_blocks
            return elapsed

        _benchmark(run, label="variable-time")
    finally:
        ray.shutdown()


# ---------------------------------------------------------------------------
# Test 8: Multi-node two-stage pipeline (4 nodes, 2 CPUs each)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("amplification_factor", [1, 5, 10])
def test_multinode_two_stage_pipeline(use_budget_scheduler, amplification_factor):
    """Same as test_two_stage_pipeline but on a simulated 4-node cluster.

    Each node has 2 CPUs and a fraction of the object store, testing that
    the budget scheduler handles distributed memory correctly.
    """
    from ray.cluster_utils import Cluster

    num_nodes = 4
    cpus_per_node = 2
    # Each worker node gets 200 MiB object store so the total cluster
    # budget (~400 MiB after the 0.5 fraction) is comparable to the
    # single-node tests.
    object_store_per_node = 200 * 1024 * 1024

    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    for _ in range(num_nodes):
        cluster.add_node(
            num_cpus=cpus_per_node,
            object_store_memory=object_store_per_node,
        )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    try:
        _warmup()

        def run():
            ctx = ray.data.DataContext.get_current()
            ctx.use_budget_scheduler = use_budget_scheduler
            ctx.target_max_block_size = BLOCK_SIZE

            def produce(batch):
                for _ in batch["id"]:
                    for _ in range(amplification_factor):
                        yield {
                            "id": [1],
                            "data": [np.zeros(BLOCK_SIZE, dtype=np.uint8)],
                        }

            def consume(batch):
                del batch["data"]
                return {"id": batch["id"]}

            ds = ray.data.range(NUM_BLOCKS, override_num_blocks=NUM_BLOCKS)
            ds = ds.map_batches(produce, batch_size=1)
            ds = ds.map_batches(consume, batch_size=None, num_cpus=0.99)

            start = time.perf_counter()
            result = ds.materialize()
            elapsed = time.perf_counter() - start

            assert result.count() == NUM_BLOCKS * amplification_factor
            return elapsed

        _benchmark(run, label=f"multinode-factor={amplification_factor}")
    finally:
        ray.shutdown()
        cluster.shutdown()


# ---------------------------------------------------------------------------
# Test 10: Heterogeneous cluster spilling stress test
# ---------------------------------------------------------------------------


def test_heterogeneous_spilling(use_budget_scheduler):
    """Demonstrates the global-budget blind spot on heterogeneous clusters.

    Cluster layout:
      2 CPU nodes: 2 CPUs, 100 MiB object store each  (200 MiB total CPU)
      1 GPU node:  0 CPUs, 1 GPU, 400 MiB object store

    Global budget = (200 + 400) * 0.5 = 300 MiB.

    The producer (CPU) amplifies each input 10x (10 MiB per task).
    The consumer (GPU) runs on the single GPU node.

    Because the global budget (300 MiB) exceeds the total CPU-side object
    store (200 MiB), the scheduler can authorize more produce output than
    the CPU nodes can hold, causing spilling. A per-node-aware scheduler
    would cap produce output at ~200 MiB and avoid spilling entirely.
    """
    from ray._private.internal_api import get_memory_info_reply, get_state_from_address
    from ray.cluster_utils import Cluster

    # Use 2 CPU nodes + 1 GPU node to keep local test fast.
    # CPU-side: 2 nodes × 100 MiB = 200 MiB
    # GPU-side: 1 node × 400 MiB
    # Global budget: (200 + 400) × 0.5 = 300 MiB > 200 MiB CPU-side
    cluster = Cluster()
    cluster.add_node(num_cpus=0)
    for _ in range(2):
        cluster.add_node(
            num_cpus=2,
            object_store_memory=100 * 1024 * 1024,
        )
    cluster.add_node(
        num_cpus=0,
        num_gpus=1,
        object_store_memory=400 * 1024 * 1024,
    )
    cluster.wait_for_nodes()
    ray.init(address=cluster.address)

    try:
        ctx = ray.data.DataContext.get_current()
        ctx.use_budget_scheduler = use_budget_scheduler
        ctx.target_max_block_size = 2 * BLOCK_SIZE

        amplification = 10  # Each produce task: 1 input → 10 MiB output

        def produce(batch):
            for _ in batch["id"]:
                for _ in range(amplification):
                    yield {
                        "id": [1],
                        "data": [np.zeros(2 * BLOCK_SIZE, dtype=np.uint8)],
                    }

        def gpu_consume(batch):
            time.sleep(0.010)
            del batch["data"]
            return {"id": batch["id"]}

        num_blocks = 20
        ds = ray.data.range(num_blocks, override_num_blocks=num_blocks)
        ds = ds.map_batches(produce, batch_size=1)
        ds = ds.map_batches(gpu_consume, batch_size=1, num_cpus=0, num_gpus=1)

        start = time.perf_counter()
        result = ds.materialize()
        elapsed = time.perf_counter() - start

        assert result.count() == num_blocks * amplification

        # Check spilling stats.
        memory_info = get_memory_info_reply(
            get_state_from_address(ray.get_runtime_context().gcs_address)
        )
        spilled = memory_info.store_stats.spilled_bytes_total
        spilled_mib = spilled / (1024 * 1024)

        print(
            f"  heterogeneous-spilling "
            f"scheduler={'budget' if use_budget_scheduler else 'default'} "
            f"time={elapsed:.2f}s "
            f"spilled={spilled_mib:.1f}MiB"
        )
    finally:
        ray.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v", "-s"]))
