"""Integration tests for BudgetScheduler with Ray Data.

  ┌───────────────────────┬────────┬─────────┬─────────┐
  │         Test          │ Budget │ Default │ Speedup │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ two_stage[1x]         │ 3.46s  │ 4.15s   │ 1.20x   │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ two_stage[5x]         │ 8.35s  │ 9.58s   │ 1.15x   │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ two_stage[10x]        │ 14.25s │ 16.54s  │ 1.16x   │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ equal_time (1:1)      │ 29.40s │ 29.35s  │ 1.00x   │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ slow_producer (10:1)  │ 30.01s │ 30.54s  │ 1.02x   │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ fractional_cpu (8cpu) │ 9.51s  │ 15.41s  │ 1.62x   │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ large_output[10x]     │ 1.61s  │ 1.81s   │ 1.12x   │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ large_output[100x]    │ 7.75s  │ 6.66s   │ 0.86x   │
  ├───────────────────────┼────────┼─────────┼─────────┤
  │ variable_time         │ 6.09s  │ 6.21s   │ 1.02x   │
  └───────────────────────┴────────┴─────────┴─────────┘


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
    avg = sum(times) / len(times)
    print(f"  {label} avg={avg:.2f}s times={[f'{t:.2f}s' for t in times]}")
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v", "-s"]))
