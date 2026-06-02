"""Integration test for BudgetScheduler with Ray Data.

  ┌────────┬──────────────────┬─────────┬─────────┐
  │ Factor │ Budget Scheduler │ Default │ Speedup │
  ├────────┼──────────────────┼─────────┼─────────┤
  │ 1x     │ 1.40s            │ 1.63s   │ 1.16x   │
  ├────────┼──────────────────┼─────────┼─────────┤
  │ 5x     │ 3.30s            │ 3.92s   │ 1.19x   │
  ├────────┼──────────────────┼─────────┼─────────┤
  │ 10x    │ 5.66s            │ 6.89s   │ 1.22x   │
  └────────┴──────────────────┴─────────┴─────────┘

"""

import time

import numpy as np
import pytest

import ray

NUM_BLOCKS = 80
BLOCK_SIZE = 1024 * 1024  # 1 MiB
NUM_RUNS = 3


def _warmup():
    """Run a small Ray Data job to warm up workers and object store."""
    ray.data.range(10).map_batches(lambda b: b).materialize()


def _run_pipeline(amplification_factor):
    """Run the produce -> consume pipeline once and return elapsed seconds."""
    ctx = ray.data.DataContext.get_current()
    ctx.use_budget_scheduler = True
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


@pytest.mark.parametrize("amplification_factor", [1, 5, 10])
def test_budget_scheduler_two_stage_pipeline(amplification_factor):
    """Test that the BudgetScheduler works end-to-end with a produce -> consume
    pipeline. Fusion is disabled by giving the consumer a different num_cpus.

    The amplification_factor controls how many BLOCK_SIZE output blocks each
    producer task yields per input row, exercising the scheduler's ability to
    adapt its peak-ratio EMA to varying output sizes.
    """
    # 200 MiB object store limit.
    ray.init(num_cpus=2, object_store_memory=200 * 1024 * 1024)
    try:
        _warmup()

        times = []
        for i in range(NUM_RUNS):
            elapsed = _run_pipeline(amplification_factor)
            times.append(elapsed)
            print(
                f"  factor={amplification_factor} run={i + 1}/{NUM_RUNS} "
                f"time={elapsed:.2f}s"
            )

        avg = sum(times) / len(times)
        print(
            f"  factor={amplification_factor} avg={avg:.2f}s "
            f"times={[f'{t:.2f}s' for t in times]}"
        )
    finally:
        ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v", "-s"]))
