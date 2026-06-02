"""Integration test for BudgetScheduler with Ray Data."""

import numpy as np
import pytest

import ray


def test_budget_scheduler_two_stage_pipeline():
    """Test that the BudgetScheduler works end-to-end with a produce -> consume
    pipeline. Fusion is disabled by giving the consumer a different num_cpus."""
    # 200 MiB object store limit.
    ray.init(num_cpus=2, object_store_memory=200 * 1024 * 1024)
    try:
        ctx = ray.data.DataContext.get_current()
        ctx.use_budget_scheduler = True

        NUM_BLOCKS = 20
        BLOCK_SIZE = 1024 * 1024  # 1 MiB
        ctx.target_max_block_size = BLOCK_SIZE

        def produce(batch):
            for _ in batch["id"]:
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

        _ = ds.materialize()
        print(ds.stats())
    finally:
        ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
