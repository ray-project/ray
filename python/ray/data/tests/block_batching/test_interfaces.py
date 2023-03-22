import pytest

import ray
from ray.data._internal.block_batching.interfaces import LogicalBatch

def test_logical_batch_resolves_blocks(ray_start_regular_shared):
    block_refs = [ray.put(1), ray.put(2)]
    logical_batch = LogicalBatch(batch_idx=0, block_refs=block_refs, starting_block_idx=0, ending_block_idx=None, num_rows=2)

    # Blocks should not be accessible before calling resolve().
    with pytest.raises(RuntimeError):
        logical_batch.blocks

    logical_batch.resolve()
    assert logical_batch.blocks == [1, 2]

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))