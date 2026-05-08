"""Unit tests for HashShuffleAggregator."""

from typing import Dict, Iterator, List

import pyarrow as pa
import pytest

from ray.data._internal.arrow_ops import transform_pyarrow
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShuffleAggregator,
    ShuffleAggregation,
)
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.block import Block

# Access underlying class for direct instantiation (bypassing Ray actor)
_HashShuffleAggregatorClass = HashShuffleAggregator.__ray_actor_class__


def make_block(n: int = 10, offset: int = 0) -> pa.Table:
    return pa.table({"x": list(range(offset, offset + n))})


def split_block(block: pa.Table, chunk_size: int) -> List[pa.Table]:
    """Split block into chunks of given size."""
    return [block.slice(i, chunk_size) for i in range(0, block.num_rows, chunk_size)]


class MockCompactingAggregation(ShuffleAggregation):
    """Tracks compact/finalize calls and input blocks."""

    def __init__(self):
        self.compact_calls: List[int] = []
        self.finalize_input: Dict[int, List[Block]] = {}

    @classmethod
    def is_compacting(cls):
        return True

    def compact(self, shards: List[Block]) -> Block:
        self.compact_calls.append(len(shards))
        return pa.concat_tables(shards) if shards else make_block(0)

    def finalize(self, shards: Dict[int, List[Block]]) -> Iterator[Block]:
        self.finalize_input = dict(shards)
        blocks = [b for bs in shards.values() for b in bs]
        yield pa.concat_tables(blocks) if blocks else make_block(0)


class MockNonCompactingAggregation(ShuffleAggregation):
    """Tracks finalize input blocks."""

    def __init__(self):
        self.finalize_input: Dict[int, List[Block]] = {}

    @classmethod
    def is_compacting(cls):
        return False

    def compact(self, shards: List[Block]) -> Block:
        raise RuntimeError("Should not be called")

    def finalize(self, shards: Dict[int, List[Block]]) -> Iterator[Block]:
        self.finalize_input = dict(shards)
        blocks = [b for bs in shards.values() for b in bs]
        yield pa.concat_tables(blocks) if blocks else make_block(0)


class TestHashShuffleAggregator:
    def test_compacting_workflow(self, ray_start_regular_shared):
        """Tests compaction triggers, threshold doubling, multi-partition/sequence."""
        agg = MockCompactingAggregation()
        aggregator = _HashShuffleAggregatorClass(
            aggregator_id=0,
            num_input_seqs=2,
            target_partition_ids=[0, 1, 2],
            agg_factory=lambda: agg,
            target_max_block_size=None,
            min_max_shards_compaction_thresholds=(3, 2000),
        )

        # Pre-generate blocks: split a 100-row block into 10 chunks of 10 rows
        full_block = make_block(80)
        input_seq0_part0 = split_block(full_block, 10)

        def get_compaction_thresholds():
            """Helper to extract compaction thresholds from partition buckets."""
            # Thresholds are now per-partition in PartitionBucket
            return {
                part_id: bucket.compaction_threshold
                for part_id, bucket in aggregator._input_seq_partition_buckets[
                    0
                ].items()
                if bucket.compaction_threshold is not None
            }

        # Submit 2 blocks (below threshold=3) - no compaction
        for b in input_seq0_part0[:2]:
            aggregator.submit(0, 0, b)
        assert agg.compact_calls == []
        assert get_compaction_thresholds() == {0: 3, 1: 3, 2: 3}

        # Submit 3rd block - triggers compaction, threshold doubles
        aggregator.submit(0, 0, input_seq0_part0[2])
        assert agg.compact_calls == [3]
        assert get_compaction_thresholds() == {0: 6, 1: 3, 2: 3}

        # Submit 5 more (queue: 1+5=6) - triggers at new threshold
        for b in input_seq0_part0[3:8]:
            aggregator.submit(0, 0, b)

        assert agg.compact_calls == [3, 6]
        assert get_compaction_thresholds() == {0: 12, 1: 3, 2: 3}

        # Partition 1 has independent threshold (starts at 3)
        for b in split_block(make_block(30, offset=1000), 10):
            aggregator.submit(0, 1, b)

        assert agg.compact_calls == [3, 6, 3]
        assert get_compaction_thresholds() == {0: 12, 1: 6, 2: 3}

        # Multiple sequences (join scenario) - seq_id=1 for partition 0
        input_seq1_part0 = split_block(make_block(20, offset=2000), 10)
        for b in input_seq1_part0:
            aggregator.submit(1, 0, b)

        # Finalize partition 0 - receives blocks from both sequences
        results = list(aggregator.finalize(0))
        block, metadata = results
        assert len(agg.finalize_input) == 2  # dict with 2 sequences

        # Verify output equals concatenation of seq0 (first 8 chunks) + seq1
        expected = transform_pyarrow.sort(
            pa.concat_tables(tables=[*input_seq0_part0, *input_seq1_part0]),
            sort_key=SortKey("x"),
        )
        assert transform_pyarrow.sort(block, sort_key=SortKey("x")) == expected

        # Empty partition
        results = list(aggregator.finalize(2))
        assert results[0] == make_block(0)

    def test_non_compacting_workflow(self, ray_start_regular_shared):
        """Tests non-compacting aggregation with and without block splitting."""
        # Without splitting
        full_block = make_block(50)
        input_seq = split_block(full_block, 10)

        aggregator = _HashShuffleAggregatorClass(
            aggregator_id=1,
            num_input_seqs=1,
            target_partition_ids=[0],
            agg_factory=MockNonCompactingAggregation,
            target_max_block_size=None,
        )
        for b in input_seq:
            aggregator.submit(0, 0, b)

        results = list(aggregator.finalize(0))
        block, metadata = results
        assert block == full_block

        # With splitting - output blocks should reconstruct to original
        full_block = make_block(500)
        input_seq = split_block(full_block, 100)

        aggregator = _HashShuffleAggregatorClass(
            aggregator_id=2,
            num_input_seqs=1,
            target_partition_ids=[0],
            agg_factory=MockNonCompactingAggregation,
            target_max_block_size=50,
        )
        for b in input_seq:
            aggregator.submit(0, 0, b)

        results = list(aggregator.finalize(0))
        output_blocks = [results[i] for i in range(0, len(results), 2)]
        assert pa.concat_tables(output_blocks) == full_block


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
