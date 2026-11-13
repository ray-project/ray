import itertools
from typing import Dict, Iterable, List, Optional

import numpy as np
import pyarrow as pa

from ray.data._internal.arrow_ops.transform_pyarrow import concat, shuffle
from ray.data._internal.execution.operators.hash_shuffle_v2 import (
    _make_slice_partition_fn,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_tasks import (
    PartitionFn,
    ReduceFn,
    TaskScopedPartitioner,
)

# A reduce task holds the fetched shards, their concatenation, and the
# permuted copy at once.
_RANDOM_SHUFFLE_REDUCE_PEAK_MEMORY_MULTIPLIER = 3


def _derive_seed(seed: int, *indices: int) -> int:
    """Mix ``seed`` with task, block, or partition indices into a 32-bit seed."""
    return int(np.random.SeedSequence([seed, *indices]).generate_state(1)[0])


class RandomShufflePartitioner(TaskScopedPartitioner):
    """Permute a block's rows, cut it into balanced slices, and send the slices
    to a random permutation of the partitions.

    Each (task, block) pair gets its own seed. If every mapper shared one
    permutation, the same row ranks of every block would land in the same
    partition, which biases partitions whenever input blocks are sorted alike.
    """

    def __init__(self, num_partitions: int, seed: Optional[int]):
        self._num_partitions = num_partitions
        self._seed = seed
        self._slice_fn = _make_slice_partition_fn(num_partitions)

    def bind(self, task_index: int) -> PartitionFn:
        block_indices = itertools.count()

        def _partition(block: pa.Table) -> Dict[int, pa.Table]:
            block_index = next(block_indices)
            if self._seed is None:
                row_seed = target_seed = None
            else:
                # Separate streams: from one seed the row and target permutations
                # are identical when num_rows == num_partitions, undoing the shuffle.
                row_seed = _derive_seed(self._seed, task_index, block_index, 0)
                target_seed = _derive_seed(self._seed, task_index, block_index, 1)
            slices = self._slice_fn(shuffle(block, row_seed))
            # Small blocks yield fewer slices than partitions; spreading them
            # over a random subset keeps low partition ids from filling first.
            targets = np.random.RandomState(target_seed).permutation(
                self._num_partitions
            )
            return {int(targets[i]): table for i, table in slices.items()}

        return _partition


def make_random_shuffle_reduce_fn(seed: Optional[int]) -> ReduceFn:
    """Return a reducer that concatenates a partition's shards and permutes them."""

    def _reduce(
        partition_id: int, tables_by_input: List[List[pa.Table]]
    ) -> Iterable[pa.Table]:
        tables = tables_by_input[0]
        if not tables:
            return
        # preserve_order keeps mixed-schema shards in map task order, so the
        # seeded permutation below starts from the same rows on every run.
        table = (
            concat(tables, promote_types=True, preserve_order=True)
            if len(tables) > 1
            else tables[0]
        )
        partition_seed = None if seed is None else _derive_seed(seed, partition_id)
        yield shuffle(table, partition_seed)

    return _reduce
