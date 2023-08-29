from typing import List, Tuple, TypeVar, Union

import numpy as np

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.sort import SortKey
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.types import ObjectRef

T = TypeVar("T")


class SortTaskSpec(ExchangeTaskSpec):
    """
    The implementation for distributed sort tasks.

    The algorithm is similar to [External Merge Sort]
    (https://en.wikipedia.org/wiki/External_sorting).
    Sorting is done in 3 steps: sampling, sorting individual blocks, and
    merging sorted blocks.

    Sampling (`sample_boundaries`): we get a number of sample items from each block,
    sort them, and use them to compute boundaries that would partition all items into
    approximately equal ranges.

    Sorting (`map`): each block is sorted locally, then partitioned into smaller
    blocks according to the boundaries. Each partitioned block is passed to a merge
    task.

    Merging (`reduce`): a merge task would receive a block from every worker that
    consists of items in a certain range. It then merges the sorted blocks into one
    sorted block and becomes part of the new, sorted block.
    """

    SORT_SAMPLE_SUB_PROGRESS_BAR_NAME = "Sort Sample"

    def __init__(
        self,
        boundaries: List[T],
        sort_key: SortKey,
    ):
        super().__init__(
            map_args=[boundaries, sort_key],
            reduce_args=[sort_key],
        )

    @staticmethod
    def map(
        idx: int,
        block: Block,
        output_num_blocks: int,
        boundaries: List[T],
        sort_key: SortKey,
    ) -> List[Union[BlockMetadata, Block]]:
        stats = BlockExecStats.builder()
        out = BlockAccessor.for_block(block).sort_and_partition(boundaries, sort_key)
        meta = BlockAccessor.for_block(block).get_metadata(
            input_files=None, exec_stats=stats.build()
        )
        return out + [meta]

    @staticmethod
    def reduce(
        sort_key: SortKey,
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> Tuple[Block, BlockMetadata]:
        return BlockAccessor.for_block(mapper_outputs[0]).merge_sorted_blocks(
            mapper_outputs, sort_key
        )

    @staticmethod
    def sample_boundaries(
        blocks: List[ObjectRef[Block]], sort_key: SortKey, num_reducers: int
    ) -> List[T]:
        """
        Return (num_reducers - 1) items in ascending order from the blocks that
        partition the domain into ranges with approximately equally many elements.
        Each boundary item is a tuple of a form (col1_value, col2_value, ...).
        """
        columns = sort_key.get_columns()
        n_samples = int(num_reducers * 10 / len(blocks))

        sample_block = cached_remote_fn(_sample_block)

        sample_results = [
            sample_block.remote(block, n_samples, sort_key) for block in blocks
        ]
        sample_bar = ProgressBar(
            SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME, len(sample_results)
        )
        samples = sample_bar.fetch_until_complete(sample_results)
        sample_bar.close()
        del sample_results
        samples = [s for s in samples if len(s) > 0]
        # The dataset is empty
        if len(samples) == 0:
            return [None] * (num_reducers - 1)
        builder = DelegatingBlockBuilder()
        for sample in samples:
            builder.add_block(sample)
        samples = builder.build()

        sample_dict = BlockAccessor.for_block(samples).to_numpy(columns=columns)
        # Compute sorted indices of the samples. In np.lexsort last key is the
        # primary key hence have to reverse the order.
        indices = np.lexsort(list(reversed(list(sample_dict.values()))))
        # Sort each column by indices, and calculate q-ths quantile items.
        # Ignore the 1st item as it's not required for the boundary
        for k, v in sample_dict.items():
            sorted_v = v[indices]
            sample_dict[k] = [
                np.quantile(sorted_v, q, interpolation="nearest")
                for q in np.linspace(0, 1, num_reducers)
            ][1:]
        # Return the list of boundaries as tuples
        # of a form (col1_value, col2_value, ...)
        return [
            tuple(sample_dict[k][i] for k in sample_dict)
            for i in range(num_reducers - 1)
        ]


def _sample_block(block: Block, n_samples: int, sort_key: SortKey) -> Block:
    return BlockAccessor.for_block(block).sample(n_samples, sort_key)
