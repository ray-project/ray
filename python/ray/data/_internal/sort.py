"""
We implement a distributed sorting algorithm similar to
[External Merge Sort](https://en.wikipedia.org/wiki/External_sorting).
Sorting is done in 3 stages: sampling, sorting individual blocks, and
merging sorted blocks.

Sampling: we get a number of sample items from each block, sort them, and
use them to compute boundaries that would partition all items into
approximately equal ranges.

Sorting: each block is sorted locally, then partitioned into smaller blocks
according to the boundaries. Each partitioned block is passed to a merge task.
This is an all-to-all shuffle.

Merging: a merge task would receive a block from every worker that consists
of items in a certain range. It then merges the sorted blocks into one sorted
block and becomes part of the new, sorted dataset.
"""
from typing import TYPE_CHECKING, List, Optional, Tuple, TypeVar, Union

import numpy as np

from ray.data._internal.block_list import BlockList
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.push_based_shuffle import PushBasedShufflePlan
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.shuffle import ShuffleOp, SimpleShufflePlan
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.data.context import DataContext
from ray.types import ObjectRef

if TYPE_CHECKING:
    import pyarrow

T = TypeVar("T")


class SortKey:
    """SortKey class to convert between different sort args formats."""

    def __init__(
        self,
        key: Optional[Union[str, List[str]]] = None,
        descending: Union[bool, List[bool]] = False,
    ):
        if key is None:
            key = []
        if isinstance(key, str):
            key = [key]
        if not (isinstance(key, list) and all(isinstance(k, str) for k in key)):
            raise ValueError(
                f"Key must be a string or a list of strings, but got {key}."
            )
        if isinstance(descending, bool):
            descending = [descending for _ in key]
        elif isinstance(descending, list):
            if len(descending) != len(key):
                raise ValueError(
                    "Length of `descending` does not match the length of the key."
                )
            if len(set(descending)) != 1:
                raise ValueError("Sorting with mixed key orders not supported yet.")
        self._columns = key
        self._descending = descending

    def get_columns(self) -> List[str]:
        return self._columns

    def get_descending(self) -> bool:
        return self._descending[0]

    def to_arrow_sort_args(self) -> List[Tuple[str, str]]:
        return [
            (key, "descending" if self._descending[0] else "ascending")
            for key in self._columns
        ]

    def to_pandas_sort_args(self) -> Tuple[List[str], bool]:
        return self._columns, not self._descending[0]

    def validate_schema(self, schema: Optional[Union[type, "pyarrow.lib.Schema"]]):
        """Check the key function is valid on the given schema."""
        if schema is None:
            # Dataset is empty/cleared, validation not possible.
            return

        if self._columns and len(schema.names) > 0:
            for column in self._columns:
                if column not in schema.names:
                    raise ValueError(
                        "The column '{}' does not exist in the "
                        "schema '{}'.".format(column, schema)
                    )


class _SortOp(ShuffleOp):
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
    ) -> (Block, BlockMetadata):
        return BlockAccessor.for_block(mapper_outputs[0]).merge_sorted_blocks(
            mapper_outputs, sort_key
        )


class SimpleSortOp(_SortOp, SimpleShufflePlan):
    pass


class PushBasedSortOp(_SortOp, PushBasedShufflePlan):
    pass


def sample_boundaries(
    blocks: List[ObjectRef[Block]],
    sort_key: SortKey,
    num_reducers: int,
    ctx: Optional[TaskContext] = None,
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

    should_close_bar = True
    if ctx is not None and ctx.sub_progress_bar_dict is not None:
        bar_name = "SortSample"
        assert bar_name in ctx.sub_progress_bar_dict, ctx.sub_progress_bar_dict
        sample_bar = ctx.sub_progress_bar_dict[bar_name]
        should_close_bar = False
    else:
        sample_bar = ProgressBar("Sort Sample", len(sample_results))

    samples = sample_bar.fetch_until_complete(sample_results)

    if should_close_bar:
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
        tuple(sample_dict[k][i] for k in sample_dict) for i in range(num_reducers - 1)
    ]


# Note: currently the map_groups() API relies on this implementation
# to partition the same key into the same block.
def sort_impl(
    blocks: BlockList,
    clear_input_blocks: bool,
    sort_key: SortKey,
    ctx: Optional[TaskContext] = None,
) -> Tuple[BlockList, dict]:
    stage_info = {}
    blocks_list = blocks.get_blocks()
    if len(blocks_list) == 0:
        return BlockList([], []), stage_info

    num_mappers = len(blocks_list)
    # Use same number of output partitions.
    num_reducers = num_mappers
    # TODO(swang): sample_boundaries could be fused with a previous stage.
    boundaries = sample_boundaries(blocks_list, sort_key, num_reducers, ctx)
    _, ascending = sort_key.to_pandas_sort_args()
    if not ascending:
        boundaries.reverse()

    context = DataContext.get_current()
    if context.use_push_based_shuffle:
        sort_op_cls = PushBasedSortOp
    else:
        sort_op_cls = SimpleSortOp
    sort_op = sort_op_cls(map_args=[boundaries, sort_key], reduce_args=[sort_key])
    return sort_op.execute(
        blocks,
        num_reducers,
        clear_input_blocks,
    )


def _sample_block(block: Block, n_samples: int, sort_key: SortKey) -> Block:
    return BlockAccessor.for_block(block).sample(n_samples, sort_key)
