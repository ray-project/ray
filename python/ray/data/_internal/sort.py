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

# from ray.data._internal.block_list import BlockList
# from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
# from ray.data._internal.execution.interfaces import TaskContext
# from ray.data._internal.progress_bar import ProgressBar
# from ray.data._internal.remote_fn import cached_remote_fn
# from ray.data._internal.shuffle import ShuffleOp, SimpleShufflePlan
from ray.data.block import Block, BlockAccessor  # , BlockExecStats, BlockMetadata

# import numpy as np

# from ray.data.context import DataContext
# from ray.types import ObjectRef

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


# class _SortOp(ShuffleOp):
#     @staticmethod
#     def map(
#         idx: int,
#         block: Block,
#         output_num_blocks: int,
#         boundaries: List[T],
#         sort_key: SortKey,
#     ) -> List[Union[BlockMetadata, Block]]:
#         stats = BlockExecStats.builder()
#         out = BlockAccessor.for_block(block).sort_and_partition(boundaries, sort_key)
#         meta = BlockAccessor.for_block(block).get_metadata(
#             input_files=None, exec_stats=stats.build()
#         )
#         return out + [meta]

#     @staticmethod
#     def reduce(
#         sort_key: SortKey,
#         *mapper_outputs: List[Block],
#         partial_reduce: bool = False,
#     ) -> (Block, BlockMetadata):
#         return BlockAccessor.for_block(mapper_outputs[0]).merge_sorted_blocks(
#             mapper_outputs, sort_key
#         )


# class SimpleSortOp(_SortOp, SimpleShufflePlan):
#     pass


def _sample_block(block: Block, n_samples: int, sort_key: SortKey) -> Block:
    return BlockAccessor.for_block(block).sample(n_samples, sort_key)
