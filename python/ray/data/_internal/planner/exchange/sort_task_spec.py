from typing import TYPE_CHECKING, List, Optional, Tuple, TypeVar, Union

import numpy as np

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.planner.exchange.interfaces import ExchangeTaskSpec
from ray.data._internal.progress_bar import ProgressBar
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data._internal.table_block import TableBlockAccessor
from ray.data._internal.util import NULL_SENTINEL
from ray.data.block import Block, BlockAccessor, BlockExecStats, BlockMetadata
from ray.types import ObjectRef

T = TypeVar("T")

if TYPE_CHECKING:
    import pyarrow


class SortKey:
    """SortKey class to convert between different sort args formats."""

    def __init__(
        self,
        key: Optional[Union[str, List[str]]] = None,
        descending: Union[bool, List[bool]] = False,
        boundaries: Optional[List[T]] = None,
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
        self._columns = key
        self._descending = descending
        if boundaries:
            for item in boundaries:
                if not isinstance(item, (int, float)):
                    raise ValueError(
                        "The type of items in boundaries must be int or float."
                    )
            boundaries = list(set(boundaries))
            boundaries.sort()
        self._boundaries = boundaries

    def get_columns(self) -> List[str]:
        return self._columns

    def get_descending(self) -> List[bool]:
        return self._descending

    def to_arrow_sort_args(self) -> List[Tuple[str, str]]:
        return [
            (key, "descending" if desc else "ascending")
            for key, desc in zip(self._columns, self._descending)
        ]

    def to_pandas_sort_args(self) -> Tuple[List[str], List[bool]]:
        return self._columns, [not desc for desc in self._descending]

    def validate_schema(self, schema: Optional[Union[type, "pyarrow.lib.Schema"]]):
        """Check the key function is valid on the given schema."""
        if schema is None:
            # Dataset is empty/cleared, validation not possible.
            return

        if self._columns and len(schema.names) > 0:
            schema_names_set = set(schema.names)
            for column in self._columns:
                if column not in schema_names_set:
                    raise ValueError(
                        f"You specified the column '{column}', but there's no such "
                        "column in the dataset. The dataset has columns: "
                        f"{schema.names}"
                    )

    @property
    def boundaries(self):
        return self._boundaries


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
        batch_format: str,
    ):
        super().__init__(
            map_args=[boundaries, sort_key],
            reduce_args=[sort_key, batch_format],
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
        meta = BlockAccessor.for_block(block).get_metadata(exec_stats=stats.build())
        return out + [meta]

    @staticmethod
    def reduce(
        sort_key: SortKey,
        batch_format: str,
        *mapper_outputs: List[Block],
        partial_reduce: bool = False,
    ) -> Tuple[Block, BlockMetadata]:
        normalized_blocks = TableBlockAccessor.normalize_block_types(
            mapper_outputs,
            target_block_type=ExchangeTaskSpec._derive_target_block_type(batch_format),
        )
        return BlockAccessor.for_block(normalized_blocks[0]).merge_sorted_blocks(
            normalized_blocks, sort_key
        )

    @staticmethod
    def sample_boundaries(
        blocks: List[ObjectRef[Block]],
        sort_key: SortKey,
        num_reducers: int,
        sample_bar: Optional[ProgressBar] = None,
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
        if sample_bar is None:
            sample_bar = ProgressBar(
                SortTaskSpec.SORT_SAMPLE_SUB_PROGRESS_BAR_NAME,
                len(blocks) * n_samples,
                unit="rows",
            )
        # TODO(zhilong): Update sort sample bar before finished.
        samples = sample_bar.fetch_until_complete(sample_results)
        del sample_results
        samples: List[Block] = [s for s in samples if len(s) > 0]
        # The dataset is empty
        if len(samples) == 0:
            return [None] * (num_reducers - 1)

        # Convert samples to a sorted list[tuple[...]] where each tuple represents a
        # sample.
        # TODO: Once we deprecate pandas blocks, we can avoid this conversion and
        # directly sort the samples.
        builder = DelegatingBlockBuilder()
        for sample in samples:
            builder.add_block(sample)
        samples_table = builder.build()
        samples_dict = BlockAccessor.for_block(samples_table).to_numpy(columns=columns)
        # This zip does the transposition from list of column values to list of tuples.
        samples_list = list(zip(*samples_dict.values()))

        def is_na(x):
            # Check if x is None or NaN. Type casting to np.array first to avoid
            # isnan failing on strings and other types.
            if x is None:
                return True
            x = np.asarray(x)
            if np.issubdtype(x.dtype, np.number):
                return np.isnan(x)
            return False

        # To allow multi-directional sort, we utilize Python's stable sort: we
        # sort several times with different directions. We do this in reverse, so
        # that the last key we sort by is the primary sort key passed by the user.
        for i, desc in list(enumerate(sort_key.get_descending()))[::-1]:
            # Sort the list, but Nones should be NULL_SENTINEL to ensure safe sorting.
            samples_list.sort(
                key=lambda sample: NULL_SENTINEL if is_na(sample[i]) else sample[i],
                reverse=desc,
            )

        # Each boundary corresponds to a quantile of the data.
        quantile_indices = [
            int(q * (len(samples_list) - 1))
            for q in np.linspace(0, 1, num_reducers + 1)
        ]
        # Exclude the first and last quantiles because they're 0 and 1.
        return [samples_list[i] for i in quantile_indices[1:-1]]


def _sample_block(block: Block, n_samples: int, sort_key: SortKey) -> Block:
    return BlockAccessor.for_block(block).sample(n_samples, sort_key)
