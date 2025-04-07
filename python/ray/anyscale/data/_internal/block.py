import collections
import logging
from typing import Tuple, List, Sequence, Dict, Iterator, TYPE_CHECKING, Any

import numpy as np

from ray._private.arrow_utils import get_pyarrow_version
from ray.anyscale.data._internal.util.numpy import find_insertion_index
from ray.anyscale.data.aggregate_vectorized import (
    MIN_PYARROW_VERSION_VECTORIZED_AGGREGATIONS,
)
from ray.data._internal.arrow_ops.transform_pyarrow import (
    concat_and_sort,
    concat,
)
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.aggregate import AggregateFn
from ray.data.block import (
    BlockAccessor,
    Block,
    BlockMetadata,
    BlockExecStats,
    BlockType,
    KeyType,
    BlockColumn,
    AggType,
    BlockColumnAccessor,
)


if TYPE_CHECKING:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey


logger = logging.getLogger(__file__)


class OptimizedTableBlockMixin(TableBlockAccessor):
    """Mixin extending ``TableBlockAccessor`` providing optimized
    implementations for some common operations like

        - Aggregations
        - Computing boundaries b/w groups in sorted block
        - etc
    """

    def _aggregate(self, sort_key: "SortKey", aggs: Tuple["AggregateFn"]) -> Block:
        """Applies provided aggregations to groups of rows with the same key.

        This assumes the block is already sorted by key in ascending order.

        Args:
            sort_key: A column name or list of column names.
            If this is ``None``, place all rows in a single group.

            aggs: The aggregations to do.

        Returns:
            A sorted block of [k, v_1, ..., v_n] columns where k is the groupby
            key and v_i is the partially combined accumulator for the ith given
            aggregation.
            If key is None then the k column is omitted.
        """

        # NOTE: For Arrow versions < 14.0, that don't support type promotions
        #       we have to fallback to existing (OSS) implementation
        if get_pyarrow_version() < MIN_PYARROW_VERSION_VECTORIZED_AGGREGATIONS:
            logger.warning(
                f"Falling back to default (non-vectorized) aggregation "
                f"implementation (Arrow version={get_pyarrow_version()})"
            )

            return super(OptimizedTableBlockMixin, self)._aggregate(sort_key, aggs)

        if self.num_rows() == 0:
            return self._empty_table()

        # Resolve target aggregation column names (to avoid conflicts)
        resolved_agg_col_names: List[str] = _resolve_aggregated_column_names(aggs)
        keys: List[str] = sort_key.get_columns()

        builder = self.builder()

        # TODO add multi-threading support

        for group_key_values, group_block in self._iter_groups_sorted(sort_key):
            # Step 1: Initialize accumulators for this group
            accumulators = [
                agg.init(
                    # NOTE: For compatibility with existing semantic we're unwrapping cases
                    #       when there's just a single column being grouped by
                    group_key_values[0]
                    if len(group_key_values) == 1
                    else group_key_values
                )
                for agg in aggs
            ]

            # Step 2: Apply aggregations to provided group's block
            for i in range(len(aggs)):
                accumulators[i] = aggs[i].accumulate_block(accumulators[i], group_block)

            # Step 3: Compose resulting row from
            #   - Grouped by column's values
            #   - Current state of accumulators
            row = dict(zip(keys, group_key_values)) if keys else {}

            for i, accumulator in enumerate(accumulators):
                agg_col_name = resolved_agg_col_names[i]
                row[agg_col_name] = accumulator

            builder.add(row)

        # TODO convert to Arrow to avoid during combination (protocol
        #      relies on blocks being Arrow)
        return builder.build()

    @classmethod
    def _combine_aggregated_blocks(
        cls,
        blocks: List[Block],
        sort_key: "SortKey",
        aggs: Tuple["AggregateFn"],
        finalize: bool = True,
    ) -> Tuple[Block, BlockMetadata]:
        """Combine previously aggregated blocks.

        This assumes blocks are already sorted by key in ascending order,
        so we can do merge sort to get all the rows with the same key.

        Args:
            blocks: A list of partially combined and sorted blocks.
            sort_key: The column name of key or None for global aggregation.
            aggs: The aggregations to do.
            finalize: Whether to finalize the aggregation. This is used as an
                optimization for cases where we repeatedly combine partially
                aggregated groups.

        Returns:
            A block of [k, v_1, ..., v_n] columns and its metadata where k is
            the groupby key and v_i is the corresponding aggregation result for
            the ith given aggregation.
            If key is None then the k column is omitted.
        """

        # NOTE: For Arrow versions < 14.0, that don't support type promotions
        #       we have to fallback to existing (OSS) implementation
        if get_pyarrow_version() < MIN_PYARROW_VERSION_VECTORIZED_AGGREGATIONS:
            logger.warning(
                f"Falling back to default (non-vectorized) aggregation "
                f"implementation (Arrow version={get_pyarrow_version()})"
            )

            return TableBlockAccessor._combine_aggregated_blocks(
                blocks, sort_key, aggs, finalize
            )

        from ray.data._internal.arrow_block import ArrowBlockAccessor

        stats = BlockExecStats.builder()

        # Filter out empty blocks
        blocks = [b for b in blocks if BlockAccessor.for_block(b).num_rows() > 0]

        if len(blocks) == 0:
            return cls._empty_table(), BlockMetadata(
                num_rows=0,
                size_bytes=0,
                exec_stats=None,
                schema=None,
                input_files=None,
            )

        # Normalize blocks to make sure these are of the Arrow type
        blocks = cls.normalize_block_types(blocks, target_block_type=BlockType.ARROW)

        # Combine input blocks, sort resulting block (if needed)
        #
        # NOTE: In case of global aggregations (ie w/o actual grouping)
        #       there's no need to sort resulting block
        if sort_key.get_columns():
            combined = concat_and_sort(blocks, sort_key, promote_types=True)
        else:
            combined = concat(blocks, promote_types=True)

        block_accessor = ArrowBlockAccessor(combined)
        builder = block_accessor.builder()

        # Resolve aggregation names as resulting column names (collisions)
        resolved_agg_col_names: List[str] = _resolve_aggregated_column_names(aggs)

        keys: List[str] = sort_key.get_columns()

        for group_key_vals, grouped_acc_block in block_accessor._iter_groups_sorted(
            sort_key
        ):
            # Append the keys to the final aggregated row
            row = dict(zip(keys, group_key_vals)) if keys else {}

            # Combine partially aggregated values
            for i, agg in enumerate(aggs):
                agg_col_name = resolved_agg_col_names[i]

                # Combine partially aggregated values (current values of the
                # corresponding aggregation column)
                agg_col = grouped_acc_block[agg_col_name]
                combined_agg_result = cls._combine_column(agg, agg_col)

                if finalize:
                    final_agg_result = agg.finalize(combined_agg_result)
                else:
                    final_agg_result = combined_agg_result

                row[agg_col_name] = final_agg_result

            builder.add(row)

        final_block = builder.build()

        return final_block, BlockAccessor.for_block(final_block).get_metadata(
            exec_stats=stats.build()
        )

    def _iter_groups_sorted(
        self, sort_key: "SortKey"
    ) -> Iterator[Tuple[Sequence[KeyType], Block]]:
        """
        NOTE: THIS METHOD ASSUMES THE BLOCK BEING SORTED

        Creates an iterator over (zero-copy) blocks of rows grouped by
        provided key(s).
        """

        key_col_names: List[str] = sort_key.get_columns()

        if not key_col_names:
            # Global aggregation consists of a single "group", so we short-circuit.
            yield tuple(), self.to_block()
            return

        boundaries = self._get_group_boundaries_sorted(key_col_names)

        for start, end in zip(boundaries[:-1], boundaries[1:]):
            # Fetch tuple of key values from the first row
            row = self._get_row(start, copy=False)

            yield row[key_col_names], self.slice(start, end, copy=False)

    @staticmethod
    def _combine_column(agg: "AggregateFn", accumulator_col: BlockColumn) -> AggType:
        from ray.anyscale.data.aggregate_vectorized import VectorizedAggregateFnV2

        if isinstance(agg, VectorizedAggregateFnV2):
            return agg._combine_column(accumulator_col)

        return _combine_column_legacy(agg, accumulator_col)

    def _find_partitions_sorted(
        self,
        boundaries: List[Tuple[Any]],
        sort_key: "SortKey",
    ):
        partitions = []

        columns = sort_key.get_columns()
        descending = sort_key.get_descending()

        key_columns = [
            BlockColumnAccessor.for_column(self._table[col]).to_numpy()
            for col in columns
        ]

        # To obtain partition indices from boundaries we employ following algorithm:
        #
        #   1. For every boundary value we determine insertion index into the
        #      list of key column values (ie, for boundary value ``b`` we determine
        #      an index i, such that ``key_column[i] <= b < key_column[i+1]``, thus
        #      determining the boundary of 2 partitions established by b).
        #
        #   2. Subsequently, list of such insertion indexes is traversed to derive
        #      partitions as ``table[insertion_index[i], insertion_index[i+1]``
        #
        insertion_indices = np.arange(len(boundaries))

        for idx, boundary in enumerate(boundaries):
            # Avoid repeating insertion index search for duplicated boundaries
            #
            # NOTE: Boundaries currently are not de-duplicated and hence we have
            #       to skip repeating insertion point searches here.
            if idx > 0 and boundary == boundaries[idx - 1]:
                insertion_indices[idx] = insertion_indices[idx - 1]
            else:
                insertion_indices[idx] = find_insertion_index(
                    key_columns,
                    boundary,
                    descending,
                    # NOTE: Search for next insertion index could be started off the
                    #       last one, rather than 0
                    _start_from_idx=(0 if idx == 0 else insertion_indices[idx - 1]),
                )

        last_idx = 0
        for idx in insertion_indices:
            partitions.append(self._table[last_idx:idx])
            last_idx = idx

        partitions.append(self._table[last_idx:])

        return partitions


def _combine_column_legacy(agg: "AggregateFn", accumulator_col: BlockColumn) -> AggType:
    if len(accumulator_col) == 0:
        return None

    accumulators = BlockColumnAccessor.for_column(accumulator_col).to_pylist()

    cur_acc = accumulators[0]
    for v in accumulators[1:]:
        cur_acc = agg.merge(cur_acc, v)

    return cur_acc


def _resolve_aggregated_column_names(aggs: Sequence["AggregateFn"]) -> List[str]:
    """Resolves aggregation column names to be unique (in case of collisions)"""

    name_counts: Dict[str, int] = collections.defaultdict(int)

    resolved_agg_names: List[str] = []

    for agg in aggs:
        name = agg.name
        # Check for conflicts with existing aggregation
        # name.
        if name in name_counts:
            name = TableBlockAccessor._munge_conflict(name, name_counts[name])

        name_counts[name] += 1
        resolved_agg_names.append(name)

    return resolved_agg_names
