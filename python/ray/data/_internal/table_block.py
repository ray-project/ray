import collections
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

from ray._common.utils import env_integer
from ray.data._internal.arrow_ops.transform_pyarrow import concat, concat_and_sort
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.size_estimator import SizeEstimator
from ray.data._internal.util import find_partition_index
from ray.data.block import (
    AggType,
    Block,
    BlockAccessor,
    BlockColumn,
    BlockColumnAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockMetadataWithSchema,
    BlockType,
    U,
)
from ray.data.context import DEFAULT_TARGET_MAX_BLOCK_SIZE

if TYPE_CHECKING:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey
    from ray.data.aggregate import AggregateFn

T = TypeVar("T")

# The max size of Python tuples to buffer before compacting them into a
# table in the BlockBuilder.
MAX_UNCOMPACTED_SIZE_BYTES = env_integer(
    "RAY_DATA_MAX_UNCOMPACTED_SIZE_BYTES", DEFAULT_TARGET_MAX_BLOCK_SIZE
)


class TableBlockBuilder(BlockBuilder):
    def __init__(self, block_type):
        # The set of uncompacted Python values buffered.
        self._columns = collections.defaultdict(list)
        # The set of compacted tables we have built so far.
        self._tables: List[Any] = []
        # Cursor into tables indicating up to which table we've accumulated table sizes.
        # This is used to defer table size calculation, which can be expensive for e.g.
        # Pandas DataFrames.
        # This cursor points to the first table for which we haven't accumulated a table
        # size.
        self._tables_size_cursor = 0
        # Accumulated table sizes, up to the table in _tables pointed to by
        # _tables_size_cursor.
        self._tables_size_bytes = 0
        # Size estimator for un-compacted table values.
        self._uncompacted_size = SizeEstimator()
        self._num_rows = 0
        self._num_uncompacted_rows = 0
        self._num_compactions = 0
        self._block_type = block_type

    def add(self, item: Union[dict, Mapping]) -> None:
        if hasattr(item, "as_pydict"):
            item = item.as_pydict()
        if not isinstance(item, collections.abc.Mapping):
            raise ValueError(
                "Returned elements of an TableBlock must be of type `dict`, "
                "got {} (type {}).".format(item, type(item))
            )

        # Fill in missing columns with None.
        for column_name in item:
            if column_name not in self._columns:
                self._columns[column_name] = [None] * self._num_uncompacted_rows

        for column_name in self._columns:
            value = item.get(column_name)
            self._columns[column_name].append(value)

        self._num_rows += 1
        self._num_uncompacted_rows += 1
        self._compact_if_needed()
        self._uncompacted_size.add(item)

    def add_block(self, block: Any) -> None:
        if not isinstance(block, self._block_type):
            raise TypeError(
                f"Got a block of type {type(block)}, expected {self._block_type}."
                "If you are mapping a function, ensure it returns an "
                "object with the expected type. Block:\n"
                f"{block}"
            )
        accessor = BlockAccessor.for_block(block)
        self._tables.append(block)
        self._num_rows += accessor.num_rows()

    @staticmethod
    def _table_from_pydict(columns: Dict[str, List[Any]]) -> Block:
        raise NotImplementedError

    @staticmethod
    def _combine_tables(tables: List[Block]) -> Block:
        raise NotImplementedError

    @staticmethod
    def _empty_table() -> Any:
        raise NotImplementedError

    @staticmethod
    def _concat_would_copy() -> bool:
        raise NotImplementedError

    def will_build_yield_copy(self) -> bool:
        if self._columns:
            # Building a table from a dict of list columns always creates a copy.
            return True
        return self._concat_would_copy() and len(self._tables) > 1

    def build(self) -> Block:
        # Preserve insertion order: previously-compacted tables (older) first,
        # then any rows added since the last compaction (newest) last.
        tables = list(self._tables)
        if self._columns:
            tables.append(self._table_from_pydict(self._columns))

        if len(tables) == 0:
            return self._empty_table()
        else:
            return self._combine_tables(tables)

    def num_rows(self) -> int:
        return self._num_rows

    def num_blocks(self) -> int:
        return len(self._tables)

    def get_estimated_memory_usage(self) -> int:
        if self._num_rows == 0:
            return 0
        for table in self._tables[self._tables_size_cursor :]:
            self._tables_size_bytes += BlockAccessor.for_block(table).size_bytes()
        self._tables_size_cursor = len(self._tables)
        return self._tables_size_bytes + self._uncompacted_size.size_bytes()

    def _compact_if_needed(self) -> None:
        assert self._columns
        if self._uncompacted_size.size_bytes() < MAX_UNCOMPACTED_SIZE_BYTES:
            return
        block = self._table_from_pydict(self._columns)
        self.add_block(block)
        self._uncompacted_size = SizeEstimator()
        self._columns.clear()
        self._num_compactions += 1
        self._num_uncompacted_rows = 0


class TableBlockAccessor(BlockAccessor):
    def __init__(self, table: Any):
        self._table = table

    @staticmethod
    def _munge_conflict(name, count):
        return f"{name}_{count + 1}"

    def to_default(self) -> Block:
        # Always promote Arrow blocks to pandas for consistency, since
        # we lazily convert pandas->Arrow internally for efficiency.
        default = self.to_pandas()

        return default

    def to_cudf(self) -> Any:
        """Convert this block to a cudf.DataFrame (requires cudf to be installed)."""
        from ray.data.util.data_batch_conversion import _lazy_import_cudf

        cudf = _lazy_import_cudf()
        if cudf is None:
            raise ValueError(
                "Attempted to convert data to cuDF DataFrame but cuDF "
                "is not installed. Please do `pip install cudf-cu12` to "
                "install cuDF (GPU required)."
            )

        return cudf.DataFrame.from_arrow(self.to_arrow())

    def column_names(self) -> List[str]:
        raise NotImplementedError

    def fill_column(self, name: str, value: Any) -> Block:
        raise NotImplementedError

    def to_block(self) -> Block:
        return self._table

    def _zip(self, acc: BlockAccessor) -> "Block":
        raise NotImplementedError

    def zip(self, other: "Block") -> "Block":
        acc = BlockAccessor.for_block(other)
        if not isinstance(acc, type(self)):
            if isinstance(self, TableBlockAccessor) and isinstance(
                acc, TableBlockAccessor
            ):
                # If block types are different, but still both of TableBlock type, try
                # converting both to default block type before zipping.
                self_norm, other_norm = TableBlockAccessor.normalize_block_types(
                    [self._table, other],
                )
                return BlockAccessor.for_block(self_norm).zip(other_norm)
            else:
                raise ValueError(
                    "Cannot zip {} with block of type {}".format(
                        type(self), type(other)
                    )
                )
        if acc.num_rows() != self.num_rows():
            raise ValueError(
                "Cannot zip self (length {}) with block of length {}".format(
                    self.num_rows(), acc.num_rows()
                )
            )
        return self._zip(acc)

    @staticmethod
    def _empty_table() -> Any:
        raise NotImplementedError

    def _sample(self, n_samples: int, sort_key: "SortKey") -> Any:
        raise NotImplementedError

    def sample(self, n_samples: int, sort_key: "SortKey") -> Any:
        if sort_key is None or callable(sort_key):
            raise NotImplementedError(
                f"Table sort key must be a column name, was: {sort_key}"
            )
        if self.num_rows() == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling table.select() will raise an error.
            return self._empty_table()
        k = min(n_samples, self.num_rows())
        return self._sample(k, sort_key)

    def count(self, on: str, ignore_nulls: bool = False) -> Optional[U]:
        accessor = BlockColumnAccessor.for_column(self._table[on])
        return accessor.count(ignore_nulls=ignore_nulls)

    def sum(self, on: str, ignore_nulls: bool) -> Optional[U]:
        self._validate_column(on)

        accessor = BlockColumnAccessor.for_column(self._table[on])
        return accessor.sum(ignore_nulls=ignore_nulls)

    def min(self, on: str, ignore_nulls: bool) -> Optional[U]:
        self._validate_column(on)

        accessor = BlockColumnAccessor.for_column(self._table[on])
        return accessor.min(ignore_nulls=ignore_nulls)

    def max(self, on: str, ignore_nulls: bool) -> Optional[U]:
        self._validate_column(on)

        accessor = BlockColumnAccessor.for_column(self._table[on])
        return accessor.max(ignore_nulls=ignore_nulls)

    def mean(self, on: str, ignore_nulls: bool) -> Optional[U]:
        self._validate_column(on)

        accessor = BlockColumnAccessor.for_column(self._table[on])
        return accessor.mean(ignore_nulls=ignore_nulls)

    def sum_of_squared_diffs_from_mean(
        self,
        on: str,
        ignore_nulls: bool,
        mean: Optional[U] = None,
    ) -> Optional[U]:
        self._validate_column(on)

        accessor = BlockColumnAccessor.for_column(self._table[on])
        return accessor.sum_of_squared_diffs_from_mean(ignore_nulls=ignore_nulls)

    def _validate_column(self, col: str):
        if col is None:
            raise ValueError(f"Provided `on` value has to be non-null (got '{col}')")
        elif col not in self.column_names():
            raise ValueError(
                f"Referencing column '{col}' not present in the schema: {self.schema()}"
            )

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
                    # NOTE: For compatibility with existing semantic we're unwrapping
                    #       cases when there's just a single column being grouped by
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
    ) -> Tuple[Block, "BlockMetadataWithSchema"]:
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

        from ray.data._internal.arrow_block import ArrowBlockAccessor

        stats = BlockExecStats.builder()

        # Filter out empty blocks
        blocks = [b for b in blocks if BlockAccessor.for_block(b).num_rows() > 0]

        if len(blocks) == 0:
            meta = BlockMetadata(
                num_rows=0,
                size_bytes=0,
                exec_stats=None,
                input_files=None,
            )
            return cls._empty_table(), BlockMetadataWithSchema.from_metadata(
                metadata=meta
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

        return final_block, BlockMetadataWithSchema.from_block(
            final_block, block_exec_stats=stats.build()
        )

    @staticmethod
    def _combine_column(agg: "AggregateFn", accumulator_col: BlockColumn) -> AggType:
        from ray.data.aggregate import (
            VectorizedAggregateFnV2,
            _fold_accumulator_column,
        )

        if isinstance(agg, VectorizedAggregateFnV2):
            return agg._combine_column(accumulator_col)

        return _fold_accumulator_column(agg, accumulator_col)

    def _find_partitions_sorted(
        self,
        boundaries: List[Tuple[Any]],
        sort_key: "SortKey",
    ):
        partitions = []

        # For each boundary value, count the number of items that are less
        # than it. Since the block is sorted, these counts partition the items
        # such that boundaries[i] <= x < boundaries[i + 1] for each x in
        # partition[i]. If `descending` is true, `boundaries` would also be
        # in descending order and we only need to count the number of items
        # *greater than* the boundary value instead.
        bounds = [
            find_partition_index(self._table, boundary, sort_key)
            for boundary in boundaries
        ]

        last_idx = 0
        for idx in bounds:
            partitions.append(self._table[last_idx:idx])
            last_idx = idx
        partitions.append(self._table[last_idx:])
        return partitions

    @classmethod
    def normalize_block_types(
        cls,
        blocks: List[Block],
        target_block_type: Optional[BlockType] = None,
    ) -> List[Block]:
        """Normalize input blocks to the specified `normalize_type`. If the blocks
        are already all of the same type, returns original blocks.

        Args:
            blocks: A list of TableBlocks to be normalized.
            target_block_type: The type to normalize the blocks to. If None,
               Ray Data chooses a type to minimize the amount of data conversions.

        Returns:
            A list of blocks of the same type.
        """
        seen_types: Dict[BlockType, int] = collections.defaultdict(int)

        for block in blocks:
            block_accessor = BlockAccessor.for_block(block)
            if not isinstance(block_accessor, TableBlockAccessor):
                raise ValueError(
                    "Block type normalization is only supported for TableBlock, "
                    f"but received block of type: {type(block)}."
                )

            seen_types[block_accessor.block_type()] += 1

        # If there's just 1 block-type and it's matching target-type, short-circuit
        if len(seen_types) == 1 and (
            target_block_type is None or [target_block_type] == list(seen_types.keys())
        ):
            return blocks

        # Pick the most prevalent block-type
        if target_block_type is None:
            _, target_block_type = sorted(
                seen_types.items(),
                key=lambda x: x[1],
                reverse=True,
            )[0]

        results = [
            cls.try_convert_block_type(block, target_block_type) for block in blocks
        ]

        if any(not isinstance(block, type(results[0])) for block in results):
            raise ValueError(
                "Expected all blocks to be of the same type after normalization, but "
                f"got different types: {[type(b) for b in results]}. "
                "Try using blocks of the same type to avoid the issue "
                "with block normalization."
            )
        return results

    @classmethod
    def try_convert_block_type(cls, block: Block, block_type: BlockType):
        if block_type == BlockType.ARROW:
            return BlockAccessor.for_block(block).to_arrow()
        elif block_type == BlockType.PANDAS:
            return BlockAccessor.for_block(block).to_pandas()
        else:
            return BlockAccessor.for_block(block).to_default()

    def hstack(self, other_block: Block) -> Block:
        """Combine this table with another table horizontally (column-wise).
        This will append the columns.

        Args:
            other_block: The table to hstack side-by-side with.

        Returns:
            A new table with columns from both tables combined.
        """
        raise NotImplementedError


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
