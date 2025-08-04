import collections
import heapq
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from ray._private.ray_constants import env_integer
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.row import TableRow
from ray.data._internal.size_estimator import SizeEstimator
from ray.data._internal.util import (
    NULL_SENTINEL,
    find_partition_index,
    is_nan,
    keys_equal,
)
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockColumnAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockType,
    KeyType,
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

    def add(self, item: Union[dict, TableRow, np.ndarray]) -> None:
        if isinstance(item, TableRow):
            item = item.as_pydict()
        elif isinstance(item, np.ndarray):
            item = {TENSOR_COLUMN_NAME: item}
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
    def _concat_tables(tables: List[Block]) -> Block:
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
        if self._columns:
            tables = [self._table_from_pydict(self._columns)]
        else:
            tables = []

        tables.extend(self._tables)

        if len(tables) > 0:
            return self._concat_tables(tables)
        else:
            return self._empty_table()

    def num_rows(self) -> int:
        return self._num_rows

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
    ROW_TYPE: TableRow = TableRow

    def __init__(self, table: Any):
        self._table = table

    def _get_row(self, index: int, copy: bool = False) -> Union[TableRow, np.ndarray]:
        base_row = self.slice(index, index + 1, copy=copy)
        row = self.ROW_TYPE(base_row)
        return row

    @staticmethod
    def _munge_conflict(name, count):
        return f"{name}_{count + 1}"

    @staticmethod
    def _build_tensor_row(row: TableRow) -> np.ndarray:
        raise NotImplementedError

    def to_default(self) -> Block:
        # Always promote Arrow blocks to pandas for consistency, since
        # we lazily convert pandas->Arrow internally for efficiency.
        default = self.to_pandas()

        return default

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
        keys: List[str] = sort_key.get_columns()

        def iter_groups() -> Iterator[Tuple[Sequence[KeyType], Block]]:
            """Creates an iterator over zero-copy group views."""
            if not keys:
                # Global aggregation consists of a single "group", so we short-circuit.
                yield tuple(), self.to_block()
                return

            start = end = 0
            iter = self.iter_rows(public_row_format=False)
            next_row = None
            while True:
                try:
                    if next_row is None:
                        next_row = next(iter)
                    next_keys = next_row[keys]
                    while keys_equal(next_row[keys], next_keys):
                        end += 1
                        try:
                            next_row = next(iter)
                        except StopIteration:
                            next_row = None
                            break
                    yield next_keys, self.slice(start, end)
                    start = end
                except StopIteration:
                    break

        builder = self.builder()
        for group_keys, group_view in iter_groups():
            # Aggregate.
            init_vals = group_keys
            if len(group_keys) == 1:
                init_vals = group_keys[0]

            accumulators = [agg.init(init_vals) for agg in aggs]
            for i in range(len(aggs)):
                accessor = BlockAccessor.for_block(group_view)
                # Skip empty blocks
                if accessor.num_rows() > 0:
                    accumulators[i] = aggs[i].accumulate_block(
                        accumulators[i], group_view
                    )

            # Build the row.
            row = {}
            if keys:
                for k, gk in zip(keys, group_keys):
                    row[k] = gk

            count = collections.defaultdict(int)
            for agg, accumulator in zip(aggs, accumulators):
                name = agg.name
                # Check for conflicts with existing aggregation name.
                if count[name] > 0:
                    name = self._munge_conflict(name, count[name])
                count[name] += 1
                row[name] = accumulator

            builder.add(row)

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

        # Handle blocks of different types.
        blocks = TableBlockAccessor.normalize_block_types(blocks)

        stats = BlockExecStats.builder()
        keys = sort_key.get_columns()

        def _key_fn(r):
            if keys:
                return tuple(r[keys])
            else:
                return (0,)

        # Replace `None`s and `np.nan` with NULL_SENTINEL to make sure
        # we can order the elements (both of these are incomparable)
        def safe_key_fn(r):
            values = _key_fn(r)
            return tuple(
                [NULL_SENTINEL if v is None or is_nan(v) else v for v in values]
            )

        iter = heapq.merge(
            *[
                BlockAccessor.for_block(block).iter_rows(public_row_format=False)
                for block in blocks
            ],
            key=safe_key_fn,
        )

        next_row = None
        builder = BlockAccessor.for_block(blocks[0]).builder()

        while True:
            try:
                if next_row is None:
                    next_row = next(iter)

                next_keys = _key_fn(next_row)
                next_key_columns = keys

                def gen():
                    nonlocal iter
                    nonlocal next_row
                    while keys_equal(_key_fn(next_row), next_keys):
                        yield next_row
                        try:
                            next_row = next(iter)
                        except StopIteration:
                            next_row = None
                            break

                # Merge.
                first = True
                accumulators = [None] * len(aggs)
                resolved_agg_names = [None] * len(aggs)
                for r in gen():
                    if first:
                        count = collections.defaultdict(int)
                        for i in range(len(aggs)):
                            name = aggs[i].name
                            # Check for conflicts with existing aggregation
                            # name.
                            if count[name] > 0:
                                name = TableBlockAccessor._munge_conflict(
                                    name, count[name]
                                )
                            count[name] += 1
                            resolved_agg_names[i] = name
                            accumulators[i] = r[name]
                        first = False
                    else:
                        for i in range(len(aggs)):
                            accumulators[i] = aggs[i].merge(
                                accumulators[i], r[resolved_agg_names[i]]
                            )
                # Build the row.
                row = {}
                if keys:
                    for col_name, next_key in zip(next_key_columns, next_keys):
                        row[col_name] = next_key

                for agg, agg_name, accumulator in zip(
                    aggs, resolved_agg_names, accumulators
                ):
                    if finalize:
                        row[agg_name] = agg.finalize(accumulator)
                    else:
                        row[agg_name] = accumulator

                builder.add(row)
            except StopIteration:
                break

        ret = builder.build()
        return ret, BlockMetadataWithSchema.from_block(ret, stats=stats.build())

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
