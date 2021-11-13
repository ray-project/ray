import collections
import random
import heapq
from typing import Iterator, List, Union, Tuple, Any, TypeVar, Optional, \
    TYPE_CHECKING

import numpy as np

try:
    import pyarrow
except ImportError:
    pyarrow = None

from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.impl.block_builder import BlockBuilder
from ray.data.impl.simple_block import SimpleBlockBuilder
from ray.data.aggregate import AggregateFn
from ray.data.impl.size_estimator import SizeEstimator

if TYPE_CHECKING:
    import pandas

T = TypeVar("T")

# An Arrow block can be sorted by a list of (column, asc/desc) pairs,
# e.g. [("column1", "ascending"), ("column2", "descending")]
SortKeyT = List[Tuple[str, str]]
GroupKeyT = Union[None, str]

# The max size of Python tuples to buffer before compacting them into an Arrow
# table in the BlockBuilder.
MAX_UNCOMPACTED_SIZE_BYTES = 50 * 1024 * 1024


class ArrowRow:
    def __init__(self, row: "pyarrow.Table"):
        self._row = row

    def as_pydict(self) -> dict:
        return {k: v[0] for k, v in self._row.to_pydict().items()}

    def keys(self) -> Iterator[str]:
        return self.as_pydict().keys()

    def values(self) -> Iterator[Any]:
        return self.as_pydict().values()

    def items(self) -> Iterator[Tuple[str, Any]]:
        return self.as_pydict().items()

    def __getitem__(self, key: str) -> Any:
        col = self._row[key]
        if len(col) == 0:
            return None
        item = col[0]
        try:
            # Try to interpret this as a pyarrow.Scalar value.
            return item.as_py()
        except AttributeError:
            # Assume that this row is an element of an extension array, and
            # that it is bypassing pyarrow's scalar model.
            return item

    def __eq__(self, other: Any) -> bool:
        return self.as_pydict() == other

    def __str__(self):
        return str(self.as_pydict())

    def __repr__(self):
        return str(self)

    def __len__(self):
        return self._row.num_columns


class DelegatingArrowBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._builder = None

    def add(self, item: Any) -> None:
        if self._builder is None:
            if isinstance(item, dict):
                try:
                    check = ArrowBlockBuilder()
                    check.add(item)
                    check.build()
                    self._builder = ArrowBlockBuilder()
                except (TypeError, pyarrow.lib.ArrowInvalid):
                    self._builder = SimpleBlockBuilder()
            else:
                self._builder = SimpleBlockBuilder()
        self._builder.add(item)

    def add_block(self, block: Block) -> None:
        if self._builder is None:
            self._builder = BlockAccessor.for_block(block).builder()
        self._builder.add_block(block)

    def build(self) -> Block:
        if self._builder is None:
            self._builder = ArrowBlockBuilder()
        return self._builder.build()

    def num_rows(self) -> int:
        return self._builder.num_rows() if self._builder is not None else 0

    def get_estimated_memory_usage(self) -> int:
        if self._builder is None:
            return 0
        return self._builder.get_estimated_memory_usage()


class ArrowBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        if pyarrow is None:
            raise ImportError("Run `pip install pyarrow` for Arrow support")
        # The set of uncompacted Python values buffered.
        self._columns = collections.defaultdict(list)
        # The set of compacted tables we have built so far.
        self._tables: List["pyarrow.Table"] = []
        self._tables_nbytes = 0
        # Size estimator for un-compacted table values.
        self._uncompacted_size = SizeEstimator()
        self._num_rows = 0
        self._num_compactions = 0

    def add(self, item: Union[dict, ArrowRow]) -> None:
        if isinstance(item, ArrowRow):
            item = item.as_pydict()
        if not isinstance(item, dict):
            raise ValueError(
                "Returned elements of an ArrowBlock must be of type `dict`, "
                "got {} (type {}).".format(item, type(item)))
        for key, value in item.items():
            self._columns[key].append(value)
        self._num_rows += 1
        self._compact_if_needed()
        self._uncompacted_size.add(item)

    def add_block(self, block: "pyarrow.Table") -> None:
        assert isinstance(block, pyarrow.Table), block
        self._tables.append(block)
        self._tables_nbytes += block.nbytes
        self._num_rows += block.num_rows

    def build(self) -> Block:
        if self._columns:
            tables = [pyarrow.Table.from_pydict(self._columns)]
        else:
            tables = []
        tables.extend(self._tables)
        if len(tables) > 1:
            return pyarrow.concat_tables(tables, promote=True)
        elif len(tables) > 0:
            return tables[0]
        else:
            return pyarrow.Table.from_pydict({})

    def num_rows(self) -> int:
        return self._num_rows

    def get_estimated_memory_usage(self) -> int:
        if self._num_rows == 0:
            return 0
        return self._tables_nbytes + self._uncompacted_size.size_bytes()

    def _compact_if_needed(self) -> None:
        assert self._columns
        if self._uncompacted_size.size_bytes() < MAX_UNCOMPACTED_SIZE_BYTES:
            return
        block = pyarrow.Table.from_pydict(self._columns)
        self._tables.append(block)
        self._tables_nbytes += block.nbytes
        self._uncompacted_size = SizeEstimator()
        self._columns.clear()
        self._num_compactions += 1


class ArrowBlockAccessor(BlockAccessor):
    def __init__(self, table: "pyarrow.Table"):
        if pyarrow is None:
            raise ImportError("Run `pip install pyarrow` for Arrow support")
        self._table = table

    @classmethod
    def from_bytes(cls, data: bytes):
        reader = pyarrow.ipc.open_stream(data)
        return cls(reader.read_all())

    def iter_rows(self) -> Iterator[ArrowRow]:
        outer = self

        class Iter:
            def __init__(self):
                self._cur = -1

            def __iter__(self):
                return self

            def __next__(self):
                self._cur += 1
                if self._cur < outer._table.num_rows:
                    row = ArrowRow(outer._table.slice(self._cur, 1))
                    return row
                raise StopIteration

        return Iter()

    def slice(self, start: int, end: int, copy: bool) -> "pyarrow.Table":
        view = self._table.slice(start, end - start)
        if copy:
            view = _copy_table(view)
        return view

    def random_shuffle(self, random_seed: Optional[int]) -> List[T]:
        random = np.random.RandomState(random_seed)
        return self._table.take(random.permutation(self.num_rows()))

    def schema(self) -> "pyarrow.lib.Schema":
        return self._table.schema

    def to_pandas(self) -> "pandas.DataFrame":
        return self._table.to_pandas()

    def to_numpy(self, column: str = None) -> np.ndarray:
        if not column:
            raise ValueError(
                "`column` must be specified when calling .to_numpy() "
                "on Arrow blocks.")
        if column not in self._table.column_names:
            raise ValueError(
                "Cannot find column {}, available columns: {}".format(
                    column, self._table.column_names))
        array = self._table[column]
        if array.num_chunks > 1:
            # TODO(ekl) combine fails since we can't concat ArrowTensorType?
            array = array.combine_chunks()
        assert array.num_chunks == 1, array
        return self._table[column].chunk(0).to_numpy()

    def to_arrow(self) -> "pyarrow.Table":
        return self._table

    def num_rows(self) -> int:
        return self._table.num_rows

    def size_bytes(self) -> int:
        return self._table.nbytes

    def zip(self, other: "Block[T]") -> "Block[T]":
        acc = BlockAccessor.for_block(other)
        if not isinstance(acc, ArrowBlockAccessor):
            raise ValueError("Cannot zip {} with block of type {}".format(
                type(self), type(other)))
        if acc.num_rows() != self.num_rows():
            raise ValueError(
                "Cannot zip self (length {}) with block of length {}".format(
                    self.num_rows(), acc.num_rows()))
        r = self.to_arrow()
        s = acc.to_arrow()
        for col_name in s.column_names:
            col = s.column(col_name)
            # Ensure the column names are unique after zip.
            if col_name in r.column_names:
                i = 1
                new_name = col_name
                while new_name in r.column_names:
                    new_name = "{}_{}".format(col_name, i)
                    i += 1
                col_name = new_name
            r = r.append_column(col_name, col)
        return r

    @staticmethod
    def builder() -> ArrowBlockBuilder[T]:
        return ArrowBlockBuilder()

    def sample(self, n_samples: int, key: SortKeyT) -> "pyarrow.Table":
        if key is None or callable(key):
            raise NotImplementedError(
                "Arrow sort key must be a column name, was: {}".format(key))
        if self._table.num_rows == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling table.select() will raise an error.
            return pyarrow.Table.from_pydict({})
        k = min(n_samples, self._table.num_rows)
        indices = random.sample(range(self._table.num_rows), k)
        return self._table.select([k[0] for k in key]).take(indices)

    def sort_and_partition(self, boundaries: List[T], key: SortKeyT,
                           descending: bool) -> List["Block[T]"]:
        if len(key) > 1:
            raise NotImplementedError(
                "sorting by multiple columns is not supported yet")

        if self._table.num_rows == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling sort_indices() will raise an error.
            return [
                pyarrow.Table.from_pydict({})
                for _ in range(len(boundaries) + 1)
            ]

        import pyarrow.compute as pac

        indices = pac.sort_indices(self._table, sort_keys=key)
        table = self._table.take(indices)
        if len(boundaries) == 0:
            return [table]

        # For each boundary value, count the number of items that are less
        # than it. Since the block is sorted, these counts partition the items
        # such that boundaries[i] <= x < boundaries[i + 1] for each x in
        # partition[i]. If `descending` is true, `boundaries` would also be
        # in descending order and we only need to count the number of items
        # *greater than* the boundary value instead.
        col, _ = key[0]
        comp_fn = pac.greater if descending else pac.less

        # TODO(ekl) this is O(n^2) but in practice it's much faster than the
        # O(n) algorithm, could be optimized.
        boundary_indices = [
            pac.sum(comp_fn(table[col], b)).as_py() for b in boundaries
        ]
        ### Compute the boundary indices in O(n) time via scan.  # noqa
        # boundary_indices = []
        # remaining = boundaries.copy()
        # values = table[col]
        # for i, x in enumerate(values):
        #     while remaining and not comp_fn(x, remaining[0]).as_py():
        #         remaining.pop(0)
        #         boundary_indices.append(i)
        # for _ in remaining:
        #     boundary_indices.append(len(values))

        ret = []
        prev_i = 0
        for i in boundary_indices:
            # Slices need to be copied to avoid including the base table
            # during serialization.
            ret.append(_copy_table(table.slice(prev_i, i - prev_i)))
            prev_i = i
        ret.append(_copy_table(table.slice(prev_i)))
        return ret

    def combine(self, key: GroupKeyT,
                aggs: Tuple[AggregateFn]) -> Block[ArrowRow]:
        """Combine rows with the same key into an accumulator.

        This assumes the block is already sorted by key in ascending order.

        Args:
            key: The column name of key or None for global aggregation.
            aggs: The aggregations to do.

        Returns:
            A sorted block of [k, v_1, ..., v_n] columns where k is the groupby
            key and v_i is the partially combined accumulator for the ith given
            aggregation.
            If key is None then the k column is omitted.
        """
        # TODO(jjyao) This can be implemented natively in Arrow
        key_fn = (lambda r: r[key]) if key is not None else (lambda r: None)
        iter = self.iter_rows()
        next_row = None
        builder = ArrowBlockBuilder()
        while True:
            try:
                if next_row is None:
                    next_row = next(iter)
                next_key = key_fn(next_row)

                def gen():
                    nonlocal iter
                    nonlocal next_row
                    while key_fn(next_row) == next_key:
                        yield next_row
                        try:
                            next_row = next(iter)
                        except StopIteration:
                            next_row = None
                            break

                # Accumulate.
                accumulators = [agg.init(next_key) for agg in aggs]
                for r in gen():
                    for i in range(len(aggs)):
                        accumulators[i] = aggs[i].accumulate(
                            accumulators[i], r)

                # Build the row.
                row = {}
                if key is not None:
                    row[key] = next_key

                count = collections.defaultdict(int)
                for agg, accumulator in zip(aggs, accumulators):
                    name = agg.name
                    # Check for conflicts with existing aggregation name.
                    if count[name] > 0:
                        name = self._munge_conflict(name, count[name])
                    count[name] += 1
                    row[name] = accumulator

                builder.add(row)
            except StopIteration:
                break
        return builder.build()

    @staticmethod
    def _munge_conflict(name, count):
        return f"{name}_{count+1}"

    @staticmethod
    def merge_sorted_blocks(
            blocks: List[Block[T]], key: SortKeyT,
            _descending: bool) -> Tuple[Block[T], BlockMetadata]:
        blocks = [b for b in blocks if b.num_rows > 0]
        if len(blocks) == 0:
            ret = pyarrow.Table.from_pydict({})
        else:
            ret = pyarrow.concat_tables(blocks, promote=True)
            indices = pyarrow.compute.sort_indices(ret, sort_keys=key)
            ret = ret.take(indices)
        return ret, ArrowBlockAccessor(ret).get_metadata(None)

    @staticmethod
    def aggregate_combined_blocks(
            blocks: List[Block[ArrowRow]], key: GroupKeyT,
            aggs: Tuple[AggregateFn]) -> Tuple[Block[ArrowRow], BlockMetadata]:
        """Aggregate sorted, partially combined blocks with the same key range.

        This assumes blocks are already sorted by key in ascending order,
        so we can do merge sort to get all the rows with the same key.

        Args:
            blocks: A list of partially combined and sorted blocks.
            key: The column name of key or None for global aggregation.
            aggs: The aggregations to do.

        Returns:
            A block of [k, v_1, ..., v_n] columns and its metadata where k is
            the groupby key and v_i is the corresponding aggregation result for
            the ith given aggregation.
            If key is None then the k column is omitted.
        """

        key_fn = (lambda r: r[r._row.schema.names[0]]
                  ) if key is not None else (lambda r: 0)

        iter = heapq.merge(
            *[ArrowBlockAccessor(block).iter_rows() for block in blocks],
            key=key_fn)
        next_row = None
        builder = ArrowBlockBuilder()
        while True:
            try:
                if next_row is None:
                    next_row = next(iter)
                next_key = key_fn(next_row)
                next_key_name = next_row._row.schema.names[
                    0] if key is not None else None

                def gen():
                    nonlocal iter
                    nonlocal next_row
                    while key_fn(next_row) == next_key:
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
                                name = ArrowBlockAccessor._munge_conflict(
                                    name, count[name])
                            count[name] += 1
                            resolved_agg_names[i] = name
                            accumulators[i] = r[name]
                        first = False
                    else:
                        for i in range(len(aggs)):
                            accumulators[i] = aggs[i].merge(
                                accumulators[i], r[resolved_agg_names[i]])
                # Build the row.
                row = {}
                if key is not None:
                    row[next_key_name] = next_key

                for agg, agg_name, accumulator in zip(aggs, resolved_agg_names,
                                                      accumulators):
                    row[agg_name] = agg.finalize(accumulator)

                builder.add(row)
            except StopIteration:
                break

        ret = builder.build()
        return ret, ArrowBlockAccessor(ret).get_metadata(None)


def _copy_table(table: "pyarrow.Table") -> "pyarrow.Table":
    """Copy the provided Arrow table."""
    import pyarrow as pa

    # Copy the table by copying each column and constructing a new table with
    # the same schema.
    cols = table.columns
    new_cols = []
    for col in cols:
        if col.num_chunks > 0 and isinstance(col.chunk(0), pa.ExtensionArray):
            # If an extension array, we copy the underlying storage arrays.
            chunk = col.chunk(0)
            arr = type(chunk).from_storage(
                chunk.type, pa.concat_arrays([c.storage for c in col.chunks]))
        else:
            # Otherwise, we copy the top-level chunk arrays.
            arr = col.combine_chunks()
        new_cols.append(arr)
    return pa.Table.from_arrays(new_cols, schema=table.schema)
