import collections
import heapq
import random
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from ray._private.utils import _get_pyarrow_version
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.data._internal.arrow_ops import transform_polars, transform_pyarrow
from ray.data._internal.numpy_support import (
    convert_udf_returns_to_numpy,
    is_valid_udf_return,
)
from ray.data._internal.table_block import TableBlockAccessor, TableBlockBuilder
from ray.data._internal.util import _truncated_repr, find_partitions
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    KeyType,
    U,
)
from ray.data.context import DataContext
from ray.data.row import TableRow

try:
    import pyarrow
except ImportError:
    pyarrow = None


if TYPE_CHECKING:
    import pandas

    from ray.data._internal.sort import SortKey
    from ray.data.aggregate import AggregateFn


T = TypeVar("T")


# We offload some transformations to polars for performance.
def get_sort_transform(context: DataContext) -> Callable:
    if context.use_polars:
        return transform_polars.sort
    else:
        return transform_pyarrow.sort


def get_concat_and_sort_transform(context: DataContext) -> Callable:
    if context.use_polars:
        return transform_polars.concat_and_sort
    else:
        return transform_pyarrow.concat_and_sort


class ArrowRow(TableRow):
    """
    Row of a tabular Dataset backed by a Arrow Table block.
    """

    def __getitem__(self, key: str) -> Any:
        from ray.data.extensions.tensor_extension import (
            ArrowTensorType,
            ArrowVariableShapedTensorType,
        )

        schema = self._row.schema
        if isinstance(
            schema.field(key).type,
            (ArrowTensorType, ArrowVariableShapedTensorType),
        ):
            # Build a tensor row.
            return ArrowBlockAccessor._build_tensor_row(self._row, col_name=key)

        col = self._row[key]
        if len(col) == 0:
            return None
        item = col[0]
        try:
            # Try to interpret this as a pyarrow.Scalar value.
            return item.as_py()
        except AttributeError:
            # Assume that this row is an element of an extension array, and
            # that it is bypassing pyarrow's scalar model for Arrow < 8.0.0.
            return item

    def __iter__(self) -> Iterator:
        for k in self._row.column_names:
            yield k

    def __len__(self):
        return self._row.num_columns


class ArrowBlockBuilder(TableBlockBuilder):
    def __init__(self):
        if pyarrow is None:
            raise ImportError("Run `pip install pyarrow` for Arrow support")
        super().__init__((pyarrow.Table, bytes))

    @staticmethod
    def _table_from_pydict(columns: Dict[str, List[Any]]) -> Block:
        for col_name, col in columns.items():
            if col_name == TENSOR_COLUMN_NAME or isinstance(
                next(iter(col), None), np.ndarray
            ):
                from ray.data.extensions.tensor_extension import ArrowTensorArray

                columns[col_name] = ArrowTensorArray.from_numpy(col)
        return pyarrow.Table.from_pydict(columns)

    @staticmethod
    def _concat_tables(tables: List[Block]) -> Block:
        return transform_pyarrow.concat(tables)

    @staticmethod
    def _concat_would_copy() -> bool:
        return False

    @staticmethod
    def _empty_table() -> "pyarrow.Table":
        return pyarrow.Table.from_pydict({})


class ArrowBlockAccessor(TableBlockAccessor):
    ROW_TYPE = ArrowRow

    def __init__(self, table: "pyarrow.Table"):
        if pyarrow is None:
            raise ImportError("Run `pip install pyarrow` for Arrow support")
        super().__init__(table)

    def column_names(self) -> List[str]:
        return self._table.column_names

    @classmethod
    def from_bytes(cls, data: bytes) -> "ArrowBlockAccessor":
        reader = pyarrow.ipc.open_stream(data)
        return cls(reader.read_all())

    @staticmethod
    def numpy_to_block(
        batch: Union[np.ndarray, Dict[str, np.ndarray], Dict[str, list]],
    ) -> "pyarrow.Table":
        import pyarrow as pa

        from ray.data.extensions.tensor_extension import ArrowTensorArray

        if isinstance(batch, np.ndarray):
            batch = {TENSOR_COLUMN_NAME: batch}
        elif not isinstance(batch, collections.abc.Mapping) or any(
            not is_valid_udf_return(col) for col in batch.values()
        ):
            raise ValueError(
                "Batch must be an ndarray or dictionary of ndarrays when converting "
                f"a numpy batch to a block, got: {type(batch)} "
                f"({_truncated_repr(batch)})"
            )
        new_batch = {}
        for col_name, col in batch.items():
            # Coerce to np.ndarray format if possible.
            col = convert_udf_returns_to_numpy(col)
            # Use Arrow's native *List types for 1-dimensional ndarrays.
            if col.dtype.type is np.object_ or col.ndim > 1:
                col = ArrowTensorArray.from_numpy(col)
            new_batch[col_name] = col
        return pa.Table.from_pydict(new_batch)

    @staticmethod
    def _build_tensor_row(
        row: ArrowRow, col_name: str = TENSOR_COLUMN_NAME
    ) -> np.ndarray:
        from pkg_resources._vendor.packaging.version import parse as parse_version

        element = row[col_name][0]
        # TODO(Clark): Reduce this to np.asarray(element) once we only support Arrow
        # 9.0.0+.
        pyarrow_version = _get_pyarrow_version()
        if pyarrow_version is not None:
            pyarrow_version = parse_version(pyarrow_version)
        if pyarrow_version is None or pyarrow_version >= parse_version("8.0.0"):
            assert isinstance(element, pyarrow.ExtensionScalar)
            if pyarrow_version is None or pyarrow_version >= parse_version("9.0.0"):
                # For Arrow 9.0.0+, accessing an element in a chunked tensor array
                # produces an ArrowTensorScalar, which we convert to an ndarray using
                # .as_py().
                element = element.as_py()
            else:
                # For Arrow 8.*, accessing an element in a chunked tensor array produces
                # an ExtensionScalar, which we convert to an ndarray using our custom
                # method.
                element = element.type._extension_scalar_to_ndarray(element)
        # For Arrow < 8.0.0, accessing an element in a chunked tensor array produces an
        # ndarray, which we return directly.
        assert isinstance(element, np.ndarray), type(element)
        return element

    def slice(self, start: int, end: int, copy: bool = False) -> "pyarrow.Table":
        view = self._table.slice(start, end - start)
        if copy:
            view = _copy_table(view)
        return view

    def random_shuffle(self, random_seed: Optional[int]) -> "pyarrow.Table":
        random = np.random.RandomState(random_seed)
        return self.take(random.permutation(self.num_rows()))

    def schema(self) -> "pyarrow.lib.Schema":
        return self._table.schema

    def to_pandas(self) -> "pandas.DataFrame":
        from ray.air.util.data_batch_conversion import _cast_tensor_columns_to_ndarrays

        df = self._table.to_pandas()
        ctx = DataContext.get_current()
        if ctx.enable_tensor_extension_casting:
            df = _cast_tensor_columns_to_ndarrays(df)
        return df

    def to_numpy(
        self, columns: Optional[Union[str, List[str]]] = None
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        from ray.air.util.transform_pyarrow import (
            _concatenate_extension_column,
            _is_column_extension_type,
        )

        if columns is None:
            columns = self._table.column_names
            should_be_single_ndarray = False
        elif isinstance(columns, list):
            should_be_single_ndarray = False
        else:
            columns = [columns]
            should_be_single_ndarray = True

        for column in columns:
            if column not in self._table.column_names:
                raise ValueError(
                    f"Cannot find column {column}, available columns: "
                    f"{self._table.column_names}"
                )

        arrays = []
        for column in columns:
            array = self._table[column]
            if _is_column_extension_type(array):
                array = _concatenate_extension_column(array)
            elif array.num_chunks == 0:
                array = pyarrow.array([], type=array.type)
            else:
                array = array.combine_chunks()
            arrays.append(array.to_numpy(zero_copy_only=False))

        if should_be_single_ndarray:
            assert len(columns) == 1
            arrays = arrays[0]
        else:
            arrays = dict(zip(columns, arrays))
        return arrays

    def to_arrow(self) -> "pyarrow.Table":
        return self._table

    def num_rows(self) -> int:
        # Arrow may represent an empty table via an N > 0 row, 0-column table, e.g. when
        # slicing an empty table, so we return 0 if num_columns == 0.
        return self._table.num_rows if self._table.num_columns > 0 else 0

    def size_bytes(self) -> int:
        return self._table.nbytes

    def _zip(self, acc: BlockAccessor) -> "Block":
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
    def builder() -> ArrowBlockBuilder:
        return ArrowBlockBuilder()

    @staticmethod
    def _empty_table() -> "pyarrow.Table":
        return ArrowBlockBuilder._empty_table()

    def take(
        self,
        indices: Union[List[int], "pyarrow.Array", "pyarrow.ChunkedArray"],
    ) -> "pyarrow.Table":
        """Select rows from the underlying table.

        This method is an alternative to pyarrow.Table.take(), which breaks for
        extension arrays.
        """
        return transform_pyarrow.take_table(self._table, indices)

    def select(self, columns: List[str]) -> "pyarrow.Table":
        if not all(isinstance(col, str) for col in columns):
            raise ValueError(
                "Columns must be a list of column name strings when aggregating on "
                f"Arrow blocks, but got: {columns}."
            )
        return self._table.select(columns)

    def _sample(self, n_samples: int, sort_key: "SortKey") -> "pyarrow.Table":
        indices = random.sample(range(self._table.num_rows), n_samples)
        table = self._table.select(sort_key.get_columns())
        return transform_pyarrow.take_table(table, indices)

    def count(self, on: str) -> Optional[U]:
        """Count the number of non-null values in the provided column."""
        import pyarrow.compute as pac

        if not isinstance(on, str):
            raise ValueError(
                "on must be a string when aggregating on Arrow blocks, but got:"
                f"{type(on)}."
            )

        if self.num_rows() == 0:
            return None

        col = self._table[on]
        return pac.count(col).as_py()

    def _apply_arrow_compute(
        self, compute_fn: Callable, on: str, ignore_nulls: bool
    ) -> Optional[U]:
        """Helper providing null handling around applying an aggregation to a column."""
        import pyarrow as pa

        if not isinstance(on, str):
            raise ValueError(
                "on must be a string when aggregating on Arrow blocks, but got:"
                f"{type(on)}."
            )

        if self.num_rows() == 0:
            return None

        col = self._table[on]
        if pa.types.is_null(col.type):
            return None
        else:
            return compute_fn(col, skip_nulls=ignore_nulls).as_py()

    def sum(self, on: str, ignore_nulls: bool) -> Optional[U]:
        import pyarrow.compute as pac

        return self._apply_arrow_compute(pac.sum, on, ignore_nulls)

    def min(self, on: str, ignore_nulls: bool) -> Optional[U]:
        import pyarrow.compute as pac

        return self._apply_arrow_compute(pac.min, on, ignore_nulls)

    def max(self, on: str, ignore_nulls: bool) -> Optional[U]:
        import pyarrow.compute as pac

        return self._apply_arrow_compute(pac.max, on, ignore_nulls)

    def mean(self, on: str, ignore_nulls: bool) -> Optional[U]:
        import pyarrow.compute as pac

        return self._apply_arrow_compute(pac.mean, on, ignore_nulls)

    def sum_of_squared_diffs_from_mean(
        self,
        on: str,
        ignore_nulls: bool,
        mean: Optional[U] = None,
    ) -> Optional[U]:
        import pyarrow.compute as pac

        if mean is None:
            # If precomputed mean not given, we compute it ourselves.
            mean = self.mean(on, ignore_nulls)
            if mean is None:
                return None
        return self._apply_arrow_compute(
            lambda col, skip_nulls: pac.sum(
                pac.power(pac.subtract(col, mean), 2),
                skip_nulls=skip_nulls,
            ),
            on,
            ignore_nulls,
        )

    def sort_and_partition(
        self, boundaries: List[T], sort_key: "SortKey"
    ) -> List["Block"]:
        if self._table.num_rows == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling sort_indices() will raise an error.
            return [self._empty_table() for _ in range(len(boundaries) + 1)]

        context = DataContext.get_current()
        sort = get_sort_transform(context)

        table = sort(self._table, sort_key)
        if len(boundaries) == 0:
            return [table]

        return find_partitions(table, boundaries, sort_key)

    def combine(self, key: str, aggs: Tuple["AggregateFn"]) -> Block:
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
        if key is not None and not isinstance(key, str):
            raise ValueError(
                "key must be a string or None when aggregating on Arrow blocks, but "
                f"got: {type(key)}."
            )

        def iter_groups() -> Iterator[Tuple[KeyType, Block]]:
            """Creates an iterator over zero-copy group views."""
            if key is None:
                # Global aggregation consists of a single "group", so we short-circuit.
                yield None, self.to_block()
                return

            start = end = 0
            iter = self.iter_rows(public_row_format=False)
            next_row = None
            while True:
                try:
                    if next_row is None:
                        next_row = next(iter)
                    next_key = next_row[key]
                    while next_row[key] == next_key:
                        end += 1
                        try:
                            next_row = next(iter)
                        except StopIteration:
                            next_row = None
                            break
                    yield next_key, self.slice(start, end)
                    start = end
                except StopIteration:
                    break

        builder = ArrowBlockBuilder()
        for group_key, group_view in iter_groups():
            # Aggregate.
            accumulators = [agg.init(group_key) for agg in aggs]
            for i in range(len(aggs)):
                accumulators[i] = aggs[i].accumulate_block(accumulators[i], group_view)

            # Build the row.
            row = {}
            if key is not None:
                row[key] = group_key

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

    @staticmethod
    def _munge_conflict(name, count):
        return f"{name}_{count+1}"

    @staticmethod
    def merge_sorted_blocks(
        blocks: List[Block], sort_key: "SortKey"
    ) -> Tuple[Block, BlockMetadata]:
        stats = BlockExecStats.builder()
        blocks = [b for b in blocks if b.num_rows > 0]
        if len(blocks) == 0:
            ret = ArrowBlockAccessor._empty_table()
        else:
            concat_and_sort = get_concat_and_sort_transform(DataContext.get_current())
            ret = concat_and_sort(blocks, sort_key)
        return ret, ArrowBlockAccessor(ret).get_metadata(None, exec_stats=stats.build())

    @staticmethod
    def aggregate_combined_blocks(
        blocks: List[Block],
        key: str,
        aggs: Tuple["AggregateFn"],
        finalize: bool,
    ) -> Tuple[Block, BlockMetadata]:
        """Aggregate sorted, partially combined blocks with the same key range.

        This assumes blocks are already sorted by key in ascending order,
        so we can do merge sort to get all the rows with the same key.

        Args:
            blocks: A list of partially combined and sorted blocks.
            key: The column name of key or None for global aggregation.
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

        stats = BlockExecStats.builder()
        key_fn = (
            (lambda r: r[r._row.schema.names[0]]) if key is not None else (lambda r: 0)
        )

        iter = heapq.merge(
            *[
                ArrowBlockAccessor(block).iter_rows(public_row_format=False)
                for block in blocks
            ],
            key=key_fn,
        )
        next_row = None
        builder = ArrowBlockBuilder()
        while True:
            try:
                if next_row is None:
                    next_row = next(iter)
                next_key = key_fn(next_row)
                next_key_name = (
                    next_row._row.schema.names[0] if key is not None else None
                )

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
                if key is not None:
                    row[next_key_name] = next_key

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
        return ret, ArrowBlockAccessor(ret).get_metadata(None, exec_stats=stats.build())


def _copy_table(table: "pyarrow.Table") -> "pyarrow.Table":
    """Copy the provided Arrow table."""
    return transform_pyarrow.combine_chunks(table)
