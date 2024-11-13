import collections
import heapq
import logging
import random
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
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

from ray._private.utils import _get_pyarrow_version
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.tensor_extensions.arrow import (
    ArrowConversionError,
    convert_list_to_pyarrow_array,
    pyarrow_table_from_pydict,
)
from ray.data._internal.arrow_ops import transform_polars, transform_pyarrow
from ray.data._internal.numpy_support import (
    convert_udf_returns_to_numpy,
    validate_numpy_batch,
)
from ray.data._internal.row import TableRow
from ray.data._internal.table_block import TableBlockAccessor, TableBlockBuilder
from ray.data._internal.util import find_partitions
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockType,
    KeyType,
    U,
)
from ray.data.context import DataContext
from ray.util.debug import log_once

try:
    import pyarrow
except ImportError:
    pyarrow = None


if TYPE_CHECKING:
    import pandas

    from ray.data._internal.planner.exchange.sort_task_spec import SortKey
    from ray.data.aggregate import AggregateFn


T = TypeVar("T")
logger = logging.getLogger(__name__)

ARROW_OBJECT_FIXABLE_ERRORS = (
    pyarrow.lib.ArrowTypeError,
    pyarrow.lib.ArrowNotImplementedError,
    pyarrow.lib.ArrowInvalid,
)


def is_object_fixable_error(e: ArrowConversionError) -> bool:
    """Returns whether this error can be fixed by using an ArrowPythonObjectArray"""
    return isinstance(e.__cause__, ARROW_OBJECT_FIXABLE_ERRORS)


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

    def __getitem__(self, key: Union[str, List[str]]) -> Any:
        from ray.data.extensions import get_arrow_extension_tensor_types

        tensor_arrow_extension_types = get_arrow_extension_tensor_types()

        def get_item(keys: List[str]) -> Any:
            schema = self._row.schema
            if isinstance(schema.field(keys[0]).type, tensor_arrow_extension_types):
                # Build a tensor row.
                return tuple(
                    [
                        ArrowBlockAccessor._build_tensor_row(self._row, col_name=key)
                        for key in keys
                    ]
                )

            table = self._row.select(keys)
            if len(table) == 0:
                return None

            items = [col[0] for col in table.columns]
            try:
                # Try to interpret this as a pyarrow.Scalar value.
                return tuple([item.as_py() for item in items])

            except AttributeError:
                # Assume that this row is an element of an extension array, and
                # that it is bypassing pyarrow's scalar model for Arrow < 8.0.0.
                return items

        is_single_item = isinstance(key, str)
        keys = [key] if is_single_item else key

        items = get_item(keys)

        if items is None:
            return None
        elif is_single_item:
            return items[0]
        else:
            return items

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
            try:
                if col_name == TENSOR_COLUMN_NAME or isinstance(
                    next(iter(col), None), np.ndarray
                ):
                    from ray.data.extensions.tensor_extension import ArrowTensorArray

                    columns[col_name] = ArrowTensorArray.from_numpy(col, col_name)
                else:
                    columns[col_name] = convert_list_to_pyarrow_array(col, columns)
            except ArrowConversionError as e:
                from ray.data.extensions.object_extension import (
                    ArrowPythonObjectArray,
                    object_extension_type_allowed,
                )

                if object_extension_type_allowed() and is_object_fixable_error(e):
                    columns[col_name] = ArrowPythonObjectArray.from_objects(col)
                else:
                    raise
        return pyarrow_table_from_pydict(columns)

    @staticmethod
    def _concat_tables(tables: List[Block]) -> Block:
        return transform_pyarrow.concat(tables)

    @staticmethod
    def _concat_would_copy() -> bool:
        return False

    @staticmethod
    def _empty_table() -> "pyarrow.Table":
        return pyarrow_table_from_pydict({})

    def block_type(self) -> BlockType:
        return BlockType.ARROW


class ArrowBlockAccessor(TableBlockAccessor):
    ROW_TYPE = ArrowRow

    def __init__(self, table: "pyarrow.Table"):
        if pyarrow is None:
            raise ImportError("Run `pip install pyarrow` for Arrow support")
        super().__init__(table)

    def column_names(self) -> List[str]:
        return self._table.column_names

    def append_column(self, name: str, data: Any) -> Block:
        assert name not in self._table.column_names

        if any(isinstance(item, np.ndarray) for item in data):
            raise NotImplementedError(
                f"`{self.__class__.__name__}.append_column()` doesn't support "
                "array-like data."
            )

        return self._table.append_column(name, [data])

    @classmethod
    def from_bytes(cls, data: bytes) -> "ArrowBlockAccessor":
        reader = pyarrow.ipc.open_stream(data)
        return cls(reader.read_all())

    @staticmethod
    def numpy_to_block(
        batch: Union[Dict[str, np.ndarray], Dict[str, list]],
    ) -> "pyarrow.Table":
        from ray.data.extensions.object_extension import (
            ArrowPythonObjectArray,
            object_extension_type_allowed,
        )
        from ray.data.extensions.tensor_extension import ArrowTensorArray

        validate_numpy_batch(batch)

        new_batch = {}
        for col_name, col in batch.items():
            # Coerce to np.ndarray format if possible.
            col = convert_udf_returns_to_numpy(col)
            # Use Arrow's native *List types for 1-dimensional ndarrays.
            if col.dtype.type is np.object_ or col.ndim > 1:
                try:
                    col = ArrowTensorArray.from_numpy(col, col_name)
                except ArrowConversionError as e:
                    if object_extension_type_allowed() and is_object_fixable_error(e):
                        if log_once(f"arrow_object_pickle_{col_name}"):
                            logger.debug(
                                f"Failed to interpret {col_name} as "
                                "multi-dimensional arrays. It will be pickled."
                            )
                        col = ArrowPythonObjectArray.from_objects(col)
                    else:
                        raise

            new_batch[col_name] = col
        return pyarrow_table_from_pydict(new_batch)

    @staticmethod
    def _build_tensor_row(
        row: ArrowRow, col_name: str = TENSOR_COLUMN_NAME
    ) -> np.ndarray:
        from packaging.version import parse as parse_version

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
        # TODO(swang): Creating this np.array index can add a lot of memory
        # pressure when there are a large number of small rows. Investigate
        # random shuffling in place to reduce memory pressure.
        # See https://github.com/ray-project/ray/issues/42146.
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

        column_names_set = set(self._table.column_names)
        for column in columns:
            if column not in column_names_set:
                raise ValueError(
                    f"Cannot find column {column}, available columns: "
                    f"{column_names_set}"
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

    def combine(self, sort_key: "SortKey", aggs: Tuple["AggregateFn"]) -> Block:
        """Combine rows with the same key into an accumulator.

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
                    while next_row[keys] == next_keys:
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

        builder = ArrowBlockBuilder()
        for group_keys, group_view in iter_groups():
            # Aggregate.
            init_vals = group_keys
            if len(group_keys) == 1:
                init_vals = group_keys[0]

            accumulators = [agg.init(init_vals) for agg in aggs]
            for i in range(len(aggs)):
                accumulators[i] = aggs[i].accumulate_block(accumulators[i], group_view)

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
            # Handle blocks of different types.
            blocks = TableBlockAccessor.normalize_block_types(blocks, "arrow")
            concat_and_sort = get_concat_and_sort_transform(DataContext.get_current())
            ret = concat_and_sort(blocks, sort_key)
        return ret, ArrowBlockAccessor(ret).get_metadata(exec_stats=stats.build())

    @staticmethod
    def aggregate_combined_blocks(
        blocks: List[Block],
        sort_key: "SortKey",
        aggs: Tuple["AggregateFn"],
        finalize: bool,
    ) -> Tuple[Block, BlockMetadata]:
        """Aggregate sorted, partially combined blocks with the same key range.

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

        stats = BlockExecStats.builder()
        keys = sort_key.get_columns()

        def key_fn(r):
            if keys:
                return tuple(r[keys])
            else:
                return (0,)

        # Handle blocks of different types.
        blocks = TableBlockAccessor.normalize_block_types(blocks, "arrow")

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
                next_keys = key_fn(next_row)
                next_key_columns = keys

                def gen():
                    nonlocal iter
                    nonlocal next_row
                    while key_fn(next_row) == next_keys:
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
        return ret, ArrowBlockAccessor(ret).get_metadata(exec_stats=stats.build())

    def block_type(self) -> BlockType:
        return BlockType.ARROW


def _copy_table(table: "pyarrow.Table") -> "pyarrow.Table":
    """Copy the provided Arrow table."""
    return transform_pyarrow.combine_chunks(table)
