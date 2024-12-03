import collections
import heapq
import logging
import sys
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

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.tensor_extensions.utils import _is_ndarray_tensor
from ray.data._internal.numpy_support import convert_to_numpy, validate_numpy_batch
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

if TYPE_CHECKING:
    import pandas
    import pyarrow

    from ray.data._internal.planner.exchange.sort_task_spec import SortKey
    from ray.data.aggregate import AggregateFn

T = TypeVar("T")
# Max number of samples used to estimate the Pandas block size.
_PANDAS_SIZE_BYTES_MAX_SAMPLE_COUNT = 50

logger = logging.getLogger(__name__)

_pandas = None


def lazy_import_pandas():
    global _pandas
    if _pandas is None:
        import pandas

        _pandas = pandas
    return _pandas


class PandasRow(TableRow):
    """
    Row of a tabular Dataset backed by a Pandas DataFrame block.
    """

    def __getitem__(self, key: Union[str, List[str]]) -> Any:
        from ray.data.extensions import TensorArrayElement

        pd = lazy_import_pandas()

        def get_item(keys: List[str]) -> Any:
            col = self._row[keys]
            if len(col) == 0:
                return None

            items = col.iloc[0]
            if isinstance(items.iloc[0], TensorArrayElement):
                # Getting an item in a Pandas tensor column may return
                # a TensorArrayElement, which we have to convert to an ndarray.
                return pd.Series(item.to_numpy() for item in items)

            try:
                # Try to interpret this as a numpy-type value.
                # See https://stackoverflow.com/questions/9452775/converting-numpy-dtypes-to-native-python-types.  # noqa: E501
                return pd.Series(item.as_py() for item in items)

            except (AttributeError, ValueError):
                # Fallback to the original form.
                return items

        is_single_item = isinstance(key, str)
        keys = [key] if is_single_item else key

        items = get_item(keys)

        if items is None:
            return None
        elif is_single_item:
            return items.iloc[0]
        else:
            return items

    def __iter__(self) -> Iterator:
        for k in self._row.columns:
            yield k

    def __len__(self):
        return self._row.shape[1]


class PandasBlockBuilder(TableBlockBuilder):
    def __init__(self):
        pandas = lazy_import_pandas()
        super().__init__(pandas.DataFrame)

    @staticmethod
    def _table_from_pydict(columns: Dict[str, List[Any]]) -> "pandas.DataFrame":
        pandas = lazy_import_pandas()

        pd_columns: Dict[str, Any] = {}

        for col_name, col_vals in columns.items():
            np_col_vals = convert_to_numpy(col_vals)

            if col_name == TENSOR_COLUMN_NAME or _is_ndarray_tensor(np_col_vals):
                from ray.data.extensions.tensor_extension import TensorArray

                pd_columns[col_name] = TensorArray(np_col_vals)
            else:
                pd_columns[col_name] = np_col_vals

        return pandas.DataFrame(pd_columns)

    @staticmethod
    def _concat_tables(tables: List["pandas.DataFrame"]) -> "pandas.DataFrame":
        pandas = lazy_import_pandas()
        from ray.air.util.data_batch_conversion import (
            _cast_ndarray_columns_to_tensor_extension,
        )

        if len(tables) > 1:
            df = pandas.concat(tables, ignore_index=True)
            df.reset_index(drop=True, inplace=True)
        else:
            df = tables[0]
        ctx = DataContext.get_current()
        if ctx.enable_tensor_extension_casting:
            df = _cast_ndarray_columns_to_tensor_extension(df)
        return df

    @staticmethod
    def _concat_would_copy() -> bool:
        return True

    @staticmethod
    def _empty_table() -> "pandas.DataFrame":
        pandas = lazy_import_pandas()
        return pandas.DataFrame()

    def block_type(self) -> BlockType:
        return BlockType.PANDAS


# This is to be compatible with pyarrow.lib.schema
# TODO (kfstorm): We need a format-independent way to represent schema.
PandasBlockSchema = collections.namedtuple("PandasBlockSchema", ["names", "types"])


class PandasBlockAccessor(TableBlockAccessor):
    ROW_TYPE = PandasRow

    def __init__(self, table: "pandas.DataFrame"):
        super().__init__(table)

    def column_names(self) -> List[str]:
        return self._table.columns.tolist()

    def append_column(self, name: str, data: Any) -> Block:
        assert name not in self._table.columns

        if any(isinstance(item, np.ndarray) for item in data):
            raise NotImplementedError(
                f"`{self.__class__.__name__}.append_column()` doesn't support "
                "array-like data."
            )

        table = self._table.copy()
        table[name] = data
        return table

    @staticmethod
    def _build_tensor_row(row: PandasRow) -> np.ndarray:
        from ray.data.extensions import TensorArrayElement

        tensor = row[TENSOR_COLUMN_NAME].iloc[0]
        if isinstance(tensor, TensorArrayElement):
            # Getting an item in a Pandas tensor column may return a TensorArrayElement,
            # which we have to convert to an ndarray.
            tensor = tensor.to_numpy()
        return tensor

    def slice(self, start: int, end: int, copy: bool = False) -> "pandas.DataFrame":
        view = self._table[start:end]
        view.reset_index(drop=True, inplace=True)
        if copy:
            view = view.copy(deep=True)
        return view

    def take(self, indices: List[int]) -> "pandas.DataFrame":
        table = self._table.take(indices)
        table.reset_index(drop=True, inplace=True)
        return table

    def select(self, columns: List[str]) -> "pandas.DataFrame":
        if not all(isinstance(col, str) for col in columns):
            raise ValueError(
                "Columns must be a list of column name strings when aggregating on "
                f"Pandas blocks, but got: {columns}."
            )
        return self._table[columns]

    def random_shuffle(self, random_seed: Optional[int]) -> "pandas.DataFrame":
        table = self._table.sample(frac=1, random_state=random_seed)
        table.reset_index(drop=True, inplace=True)
        return table

    def schema(self) -> PandasBlockSchema:
        dtypes = self._table.dtypes
        schema = PandasBlockSchema(
            names=dtypes.index.tolist(), types=dtypes.values.tolist()
        )
        # Column names with non-str types of a pandas DataFrame is not
        # supported by Ray Dataset.
        if any(not isinstance(name, str) for name in schema.names):
            raise ValueError(
                "A Pandas DataFrame with column names of non-str types"
                " is not supported by Ray Dataset. Column names of this"
                f" DataFrame: {schema.names!r}."
            )
        return schema

    def to_pandas(self) -> "pandas.DataFrame":
        from ray.air.util.data_batch_conversion import _cast_tensor_columns_to_ndarrays

        ctx = DataContext.get_current()
        table = self._table
        if ctx.enable_tensor_extension_casting:
            table = _cast_tensor_columns_to_ndarrays(table)
        return table

    def to_numpy(
        self, columns: Optional[Union[str, List[str]]] = None
    ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
        if columns is None:
            columns = self._table.columns.tolist()
            should_be_single_ndarray = False
        elif isinstance(columns, list):
            should_be_single_ndarray = False
        else:
            columns = [columns]
            should_be_single_ndarray = True

        column_names_set = set(self._table.columns)
        for column in columns:
            if column not in column_names_set:
                raise ValueError(
                    f"Cannot find column {column}, available columns: "
                    f"{self._table.columns.tolist()}"
                )

        arrays = []
        for column in columns:
            arrays.append(self._table[column].to_numpy())

        if should_be_single_ndarray:
            arrays = arrays[0]
        else:
            arrays = dict(zip(columns, arrays))
        return arrays

    def to_arrow(self) -> "pyarrow.Table":
        import pyarrow

        # Set `preserve_index=False` so that Arrow doesn't add a '__index_level_0__'
        # column to the resulting table.
        return pyarrow.Table.from_pandas(self._table, preserve_index=False)

    @staticmethod
    def numpy_to_block(
        batch: Union[Dict[str, np.ndarray], Dict[str, list]],
    ) -> "pandas.DataFrame":
        validate_numpy_batch(batch)

        block = PandasBlockBuilder._table_from_pydict(batch)
        return block

    def num_rows(self) -> int:
        return self._table.shape[0]

    def size_bytes(self) -> int:
        from pandas.api.types import is_object_dtype

        from ray.air.util.tensor_extensions.pandas import TensorArray
        from ray.data.extensions import TensorArrayElement, TensorDtype

        pd = lazy_import_pandas()

        def get_deep_size(obj):
            """Calculates the memory size of objects,
            including nested objects using an iterative approach."""
            seen = set()
            total_size = 0
            objects = collections.deque([obj])
            while objects:
                current = objects.pop()

                # Skip interning-eligible immutable objects
                if isinstance(current, (str, bytes, int, float)):
                    size = sys.getsizeof(current)
                    total_size += size
                    continue

                # Check if the object has been seen before
                # i.e. a = np.ndarray([1,2,3]), b = [a,a]
                # The patten above will have only one memory copy
                if id(current) in seen:
                    continue
                seen.add(id(current))

                try:
                    size = sys.getsizeof(current)
                except TypeError:
                    size = 0
                total_size += size

                # Handle specific cases
                if isinstance(current, np.ndarray):
                    total_size += current.nbytes - size  # Avoid double counting
                elif isinstance(current, pd.DataFrame):
                    total_size += (
                        current.memory_usage(index=True, deep=True).sum() - size
                    )
                elif isinstance(current, (list, tuple, set)):
                    objects.extend(current)
                elif isinstance(current, dict):
                    objects.extend(current.keys())
                    objects.extend(current.values())
                elif isinstance(current, TensorArrayElement):
                    objects.extend(current.to_numpy())
            return total_size

        # Get initial memory usage including deep introspection
        memory_usage = self._table.memory_usage(index=True, deep=True)

        # TensorDtype for ray.air.util.tensor_extensions.pandas.TensorDtype
        object_need_check = (TensorDtype,)
        max_sample_count = _PANDAS_SIZE_BYTES_MAX_SAMPLE_COUNT

        # Handle object columns separately
        for column in self._table.columns:
            # Check pandas object dtype and the extension dtype
            if is_object_dtype(self._table[column].dtype) or isinstance(
                self._table[column].dtype, object_need_check
            ):
                total_size = len(self._table[column])

                # Determine the sample size based on max_sample_count
                sample_size = min(total_size, max_sample_count)
                # Following codes can also handel case that sample_size == total_size
                sampled_data = self._table[column].sample(n=sample_size).values

                try:
                    if isinstance(sampled_data, TensorArray) and np.issubdtype(
                        sampled_data[0].numpy_dtype, np.number
                    ):
                        column_memory_sample = sampled_data.nbytes
                    else:
                        vectorized_size_calc = np.vectorize(lambda x: get_deep_size(x))
                        column_memory_sample = np.sum(
                            vectorized_size_calc(sampled_data)
                        )
                    # Scale back to the full column size if we sampled
                    column_memory = column_memory_sample * (total_size / sample_size)
                    memory_usage[column] = int(column_memory)
                except Exception as e:
                    # Handle or log the exception as needed
                    logger.warning(f"Error calculating size for column '{column}': {e}")

        # Sum up total memory usage
        total_memory_usage = memory_usage.sum()

        return int(total_memory_usage)

    def _zip(self, acc: BlockAccessor) -> "pandas.DataFrame":
        r = self.to_pandas().copy(deep=False)
        s = acc.to_pandas()
        for col_name in s.columns:
            col = s[col_name]
            column_names = list(r.columns)
            # Ensure the column names are unique after zip.
            if col_name in column_names:
                i = 1
                new_name = col_name
                while new_name in column_names:
                    new_name = "{}_{}".format(col_name, i)
                    i += 1
                col_name = new_name
            r[col_name] = col
        return r

    @staticmethod
    def builder() -> PandasBlockBuilder:
        return PandasBlockBuilder()

    @staticmethod
    def _empty_table() -> "pandas.DataFrame":
        return PandasBlockBuilder._empty_table()

    def _sample(self, n_samples: int, sort_key: "SortKey") -> "pandas.DataFrame":
        return self._table[sort_key.get_columns()].sample(n_samples, ignore_index=True)

    def _apply_agg(
        self, agg_fn: Callable[["pandas.Series", bool], U], on: str
    ) -> Optional[U]:
        """Helper providing null handling around applying an aggregation to a column."""
        pd = lazy_import_pandas()
        if on is not None and not isinstance(on, str):
            raise ValueError(
                "on must be a string or None when aggregating on Pandas blocks, but "
                f"got: {type(on)}."
            )

        if self.num_rows() == 0:
            return None

        col = self._table[on]
        try:
            val = agg_fn(col)
        except TypeError as e:
            # Converting an all-null column in an Arrow Table to a Pandas DataFrame
            # column will result in an all-None column of object type, which will raise
            # a type error when attempting to do most binary operations. We explicitly
            # check for this type failure here so we can properly propagate a null.
            if np.issubdtype(col.dtype, np.object_) and col.isnull().all():
                return None
            raise e from None
        if pd.isnull(val):
            return None
        return val

    def count(self, on: str) -> Optional[U]:
        return self._apply_agg(lambda col: col.count(), on)

    def sum(self, on: str, ignore_nulls: bool) -> Optional[U]:
        pd = lazy_import_pandas()
        if on is not None and not isinstance(on, str):
            raise ValueError(
                "on must be a string or None when aggregating on Pandas blocks, but "
                f"got: {type(on)}."
            )

        if self.num_rows() == 0:
            return None

        col = self._table[on]
        if col.isnull().all():
            # Short-circuit on an all-null column, returning None. This is required for
            # sum() since it will otherwise return 0 when summing on an all-null column,
            # which is not what we want.
            return None
        val = col.sum(skipna=ignore_nulls)
        if pd.isnull(val):
            return None
        return val

    def min(self, on: str, ignore_nulls: bool) -> Optional[U]:
        return self._apply_agg(lambda col: col.min(skipna=ignore_nulls), on)

    def max(self, on: str, ignore_nulls: bool) -> Optional[U]:
        return self._apply_agg(lambda col: col.max(skipna=ignore_nulls), on)

    def mean(self, on: str, ignore_nulls: bool) -> Optional[U]:
        return self._apply_agg(lambda col: col.mean(skipna=ignore_nulls), on)

    def sum_of_squared_diffs_from_mean(
        self,
        on: str,
        ignore_nulls: bool,
        mean: Optional[U] = None,
    ) -> Optional[U]:
        if mean is None:
            mean = self.mean(on, ignore_nulls)
        return self._apply_agg(
            lambda col: ((col - mean) ** 2).sum(skipna=ignore_nulls),
            on,
        )

    def sort_and_partition(
        self, boundaries: List[T], sort_key: "SortKey"
    ) -> List[Block]:
        if self._table.shape[0] == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling sort_indices() will raise an error.
            return [self._empty_table() for _ in range(len(boundaries) + 1)]

        columns, ascending = sort_key.to_pandas_sort_args()
        table = self._table.sort_values(by=columns, ascending=ascending)
        if len(boundaries) == 0:
            return [table]

        return find_partitions(table, boundaries, sort_key)

    def combine(
        self, sort_key: "SortKey", aggs: Tuple["AggregateFn"]
    ) -> "pandas.DataFrame":
        """Combine rows with the same key into an accumulator.

        This assumes the block is already sorted by key in ascending order.

        Args:
            sort_key: A SortKey object which holds column names/keys.
            If this is ``None``, place all rows in a single group.

            aggs: The aggregations to do.

        Returns:
            A sorted block of [k, v_1, ..., v_n] columns where k is the groupby
            key and v_i is the partially combined accumulator for the ith given
            aggregation.
            If key is None then the k column is omitted.
        """
        keys: List[str] = sort_key.get_columns()
        pd = lazy_import_pandas()

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
                    while np.all(next_row[keys] == next_keys):
                        end += 1
                        try:
                            next_row = next(iter)
                        except StopIteration:
                            next_row = None
                            break
                    if isinstance(next_keys, pd.Series):
                        next_keys = next_keys.values
                    yield next_keys, self.slice(start, end, copy=False)
                    start = end
                except StopIteration:
                    break

        builder = PandasBlockBuilder()
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
    def merge_sorted_blocks(
        blocks: List[Block], sort_key: "SortKey"
    ) -> Tuple["pandas.DataFrame", BlockMetadata]:
        pd = lazy_import_pandas()
        stats = BlockExecStats.builder()
        blocks = [b for b in blocks if b.shape[0] > 0]
        if len(blocks) == 0:
            ret = PandasBlockAccessor._empty_table()
        else:
            # Handle blocks of different types.
            blocks = TableBlockAccessor.normalize_block_types(blocks, "pandas")
            ret = pd.concat(blocks, ignore_index=True)
            columns, ascending = sort_key.to_pandas_sort_args()
            ret = ret.sort_values(by=columns, ascending=ascending)
        return ret, PandasBlockAccessor(ret).get_metadata(exec_stats=stats.build())

    @staticmethod
    def aggregate_combined_blocks(
        blocks: List["pandas.DataFrame"],
        sort_key: "SortKey",
        aggs: Tuple["AggregateFn"],
        finalize: bool,
    ) -> Tuple["pandas.DataFrame", BlockMetadata]:
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
        blocks = TableBlockAccessor.normalize_block_types(blocks, "pandas")

        iter = heapq.merge(
            *[
                PandasBlockAccessor(block).iter_rows(public_row_format=False)
                for block in blocks
            ],
            key=key_fn,
        )
        next_row = None
        builder = PandasBlockBuilder()
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
                                name = PandasBlockAccessor._munge_conflict(
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
        return ret, PandasBlockAccessor(ret).get_metadata(exec_stats=stats.build())

    def block_type(self) -> BlockType:
        return BlockType.PANDAS
