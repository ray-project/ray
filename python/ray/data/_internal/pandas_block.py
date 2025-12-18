import collections
import logging
import sys
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
)

import numpy as np
import pandas as pd
from pandas.api.types import is_object_dtype, is_scalar, is_string_dtype

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.tensor_extensions.utils import _should_convert_to_tensor
from ray.data._internal.numpy_support import convert_to_numpy
from ray.data._internal.row import row_repr, row_repr_pretty, row_str
from ray.data._internal.table_block import TableBlockAccessor, TableBlockBuilder
from ray.data._internal.util import is_null
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockColumn,
    BlockColumnAccessor,
    BlockExecStats,
    BlockType,
    U,
)
from ray.data.context import DataContext
from ray.data.expressions import Expr

if TYPE_CHECKING:
    import pandas
    import pyarrow

    from ray.data._internal.planner.exchange.sort_task_spec import SortKey
    from ray.data.block import BlockMetadataWithSchema

T = TypeVar("T")
# Max number of samples used to estimate the Pandas block size.
_PANDAS_SIZE_BYTES_MAX_SAMPLE_COUNT = 200

logger = logging.getLogger(__name__)

_pandas = None


def lazy_import_pandas():
    global _pandas
    if _pandas is None:
        import pandas

        _pandas = pandas
    return _pandas


class PandasRow(Mapping):
    """
    Row of a tabular Dataset backed by a Pandas DataFrame block.
    """

    def __init__(self, row: Any):
        self._row = row

    def __getitem__(self, key: Union[str, List[str]]) -> Any:
        from ray.data.extensions import TensorArrayElement

        def get_item(keys: List[str]) -> Any:
            col = self._row[keys]
            if len(col) == 0:
                return None

            items = col.iloc[0]
            if isinstance(items.iloc[0], TensorArrayElement):
                # Getting an item in a Pandas tensor column may return
                # a TensorArrayElement, which we have to convert to an ndarray.
                return tuple(item.to_numpy() for item in items)

            try:
                # Try to interpret this as a numpy-type value.
                # See https://stackoverflow.com/questions/9452775/converting-numpy-dtypes-to-native-python-types.  # noqa: E501
                return tuple(item for item in items)

            except (AttributeError, ValueError) as e:
                logger.warning(f"Failed to convert {items} to a tuple", exc_info=e)

                # Fallback to the original form.
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
        for k in self._row.columns:
            yield k

    def __len__(self):
        return self._row.shape[1]

    def as_pydict(self) -> Dict[str, Any]:
        pydict: Dict[str, Any] = {}
        for key, value in self.items():
            # Convert NA to None for consistency across block formats. `pd.isna`
            # returns True for both NA and NaN, but since we want to preserve NaN
            # values, we check for identity instead.
            if is_scalar(value) and value is pd.NA:
                pydict[key] = None
            else:
                pydict[key] = value

        return pydict

    def __str__(self):
        return row_str(self)

    def __repr__(self):
        return row_repr(self)

    def _repr_pretty_(self, p, cycle):
        return row_repr_pretty(self, p, cycle)


class PandasBlockColumnAccessor(BlockColumnAccessor):
    def __init__(self, col: "pandas.Series"):
        super().__init__(col)

    def count(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        return self._column.count() if ignore_nulls else len(self._column)

    def sum(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        # NOTE: Pandas ``Series`` isn't able to properly handle the case with
        #       all-null/NaN values in the column, hence we have to handle it here
        if self._is_all_null():
            return None

        # NOTE: We pass `min_count=1` to workaround quirky Pandas behavior,
        #       where (by default) when min_count=0 it will return 0.0 for
        #       all-null/NaN series
        return self._column.sum(skipna=ignore_nulls, min_count=1)

    def min(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        # NOTE: Pandas ``Series`` isn't able to properly handle the case with
        #       all-null/NaN values in the column, hence we have to handle it here
        if self._is_all_null():
            return None

        return self._column.min(skipna=ignore_nulls)

    def max(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        # NOTE: Pandas ``Series`` isn't able to properly handle the case with
        #       all-null/NaN values in the column, hence we have to handle it here
        if self._is_all_null():
            return None

        return self._column.max(skipna=ignore_nulls)

    def mean(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        # NOTE: We manually implement mean here to keep implementation consistent
        #       with behavior of ``sum`` method returning null if the series
        #       contains exclusively null values
        sum_ = self.sum(ignore_nulls=ignore_nulls)

        return (
            sum_ / self.count(ignore_nulls=ignore_nulls) if not is_null(sum_) else sum_
        )

    def quantile(
        self, *, q: float, ignore_nulls: bool, as_py: bool = True
    ) -> Optional[U]:
        return self._column.quantile(q=q)

    def value_counts(self) -> Optional[Dict[str, List]]:
        value_counts = self._column.value_counts()
        if len(value_counts) == 0:
            return None
        return {
            "values": value_counts.index.tolist(),
            "counts": value_counts.values.tolist(),
        }

    def hash(self) -> BlockColumn:

        from ray.air.util.tensor_extensions.pandas import TensorArrayElement

        first_non_null = next((x for x in self._column if x is not None), None)
        if isinstance(first_non_null, TensorArrayElement):
            self._column = self._column.apply(lambda x: x.to_numpy())

        import polars as pl

        df = pl.from_pandas(self._column.to_frame())
        hashes = df.hash_rows().cast(pl.Int64, wrap_numerical=True)
        return hashes.to_pandas()

    def unique(self) -> BlockColumn:

        pd = lazy_import_pandas()

        try:
            if self.is_composed_of_lists():
                # NOTE: Pandas uses hashing internally to compute unique values,
                #       and hence we have to convert lists into tuples to make
                #       them hashable
                col = self._column.map(lambda l: l if l is None else tuple(l))
            else:
                col = self._column

            return pd.Series(col.unique())
        except ValueError as e:
            if "buffer source array is read-only" in str(e):
                # NOTE: Pandas < 2.0 somehow tries to update the underlying buffer
                #       when computing unique values hence failing
                return pd.Series(self._column.copy().unique())
            else:
                raise

    def flatten(self) -> BlockColumn:
        from ray.air.util.tensor_extensions.pandas import TensorArrayElement

        first_non_null = next((x for x in self._column if x is not None), None)
        if not isinstance(first_non_null, TensorArrayElement):
            column = self._column
        else:
            column = self._column.apply(
                lambda x: x.to_numpy() if isinstance(x, TensorArrayElement) else x
            )

        # NOTE: `Series.explode` explodes empty lists into NaNs, necessitating
        #       filtering out of empty lists first
        if self.is_composed_of_lists():
            mask = column.apply(lambda x: x is not None and len(x) > 0)
            column = column[mask]

        return column.explode(ignore_index=True)

    def dropna(self) -> BlockColumn:
        return self._column.dropna()

    def sum_of_squared_diffs_from_mean(
        self,
        ignore_nulls: bool,
        mean: Optional[U] = None,
        as_py: bool = True,
    ) -> Optional[U]:
        if mean is None:
            mean = self.mean(ignore_nulls=ignore_nulls)

        if is_null(mean):
            return mean

        return ((self._column - mean) ** 2).sum(skipna=ignore_nulls)

    def to_pylist(self) -> List[Any]:
        return self._column.to_list()

    def to_numpy(self, zero_copy_only: bool = False) -> np.ndarray:
        """NOTE: Unlike Arrow, specifying `zero_copy_only=True` isn't a guarantee
        that no copy will be made
        """

        return self._column.to_numpy(copy=not zero_copy_only)

    def _as_arrow_compatible(self) -> Union[List[Any], "pyarrow.Array"]:
        return self.to_pylist()

    def _is_all_null(self):
        return not self._column.notna().any()

    def is_composed_of_lists(self) -> bool:
        from ray.air.util.tensor_extensions.pandas import TensorArrayElement

        types = (list, np.ndarray, TensorArrayElement)
        first_non_null = next((x for x in self._column if x is not None), None)
        return isinstance(first_non_null, types)


class PandasBlockBuilder(TableBlockBuilder):
    def __init__(self):
        pandas = lazy_import_pandas()
        super().__init__(pandas.DataFrame)

    @staticmethod
    def _table_from_pydict(columns: Dict[str, List[Any]]) -> "pandas.DataFrame":
        from ray.data.extensions.tensor_extension import TensorArray

        pandas = lazy_import_pandas()

        return pandas.DataFrame(
            {
                column_name: (
                    TensorArray(convert_to_numpy(column_values))
                    if len(column_values) > 0
                    and _should_convert_to_tensor(column_values, column_name)
                    else column_values
                )
                for column_name, column_values in columns.items()
            }
        )

    @staticmethod
    def _combine_tables(tables: List["pandas.DataFrame"]) -> "pandas.DataFrame":
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

    def _get_row(self, index: int) -> PandasRow:
        base_row = self.slice(index, index + 1, copy=False)
        return PandasRow(base_row)

    def column_names(self) -> List[str]:
        return self._table.columns.tolist()

    def fill_column(self, name: str, value: Any) -> Block:
        # Check if value is array-like - if so, use upsert_column logic
        if isinstance(value, (pd.Series, np.ndarray)):
            return self.upsert_column(name, value)
        # Scalar value - use original fill_column logic
        return self._table.assign(**{name: value})

    @staticmethod
    def _build_tensor_row(row: PandasRow, row_idx: int) -> np.ndarray:
        from ray.data.extensions import TensorArrayElement

        tensor = row[TENSOR_COLUMN_NAME].iloc[row_idx]
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

    def drop(self, columns: List[str]) -> Block:
        return self._table.drop(columns, axis="columns")

    def select(self, columns: List[str]) -> "pandas.DataFrame":
        if not all(isinstance(col, str) for col in columns):
            raise ValueError(
                "Columns must be a list of column name strings when aggregating on "
                f"Pandas blocks, but got: {columns}."
            )
        return self._table[columns]

    def rename_columns(self, columns_rename: Dict[str, str]) -> "pandas.DataFrame":
        return self._table.rename(columns=columns_rename, inplace=False, copy=False)

    def upsert_column(
        self, column_name: str, column_data: BlockColumn
    ) -> "pandas.DataFrame":
        import pyarrow

        if isinstance(column_data, (pyarrow.Array, pyarrow.ChunkedArray)):
            column_data = column_data.to_pandas()

        return self._table.assign(**{column_name: column_data})

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
        import pyarrow as pa

        from ray.air.util.tensor_extensions.pandas import TensorDtype

        # Set `preserve_index=False` so that Arrow doesn't add a '__index_level_0__'
        # column to the resulting table.
        arrow_table = pa.Table.from_pandas(self._table, preserve_index=False)

        # NOTE: Pandas by default coerces all-null column types (including None,
        #       NaN, etc) into "double" type by default, which is incorrect in a
        #       a lot of cases.
        #
        #       To fix that, we traverse all the columns after conversion and
        #       replace all-null ones with the column of null-type that allows
        #       these columns to be properly combined with the same column
        #       containing non-null values and carrying appropriate type later.
        null_coerced_columns = {}

        for idx, col_name in enumerate(self._table.columns):
            col = self._table[col_name]

            # Skip coercing tensors to null-type to avoid type information loss
            # See https://github.com/ray-project/ray/issues/59087 for context
            if isinstance(col.dtype, TensorDtype):
                continue

            if not col.notna().any():
                # If there are only null-values, coerce column to Arrow's `NullType`
                null_coerced_columns[(idx, col_name)] = pa.nulls(
                    len(col), type=pa.null()
                )

        # NOTE: We're updating columns in place to preserve any potential metadata
        #       set from conversion from original Pandas data-frame
        for (idx, col_name), null_col in null_coerced_columns.items():
            arrow_table = arrow_table.set_column(idx, col_name, null_col)

        return arrow_table

    def num_rows(self) -> int:
        return self._table.shape[0]

    def size_bytes(self) -> int:
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

        # Get initial memory usage.
        # No need for deep inspection here, as we will handle the str, object and
        # extension columns separately.
        memory_usage = self._table.memory_usage(index=True, deep=False)

        # TensorDtype for ray.air.util.tensor_extensions.pandas.TensorDtype
        object_need_check = (TensorDtype,)
        max_sample_count = _PANDAS_SIZE_BYTES_MAX_SAMPLE_COUNT

        # Handle object columns separately
        for column in self._table.columns:
            # For str, object and extension dtypes, we calculate the size
            # by sampling the data.
            dtype = self._table[column].dtype
            if (
                is_string_dtype(dtype)
                or is_object_dtype(dtype)
                or isinstance(dtype, object_need_check)
            ):
                total_size = len(self._table[column])

                # Determine the sample size based on max_sample_count
                sample_size = min(total_size, max_sample_count)
                # Skip size calculation for empty columns
                if sample_size == 0:
                    continue
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
                    # Add the data memory usage on top of the index memory usage.
                    memory_usage[column] += int(column_memory)
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

    def sort(self, sort_key: "SortKey"):
        assert (
            sort_key.get_columns()
        ), f"Sorting columns couldn't be empty (got {sort_key.get_columns()})"

        if self._table.shape[0] == 0:
            return self._empty_table()

        columns, ascending = sort_key.to_pandas_sort_args()
        return self._table.sort_values(by=columns, ascending=ascending)

    def sort_and_partition(
        self, boundaries: List[T], sort_key: "SortKey"
    ) -> List[Block]:
        table = self.sort(sort_key)

        if table.shape[0] == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling sort_indices() will raise an error.
            return [self._empty_table() for _ in range(len(boundaries) + 1)]
        elif len(boundaries) == 0:
            return [table]

        return BlockAccessor.for_block(table)._find_partitions_sorted(
            boundaries, sort_key
        )

    @staticmethod
    def merge_sorted_blocks(
        blocks: List[Block], sort_key: "SortKey"
    ) -> Tuple[Block, "BlockMetadataWithSchema"]:
        pd = lazy_import_pandas()
        stats = BlockExecStats.builder()
        blocks = [b for b in blocks if b.shape[0] > 0]
        if len(blocks) == 0:
            ret = PandasBlockAccessor._empty_table()
        else:
            # Handle blocks of different types.
            blocks = TableBlockAccessor.normalize_block_types(blocks, BlockType.PANDAS)
            ret = pd.concat(blocks, ignore_index=True)
            columns, ascending = sort_key.to_pandas_sort_args()
            ret = ret.sort_values(by=columns, ascending=ascending)
        from ray.data.block import BlockMetadataWithSchema

        return ret, BlockMetadataWithSchema.from_block(ret, stats=stats.build())

    def block_type(self) -> BlockType:
        return BlockType.PANDAS

    def iter_rows(
        self, public_row_format: bool
    ) -> Iterator[Union[Mapping, np.ndarray]]:
        num_rows = self.num_rows()
        for i in range(num_rows):
            row = self._get_row(i)
            if public_row_format:
                yield row.as_pydict()
            else:
                yield row

    def filter(self, predicate_expr: "Expr") -> "pandas.DataFrame":
        """Filter rows based on a predicate expression."""
        if self._table.empty:
            return self._table

        from ray.data._internal.planner.plan_expression.expression_evaluator import (
            eval_expr,
        )

        # Evaluate the expression to get a boolean mask
        mask = eval_expr(predicate_expr, self._table)

        # Use pandas boolean indexing
        return self._table[mask]
