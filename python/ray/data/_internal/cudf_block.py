"""cuDF accessor implementation for Ray Data.

Provides cuDF DataFrame support as a batch format for GPU-native UDF pipelines.
cuDF is a batch-format only: blocks are always Arrow or Pandas in the object store,
and cuDF DataFrames are created transiently within tasks when batch_format="cudf".

Requires cudf to be installed. All cudf imports are lazy to avoid breaking
CPU-only installations.
"""

from __future__ import annotations

import collections
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterator,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
)

import numpy as np

from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockColumn,
    BlockColumnAccessor,
    BlockType,
    U,
)

if TYPE_CHECKING:
    import cudf
    import pandas
    import pyarrow

    from ray.data.expressions import Expr

T = TypeVar("T")
logger = logging.getLogger(__name__)

_cudf = None


def _lazy_import_cudf():
    """Lazy import cudf to avoid ImportError when cudf is not installed."""
    global _cudf
    if _cudf is None:
        import cudf as cudf_module

        _cudf = cudf_module
    return _cudf


def _cudf_series_to_pylist(series: "cudf.Series") -> List[Any]:
    """Convert a cuDF Series to a Python list, preserving nulls as None."""
    return series.to_arrow().to_pylist()


# Schema type for cuDF blocks (similar to PandasBlockSchema)
CudfBlockSchema = collections.namedtuple("CudfBlockSchema", ["names", "types"])


class CudfRow(Mapping):
    """Row of a tabular Dataset backed by a cuDF DataFrame block."""

    def __init__(self, row: "cudf.DataFrame"):
        self._row = row

    def __getitem__(self, key: Union[str, List[str]]) -> Any:
        def get_item(keys: List[str]) -> Any:
            col = self._row[keys]
            if len(col) == 0:
                return None
            items = col.iloc[0]  # cuDF Series of row values
            return tuple(_cudf_series_to_pylist(items))

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
        return self._row.to_dict(orient="records")[0]

    def __str__(self):
        return str(dict(self.items()))

    def __repr__(self):
        return repr(dict(self.items()))


class CudfBlockColumnAccessor(BlockColumnAccessor):
    """BlockColumnAccessor for cudf.Series columns."""

    def __init__(self, col: "cudf.Series"):
        super().__init__(col)

    def count(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        return self._column.count() if ignore_nulls else len(self._column)

    def sum(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        if self._column.isna().all():
            return None
        return self._column.sum(skipna=ignore_nulls)

    def min(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        if self._column.isna().all():
            return None
        return self._column.min(skipna=ignore_nulls)

    def max(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        if self._column.isna().all():
            return None
        return self._column.max(skipna=ignore_nulls)

    def mean(self, *, ignore_nulls: bool, as_py: bool = True) -> Optional[U]:
        if self._column.isna().all():
            return None
        return self._column.mean(skipna=ignore_nulls)

    def sum_of_squared_diffs_from_mean(
        self,
        ignore_nulls: bool,
        mean: Optional[U] = None,
        as_py: bool = True,
    ) -> Optional[U]:
        if mean is None:
            mean = self.mean(ignore_nulls=ignore_nulls)
        if mean is None:
            return None
        return ((self._column - mean) ** 2).sum(skipna=ignore_nulls)

    def quantile(
        self, *, q: float, ignore_nulls: bool, as_py: bool = True
    ) -> Optional[U]:
        return self._column.quantile(q=q)

    def value_counts(self) -> Optional[Dict[str, List]]:
        value_counts = self._column.value_counts()
        if len(value_counts) == 0:
            return None
        # Counts are always non-null int64 so the fast path in the helper is
        # always taken.  Index values may be any type (strings, categoricals,
        # etc.) so use to_arrow().to_pylist() which handles all types correctly.
        return {
            "values": value_counts.index.to_arrow().to_pylist(),
            "counts": _cudf_series_to_pylist(value_counts),
        }

    def unique(self) -> BlockColumn:
        return self._column.unique()

    def to_pylist(self) -> List[Any]:
        return _cudf_series_to_pylist(self._column)

    def to_numpy(self, zero_copy_only: bool = False) -> np.ndarray:
        return self._column.to_numpy()

    def _as_arrow_compatible(self) -> Union[List[Any], "pyarrow.Array"]:
        return self._column.to_arrow()

    def dropna(self) -> BlockColumn:
        return self._column.dropna()

    def is_composed_of_lists(self) -> bool:
        return False  # cuDF list columns need special handling if added

    def hash(self) -> BlockColumn:
        # hash_values() returns a Series directly (per cuDF docs)
        return self._column.hash_values()

    def flatten(self) -> BlockColumn:
        raise NotImplementedError("flatten not implemented for cudf.Series")


class CudfBlockAccessor(TableBlockAccessor):
    """BlockAccessor for cuDF DataFrame batches.

    Used for in-task operations on cuDF DataFrames when batch_format="cudf".
    cuDF is batch-format only: blocks are stored as Arrow or Pandas in the
    Ray object store; cuDF DataFrames only exist transiently within tasks.
    """

    ROW_TYPE = CudfRow

    def __init__(self, table: "cudf.DataFrame"):
        _lazy_import_cudf()
        super().__init__(table)

    def _get_row(self, index: int) -> CudfRow:
        base_row = self.slice(index, index + 1, copy=False)
        return CudfRow(base_row)

    def column_names(self) -> List[str]:
        return self._table.columns.tolist()

    def to_cudf(self) -> "cudf.DataFrame":
        """Return the block as cudf.DataFrame (zero-copy for cuDF blocks)."""
        return self._table

    def to_arrow(self) -> "pyarrow.Table":
        return self._table.to_arrow()

    def to_pandas(self) -> "pandas.DataFrame":
        return self._table.to_pandas()

    def fill_column(self, name: str, value: Any) -> Block:
        cudf = _lazy_import_cudf()
        if isinstance(value, (cudf.Series, np.ndarray)):
            return self.upsert_column(name, value)
        return self._table.assign(**{name: value})

    def slice(self, start: int, end: int, copy: bool = False) -> "cudf.DataFrame":
        view = self._table.iloc[start:end]
        if copy:
            view = view.copy(deep=True)
        return view

    def take(self, indices: List[int]) -> "cudf.DataFrame":
        return self._table.take(indices)

    def drop(self, columns: List[str]) -> Block:
        return self._table.drop(columns, axis=1)

    def select(self, columns: List[str]) -> "cudf.DataFrame":
        if not all(isinstance(col, str) for col in columns):
            raise ValueError(
                "Columns must be a list of column name strings when aggregating on "
                f"cuDF blocks, but got: {columns}."
            )
        return self._table[columns]

    def rename_columns(self, columns_rename: Dict[str, str]) -> "cudf.DataFrame":
        return self._table.rename(columns=columns_rename)

    def upsert_column(
        self, column_name: str, column_data: BlockColumn
    ) -> "cudf.DataFrame":
        import pandas as pd
        import pyarrow

        cudf = _lazy_import_cudf()
        if isinstance(column_data, (pyarrow.Array, pyarrow.ChunkedArray)):
            column_data = cudf.Series(column_data)
        elif isinstance(column_data, pd.Series):
            column_data = cudf.from_pandas(column_data)
        return self._table.assign(**{column_name: column_data})

    def random_shuffle(self, random_seed: Optional[int]) -> "cudf.DataFrame":
        return self._table.sample(frac=1.0, random_state=random_seed).reset_index(
            drop=True
        )

    def schema(self) -> CudfBlockSchema:
        dtypes = self._table.dtypes
        schema = CudfBlockSchema(
            names=self._table.columns.tolist(),
            types=[str(dt) for dt in dtypes.values],
        )
        return schema

    def num_rows(self) -> int:
        return len(self._table)

    def size_bytes(self) -> int:
        return self._table.memory_usage(deep=True).sum()

    def _zip(self, acc: BlockAccessor) -> "cudf.DataFrame":
        cudf = _lazy_import_cudf()
        r = self._table.copy(deep=False)
        if hasattr(acc, "to_cudf"):
            s = acc.to_cudf()
        elif acc.block_type() == BlockType.ARROW:
            # Arrow: from_arrow can be zero-copy (cuDF built on Arrow)
            s = cudf.DataFrame.from_arrow(acc.to_arrow())
        else:
            # Pandas: from_pandas avoids Arrow round-trip
            s = cudf.from_pandas(acc.to_pandas())
        column_names = set(r.columns)
        for col_name in s.columns:
            col = s[col_name]
            if col_name in column_names:
                i = 1
                new_name = col_name
                while new_name in column_names:
                    new_name = "{}_{}".format(col_name, i)
                    i += 1
                col_name = new_name
            r[col_name] = col
            column_names.add(col_name)
        return r

    @staticmethod
    def _empty_table() -> "cudf.DataFrame":
        cudf = _lazy_import_cudf()
        return cudf.DataFrame()

    def filter(self, predicate_expr: "Expr") -> "cudf.DataFrame":
        """Filter rows based on a predicate expression (cuDF-native)."""
        if len(self._table) == 0:
            return self._table

        from ray.data._internal.planner.plan_expression.expression_evaluator import (
            eval_expr,
        )

        # eval_expr supports cuDF blocks natively - returns cudf.Series (boolean mask)
        mask = eval_expr(predicate_expr, self._table)

        # Scalar result (e.g. literal True/False or pyarrow.Scalar)
        if isinstance(mask, bool):
            return self._table if mask else self._empty_table()
        if hasattr(mask, "as_py"):
            return self._table if mask.as_py() else self._empty_table()
        # Boolean mask - apply directly to cuDF DataFrame
        return self._table[mask]

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

        result = {col: self._table[col].to_numpy() for col in columns}
        if should_be_single_ndarray:
            return result[columns[0]]
        return result
