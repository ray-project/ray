"""cuDF block implementation for Ray Data.

Enables cuDF DataFrames as first-class block types for GPU-native pipelines.
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
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from ray.data._internal.table_block import TableBlockAccessor, TableBlockBuilder
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockColumn,
    BlockColumnAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockType,
    U,
)

if TYPE_CHECKING:
    import cudf
    import pandas
    import pyarrow

    from ray.data._internal.planner.exchange.sort_task_spec import SortKey
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
            items = col.iloc[0]
            return tuple(item for item in items)

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
        # Counts are always non-null int64: direct GPU→CPU via values_host.
        # Index values may be any type (strings, categoricals, etc.) so use
        # to_arrow().to_pylist() which handles all types correctly.
        return {
            "values": value_counts.index.to_arrow().to_pylist(),
            "counts": value_counts.values_host.tolist(),
        }

    def unique(self) -> BlockColumn:
        return self._column.unique()

    def to_pylist(self) -> List[Any]:
        if not self._column.has_nulls:
            # Fast path: direct GPU→CPU transfer, no Arrow intermediate.
            return self._column.values_host.tolist()
        # Null-safe path: get values and null mask in two D2H transfers.
        # values_host fills nulls with sentinel values (0/NaN), so we overlay
        # the null mask to replace those positions with Python None.
        null_mask = self._column.isnull().values_host
        raw = self._column.values_host.tolist()
        return [None if null_mask[i] else raw[i] for i in range(len(raw))]

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


class CudfBlockBuilder(TableBlockBuilder):
    """BlockBuilder for cuDF DataFrame blocks."""

    def __init__(self):
        cudf = _lazy_import_cudf()
        super().__init__(cudf.DataFrame)

    @staticmethod
    def _table_from_pydict(columns: Dict[str, List[Any]]) -> "cudf.DataFrame":
        cudf = _lazy_import_cudf()
        return cudf.DataFrame(columns)

    @staticmethod
    def _combine_tables(tables: List["cudf.DataFrame"]) -> "cudf.DataFrame":
        cudf = _lazy_import_cudf()
        if len(tables) > 1:
            return cudf.concat(tables, ignore_index=True)
        return tables[0]

    @staticmethod
    def _concat_would_copy() -> bool:
        return True

    @staticmethod
    def _empty_table() -> "cudf.DataFrame":
        cudf = _lazy_import_cudf()
        return cudf.DataFrame()

    def block_type(self) -> BlockType:
        return BlockType.CUDF


class CudfBlockAccessor(TableBlockAccessor):
    """BlockAccessor for cuDF DataFrame blocks."""

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
        import pyarrow

        cudf = _lazy_import_cudf()
        if isinstance(column_data, (pyarrow.Array, pyarrow.ChunkedArray)):
            column_data = cudf.Series(column_data)
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
    def builder() -> CudfBlockBuilder:
        return CudfBlockBuilder()

    @staticmethod
    def _empty_table() -> "cudf.DataFrame":
        return CudfBlockBuilder._empty_table()

    def _sample(self, n_samples: int, sort_key: "SortKey") -> "cudf.DataFrame":
        return self._table[sort_key.get_columns()].sample(
            n=min(n_samples, len(self._table))
        )

    def sort(self, sort_key: "SortKey") -> "cudf.DataFrame":
        assert (
            sort_key.get_columns()
        ), f"Sorting columns couldn't be empty (got {sort_key.get_columns()})"
        if len(self._table) == 0:
            return self._empty_table()
        columns, ascending = sort_key.to_pandas_sort_args()
        return self._table.sort_values(by=columns, ascending=ascending)

    def _find_partitions_sorted(
        self,
        boundaries: List[T],
        sort_key: "SortKey",
    ) -> List["cudf.DataFrame"]:
        """GPU-native partition finding via cudf.DataFrame.searchsorted.

        Avoids the col.to_numpy() calls in the base-class find_partition_index
        which would pull data off the GPU for every sort column × boundary.
        """
        cudf = _lazy_import_cudf()
        columns, ascending = sort_key.to_pandas_sort_args()

        # Normalise boundaries: production code uses tuples (one value per
        # sort column); single-column tests may pass plain scalars.
        def _val(b, i):
            return b[i] if isinstance(b, (tuple, list)) else b

        boundary_df = cudf.DataFrame(
            {col: [_val(b, i) for b in boundaries] for i, col in enumerate(columns)}
        )

        # cudf.DataFrame.searchsorted accepts ascending as bool or list[bool]
        # and returns a cupy ndarray (stays on GPU).
        # .tolist() converts only the tiny index array (n_boundaries ints) to CPU.
        bounds = (
            self._table[columns]
            .searchsorted(boundary_df, side="left", ascending=ascending)
            .tolist()
        )

        partitions = []
        last_idx = 0
        for idx in bounds:
            partitions.append(self._table[last_idx:idx])
            last_idx = idx
        partitions.append(self._table[last_idx:])
        return partitions

    def sort_and_partition(
        self, boundaries: List[T], sort_key: "SortKey"
    ) -> List[Block]:
        table = self.sort(sort_key)
        if len(table) == 0:
            return [self._empty_table() for _ in range(len(boundaries) + 1)]
        elif len(boundaries) == 0:
            return [table]
        return CudfBlockAccessor(table)._find_partitions_sorted(boundaries, sort_key)

    @staticmethod
    def merge_sorted_blocks(
        blocks: List[Block], sort_key: "SortKey"
    ) -> Tuple[Block, BlockMetadataWithSchema]:
        cudf = _lazy_import_cudf()
        stats = BlockExecStats.builder()
        blocks = [b for b in blocks if len(b) > 0]
        if len(blocks) == 0:
            ret = CudfBlockAccessor._empty_table()
        else:
            blocks = TableBlockAccessor.normalize_block_types(blocks, BlockType.CUDF)
            ret = cudf.concat(blocks, ignore_index=True)
            columns, ascending = sort_key.to_pandas_sort_args()
            ret = ret.sort_values(by=columns, ascending=ascending)
        return ret, BlockMetadataWithSchema.from_block(ret, stats=stats.build())

    def block_type(self) -> BlockType:
        return BlockType.CUDF

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
