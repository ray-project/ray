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
    Tuple,
    TypeVar,
    Union,
)

import numpy as np

from ray._private.utils import _get_pyarrow_version
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.tensor_extensions.arrow import (
    convert_to_pyarrow_array,
    pyarrow_table_from_pydict,
)
from ray.data._internal.arrow_ops import transform_polars, transform_pyarrow
from ray.data._internal.numpy_support import convert_to_numpy
from ray.data._internal.row import TableRow
from ray.data._internal.table_block import TableBlockAccessor, TableBlockBuilder
from ray.data._internal.util import find_partitions
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadata,
    BlockType,
    U,
)
from ray.data.context import DataContext

try:
    import pyarrow
except ImportError:
    pyarrow = None


if TYPE_CHECKING:
    import pandas

    from ray.data._internal.planner.exchange.sort_task_spec import SortKey


T = TypeVar("T")
logger = logging.getLogger(__name__)


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
        pa_cols: Dict[str, pyarrow.Array] = dict()

        for col_name, col_vals in columns.items():
            np_col_vals = convert_to_numpy(col_vals)

            pa_cols[col_name] = convert_to_pyarrow_array(np_col_vals, col_name)

        return pyarrow_table_from_pydict(pa_cols)

    @staticmethod
    def _concat_tables(tables: List[Block]) -> Block:
        return transform_pyarrow.concat(tables, promote_types=True)

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
            view = transform_pyarrow.combine_chunks(view, copy)
        return view

    def random_shuffle(self, random_seed: Optional[int]) -> "pyarrow.Table":
        num_rows = self.num_rows()
        if num_rows == 0:
            return pyarrow.table([])
        random = np.random.RandomState(random_seed)
        shuffled_indices = np.arange(num_rows)
        # Shuffle all rows in-place
        random.shuffle(shuffled_indices)
        return self.take(pyarrow.array(shuffled_indices))

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

        column_values_ndarrays = []

        for col_name in columns:
            col = self._table[col_name]

            # Combine columnar values arrays to make these contiguous
            # (making them compatible with numpy format)
            combined_array = transform_pyarrow.combine_chunked_array(col)

            column_values_ndarrays.append(
                transform_pyarrow.to_numpy(combined_array, zero_copy_only=False)
            )

        if should_be_single_ndarray:
            assert len(columns) == 1
            return column_values_ndarrays[0]
        else:
            return dict(zip(columns, column_values_ndarrays))

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

    def rename_columns(self, columns_rename: Dict[str, str]) -> "pyarrow.Table":
        return self._table.rename_columns(columns_rename)

    def _sample(self, n_samples: int, sort_key: "SortKey") -> "pyarrow.Table":
        indices = random.sample(range(self._table.num_rows), n_samples)
        table = self._table.select(sort_key.get_columns())
        return transform_pyarrow.take_table(table, indices)

    def count(self, on: str, ignore_nulls: bool = False) -> Optional[U]:
        """Count the number of non-null values in the provided column."""
        import pyarrow.compute as pac

        if not isinstance(on, str):
            raise ValueError(
                "on must be a string when aggregating on Arrow blocks, but got:"
                f"{type(on)}."
            )

        if self.num_rows() == 0:
            return None

        mode = "only_valid" if ignore_nulls else "all"

        col = self._table[on]
        return pac.count(col, mode=mode).as_py()

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

    def sort(self, sort_key: "SortKey") -> Block:
        assert (
            sort_key.get_columns()
        ), f"Sorting columns couldn't be empty (got {sort_key.get_columns()})"

        if self._table.num_rows == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling sort_indices() will raise an error.
            return self._empty_table()

        context = DataContext.get_current()
        sort = get_sort_transform(context)

        return sort(self._table, sort_key)

    def sort_and_partition(
        self, boundaries: List[T], sort_key: "SortKey"
    ) -> List["Block"]:
        table = self.sort(sort_key)

        if table.num_rows == 0:
            return [self._empty_table() for _ in range(len(boundaries) + 1)]
        elif len(boundaries) == 0:
            return [table]

        return find_partitions(table, boundaries, sort_key)

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
            blocks = TableBlockAccessor.normalize_block_types(blocks, BlockType.ARROW)
            concat_and_sort = get_concat_and_sort_transform(DataContext.get_current())
            ret = concat_and_sort(blocks, sort_key)
        return ret, ArrowBlockAccessor(ret).get_metadata(exec_stats=stats.build())

    def block_type(self) -> BlockType:
        return BlockType.ARROW
