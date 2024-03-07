import collections
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

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.numpy_support import convert_udf_returns_to_numpy, is_array_like
from ray.data._internal.row import TableRow
from ray.data._internal.size_estimator import SizeEstimator
from ray.data.block import Block, BlockAccessor

if TYPE_CHECKING:
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey


T = TypeVar("T")

# The max size of Python tuples to buffer before compacting them into a
# table in the BlockBuilder.
MAX_UNCOMPACTED_SIZE_BYTES = 50 * 1024 * 1024


class TableBlockBuilder(BlockBuilder):
    def __init__(self, block_type):
        # The set of uncompacted Python values buffered.
        self._columns = collections.defaultdict(list)
        # The column names of uncompacted Python values buffered.
        self._column_names = None
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

        item_column_names = item.keys()
        if self._column_names is not None:
            # Check all added rows have same columns.
            if item_column_names != self._column_names:
                raise ValueError(
                    "Current row has different columns compared to previous rows. "
                    f"Columns of current row: {sorted(item_column_names)}, "
                    f"Columns of previous rows: {sorted(self._column_names)}."
                )
        else:
            # Initialize column names with the first added row.
            self._column_names = item_column_names

        for key, value in item.items():
            if is_array_like(value) and not isinstance(value, np.ndarray):
                value = np.array(value)
            self._columns[key].append(value)
        self._num_rows += 1
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
        columns = {
            key: convert_udf_returns_to_numpy(col) for key, col in self._columns.items()
        }
        if columns:
            tables = [self._table_from_pydict(columns)]
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
        columns = {
            key: convert_udf_returns_to_numpy(col) for key, col in self._columns.items()
        }
        block = self._table_from_pydict(columns)
        self.add_block(block)
        self._uncompacted_size = SizeEstimator()
        self._columns.clear()
        self._num_compactions += 1


class TableBlockAccessor(BlockAccessor):
    ROW_TYPE: TableRow = TableRow

    def __init__(self, table: Any):
        self._table = table

    def _get_row(self, index: int, copy: bool = False) -> Union[TableRow, np.ndarray]:
        base_row = self.slice(index, index + 1, copy=copy)
        row = self.ROW_TYPE(base_row)
        return row

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

    def append_column(self, name: str, data: Any) -> Block:
        raise NotImplementedError

    def to_block(self) -> Block:
        return self._table

    def iter_rows(
        self, public_row_format: bool
    ) -> Iterator[Union[Mapping, np.ndarray]]:
        outer = self

        class Iter:
            def __init__(self):
                self._cur = -1

            def __iter__(self):
                return self

            def __next__(self):
                self._cur += 1
                if self._cur < outer.num_rows():
                    row = outer._get_row(self._cur)
                    if public_row_format and isinstance(row, TableRow):
                        return row.as_pydict()
                    else:
                        return row
                raise StopIteration

        return Iter()

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

    @classmethod
    def normalize_block_types(
        cls,
        blocks: List[Block],
        normalize_type: Optional[str] = None,
    ) -> List[Block]:
        """Normalize input blocks to the specified `normalize_type`. If the blocks
        are already all of the same type, returns the original blocks.

         Args:
            blocks: A list of TableBlocks to be normalized.
            normalize_type: The type to normalize the blocks to. If None,
                the default block type (Arrow) is used.

        Returns:
            A list of blocks of the same type.
        """
        seen_types = set()
        for block in blocks:
            acc = BlockAccessor.for_block(block)
            if not isinstance(acc, TableBlockAccessor):
                raise ValueError(
                    "Block type normalization is only supported for TableBlock, "
                    f"but received block of type: {type(block)}."
                )
            seen_types.add(type(block))

        # Return original blocks if they are all of the same type.
        if len(seen_types) <= 1:
            return blocks

        if normalize_type == "arrow":
            results = [BlockAccessor.for_block(block).to_arrow() for block in blocks]
        elif normalize_type == "pandas":
            results = [BlockAccessor.for_block(block).to_pandas() for block in blocks]
        else:
            results = [BlockAccessor.for_block(block).to_default() for block in blocks]

        if any(not isinstance(block, type(results[0])) for block in results):
            raise ValueError(
                "Expected all blocks to be of the same type after normalization, but "
                f"got different types: {[type(b) for b in results]}. "
                "Try using blocks of the same type to avoid the issue "
                "with block normalization."
            )
        return results
