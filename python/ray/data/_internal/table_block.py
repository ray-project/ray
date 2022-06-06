import collections
from typing import Dict, Iterator, List, Union, Any, TypeVar, TYPE_CHECKING

import numpy as np

from ray.data.block import Block, BlockAccessor
from ray.data.row import TableRow
from ray.data._internal.block_builder import BlockBuilder
from ray.data._internal.size_estimator import SizeEstimator

if TYPE_CHECKING:
    from ray.data._internal.sort import SortKeyT


# The internal column name used for pure-tensor datasets, represented as
# single-tensor-column tables.
VALUE_COL_NAME = "__value__"

T = TypeVar("T")

# The max size of Python tuples to buffer before compacting them into a
# table in the BlockBuilder.
MAX_UNCOMPACTED_SIZE_BYTES = 50 * 1024 * 1024


class TableBlockBuilder(BlockBuilder[T]):
    def __init__(self, block_type):
        # The set of uncompacted Python values buffered.
        self._columns = collections.defaultdict(list)
        # The set of compacted tables we have built so far.
        self._tables: List[Any] = []
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
            item = {VALUE_COL_NAME: item}
        if not isinstance(item, dict):
            raise ValueError(
                "Returned elements of an TableBlock must be of type `dict`, "
                "got {} (type {}).".format(item, type(item))
            )
        for key, value in item.items():
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
        self._tables_size_bytes += accessor.size_bytes()
        self._num_rows += accessor.num_rows()

    def _table_from_pydict(self, columns: Dict[str, List[Any]]) -> Block:
        raise NotImplementedError

    def _concat_tables(self, tables: List[Block]) -> Block:
        raise NotImplementedError

    @staticmethod
    def _empty_table() -> Any:
        raise NotImplementedError

    def build(self) -> Block:
        if self._columns:
            tables = [self._table_from_pydict(self._columns)]
        else:
            tables = []
        tables.extend(self._tables)
        if len(tables) > 1:
            return self._concat_tables(tables)
        elif len(tables) > 0:
            return tables[0]
        else:
            return self._empty_table()

    def num_rows(self) -> int:
        return self._num_rows

    def get_estimated_memory_usage(self) -> int:
        if self._num_rows == 0:
            return 0
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


class TableBlockAccessor(BlockAccessor):
    ROW_TYPE: TableRow = TableRow

    def __init__(self, table: Any):
        self._table = table

    def _get_row(self, index: int, copy: bool = False) -> Union[TableRow, np.ndarray]:
        row = self.slice(index, index + 1, copy=copy)
        if self.is_tensor_wrapper():
            row = self._build_tensor_row(row)
        else:
            row = self.ROW_TYPE(row)
        return row

    @staticmethod
    def _build_tensor_row(row: TableRow) -> np.ndarray:
        raise NotImplementedError

    def to_native(self) -> Block:
        if self.is_tensor_wrapper():
            native = self.to_numpy()
        else:
            # Always promote Arrow blocks to pandas for consistency, since
            # we lazily convert pandas->Arrow internally for efficiency.
            native = self.to_pandas()
        return native

    def column_names(self) -> List[str]:
        raise NotImplementedError

    def to_block(self) -> Block:
        return self._table

    def is_tensor_wrapper(self) -> bool:
        return self.column_names() == [VALUE_COL_NAME]

    def iter_rows(self) -> Iterator[Union[TableRow, np.ndarray]]:
        outer = self

        class Iter:
            def __init__(self):
                self._cur = -1

            def __iter__(self):
                return self

            def __next__(self):
                self._cur += 1
                if self._cur < outer.num_rows():
                    return outer._get_row(self._cur)
                raise StopIteration

        return Iter()

    def _zip(self, acc: BlockAccessor) -> "Block[T]":
        raise NotImplementedError

    def zip(self, other: "Block[T]") -> "Block[T]":
        acc = BlockAccessor.for_block(other)
        if not isinstance(acc, type(self)):
            raise ValueError(
                "Cannot zip {} with block of type {}".format(type(self), type(other))
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

    def _sample(self, n_samples: int, key: "SortKeyT") -> Any:
        raise NotImplementedError

    def sample(self, n_samples: int, key: "SortKeyT") -> Any:
        if key is None or callable(key):
            raise NotImplementedError(
                f"Table sort key must be a column name, was: {key}"
            )
        if self.num_rows() == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling table.select() will raise an error.
            return self._empty_table()
        k = min(n_samples, self.num_rows())
        return self._sample(k, key)
