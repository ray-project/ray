import collections

from typing import Dict, Iterator, List, Union, Tuple, Any, TypeVar, \
    TYPE_CHECKING

from ray.data.block import Block, BlockAccessor
from ray.data.impl.block_builder import BlockBuilder
from ray.data.impl.size_estimator import SizeEstimator

if TYPE_CHECKING:
    from ray.data.impl.sort import SortKeyT

T = TypeVar("T")

# The max size of Python tuples to buffer before compacting them into a
# table in the BlockBuilder.
MAX_UNCOMPACTED_SIZE_BYTES = 50 * 1024 * 1024


class TableRow:
    def __init__(self, row: Any):
        self._row = row

    def as_pydict(self) -> dict:
        raise NotImplementedError

    def keys(self) -> Iterator[str]:
        return self.as_pydict().keys()

    def values(self) -> Iterator[Any]:
        return self.as_pydict().values()

    def items(self) -> Iterator[Tuple[str, Any]]:
        return self.as_pydict().items()

    def __getitem__(self, key: str) -> Any:
        raise NotImplementedError

    def __eq__(self, other: Any) -> bool:
        return self.as_pydict() == other

    def __str__(self):
        return str(self.as_pydict())

    def __repr__(self):
        return str(self)

    def __len__(self):
        raise NotImplementedError


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

    def add(self, item: Union[dict, TableRow]) -> None:
        if isinstance(item, TableRow):
            item = item.as_pydict()
        if not isinstance(item, dict):
            raise ValueError(
                "Returned elements of an TableBlock must be of type `dict`, "
                "got {} (type {}).".format(item, type(item)))
        for key, value in item.items():
            self._columns[key].append(value)
        self._num_rows += 1
        self._compact_if_needed()
        self._uncompacted_size.add(item)

    def add_block(self, block: Any) -> None:
        assert isinstance(block, self._block_type), block
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
    def __init__(self, table: Any):
        self._table = table

    def _create_table_row(self, row: Any) -> TableRow:
        raise NotImplementedError

    def iter_rows(self) -> Iterator[TableRow]:
        outer = self

        class Iter:
            def __init__(self):
                self._cur = -1

            def __iter__(self):
                return self

            def __next__(self):
                self._cur += 1
                if self._cur < outer.num_rows():
                    row = outer._create_table_row(
                        outer.slice(self._cur, self._cur + 1, copy=False))
                    return row
                raise StopIteration

        return Iter()

    def _zip(self, acc: BlockAccessor) -> "Block[T]":
        raise NotImplementedError

    def zip(self, other: "Block[T]") -> "Block[T]":
        acc = BlockAccessor.for_block(other)
        if not isinstance(acc, type(self)):
            raise ValueError("Cannot zip {} with block of type {}".format(
                type(self), type(other)))
        if acc.num_rows() != self.num_rows():
            raise ValueError(
                "Cannot zip self (length {}) with block of length {}".format(
                    self.num_rows(), acc.num_rows()))
        return self._zip(acc)

    @staticmethod
    def _empty_table() -> Any:
        raise NotImplementedError

    def _sample(self, n_samples: int, key: "SortKeyT") -> Any:
        raise NotImplementedError

    def sample(self, n_samples: int, key: "SortKeyT") -> Any:
        if key is None or callable(key):
            raise NotImplementedError(
                f"Table sort key must be a column name, was: {key}")
        if self.num_rows() == 0:
            # If the pyarrow table is empty we may not have schema
            # so calling table.select() will raise an error.
            return self._empty_table()
        k = min(n_samples, self.num_rows())
        return self._sample(k, key)
