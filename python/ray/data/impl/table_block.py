from typing import Dict, Iterator, List, Union, Tuple, Any, TypeVar, Optional, \
    TYPE_CHECKING

from ray.data.block import Block, BlockAccessor
from ray.data.impl.block_builder import BlockBuilder
from ray.data.impl.arrow_block import ArrowRow, ArrowBlockBuilder
from ray.data.impl.pandas_block import PandasRow, PandasBlockBuilder
from ray.data.impl.simple_block import SimpleBlockBuilder

try:
    import pyarrow
except ImportError:
    pyarrow = None

try:
    import pandas
except ImportError:
    pandas = None

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


class DelegatingBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._builder = None

    def add(self, item: Any) -> None:
        if self._builder is None:
            if isinstance(item, dict) or isinstance(item, ArrowRow):
                try:
                    check = ArrowBlockBuilder()
                    check.add(item)
                    check.build()
                    self._builder = ArrowBlockBuilder()
                except (TypeError, pyarrow.lib.ArrowInvalid):
                    self._builder = SimpleBlockBuilder()
            elif isinstance(item, PandasRow):
                self._builder = PandasBlockBuilder()
            else:
                self._builder = SimpleBlockBuilder()
        self._builder.add(item)

    def add_block(self, block: Block) -> None:
        if self._builder is None:
            self._builder = BlockAccessor.for_block(block).builder()
        self._builder.add_block(block)

    def build(self) -> Block:
        if self._builder is None:
            # TODO: Allow switching to PandasBlockBuilder
            self._builder = ArrowBlockBuilder()
        return self._builder.build()

    def num_rows(self) -> int:
        return self._builder.num_rows() if self._builder is not None else 0

    def get_estimated_memory_usage(self) -> int:
        if self._builder is None:
            return 0
        return self._builder.get_estimated_memory_usage()


class TableBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        # The set of uncompacted Python values buffered.
        self._columns = collections.defaultdict(list)
        # The set of compacted tables we have built so far.
        self._tables: List[Any] = []
        self._tables_size_bytes = 0
        # Size estimator for un-compacted table values.
        self._uncompacted_size = SizeEstimator()
        self._num_rows = 0
        self._num_compactions = 0

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

    def add_block(self, block: Union["pyarrow.Table", "pandas.DataFrame"]) -> None:
        assert isinstance(block, pyarrow.Table) or isinstance(block, pandas.DataFrame), block
        accessor = BlockAccessor.for_block(block)
        self._tables.append(block)
        self._tables_size_bytes += accessor.size_bytes()
        self._num_rows += accessor.num_rows()

    def _table_from_pydict(self, columns: Dict[str, List[Any]]) -> Block:
        raise NotImplementedError

    def _concat_tables(self, tables: List[Block]) -> Block:
        raise NotImplementedError

    def _empty_table(self):
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
