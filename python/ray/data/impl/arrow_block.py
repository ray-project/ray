import collections
import random
from typing import Iterator, List, Union, Tuple, Any, TypeVar, Optional, \
    TYPE_CHECKING

import numpy as np

try:
    import pyarrow
except ImportError:
    pyarrow = None

from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.impl.block_builder import BlockBuilder
from ray.data.impl.simple_block import SimpleBlockBuilder
from ray.data.impl.tensor_block import TensorBlockBuilder

if TYPE_CHECKING:
    import pandas

T = TypeVar("T")

# An Arrow block can be sorted by a list of (column, asc/desc) pairs,
# e.g. [("column1", "ascending"), ("column2", "descending")]
SortKeyT = List[Tuple[str, str]]


class ArrowRow:
    def __init__(self, row: "pyarrow.Table"):
        self._row = row

    def as_pydict(self) -> dict:
        return {k: v[0] for k, v in self._row.to_pydict().items()}

    def keys(self) -> Iterator[str]:
        return self.as_pydict().keys()

    def values(self) -> Iterator[Any]:
        return self.as_pydict().values()

    def items(self) -> Iterator[Tuple[str, Any]]:
        return self.as_pydict().items()

    def __getitem__(self, key: str) -> Any:
        return self._row[key][0].as_py()

    def __eq__(self, other: Any) -> bool:
        return self.as_pydict() == other

    def __str__(self):
        return "ArrowRow({})".format(self.as_pydict())

    def __repr__(self):
        return str(self)


class DelegatingArrowBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._builder = None

    def add(self, item: Any) -> None:
        if self._builder is None:
            if isinstance(item, dict):
                try:
                    check = ArrowBlockBuilder()
                    check.add(item)
                    check.build()
                    self._builder = ArrowBlockBuilder()
                except (TypeError, pyarrow.lib.ArrowInvalid):
                    self._builder = SimpleBlockBuilder()
            elif isinstance(item, np.ndarray):
                self._builder = TensorBlockBuilder()
            else:
                self._builder = SimpleBlockBuilder()
        self._builder.add(item)

    def add_block(self, block: Block) -> None:
        if self._builder is None:
            self._builder = BlockAccessor.for_block(block).builder()
        self._builder.add_block(block)

    def build(self) -> Block:
        if self._builder is None:
            self._builder = ArrowBlockBuilder()
        return self._builder.build()

    def num_rows(self) -> int:
        return self._builder.num_rows() if self._builder is not None else 0


class ArrowBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        if pyarrow is None:
            raise ImportError("Run `pip install pyarrow` for Arrow support")
        self._columns = collections.defaultdict(list)
        self._tables: List["pyarrow.Table"] = []
        self._num_rows = 0

    def add(self, item: Union[dict, ArrowRow]) -> None:
        if isinstance(item, ArrowRow):
            item = item.as_pydict()
        if not isinstance(item, dict):
            raise ValueError(
                "Returned elements of an ArrowBlock must be of type `dict`, "
                "got {} (type {}).".format(item, type(item)))
        for key, value in item.items():
            self._columns[key].append(value)
        self._num_rows += 1

    def add_block(self, block: "pyarrow.Table") -> None:
        assert isinstance(block, pyarrow.Table), block
        self._tables.append(block)
        self._num_rows += block.num_rows

    def build(self) -> Block:
        if self._columns:
            tables = [pyarrow.Table.from_pydict(self._columns)]
        else:
            tables = []
        tables.extend(self._tables)
        if len(tables) > 1:
            return pyarrow.concat_tables(tables)
        elif len(tables) > 0:
            return tables[0]
        else:
            return pyarrow.Table.from_pydict({})

    def num_rows(self) -> int:
        return self._num_rows


class ArrowBlockAccessor(BlockAccessor):
    def __init__(self, table: "pyarrow.Table"):
        if pyarrow is None:
            raise ImportError("Run `pip install pyarrow` for Arrow support")
        self._table = table

    def iter_rows(self) -> Iterator[ArrowRow]:
        outer = self

        class Iter:
            def __init__(self):
                self._cur = -1

            def __iter__(self):
                return self

            def __next__(self):
                self._cur += 1
                if self._cur < outer._table.num_rows:
                    row = ArrowRow(outer._table.slice(self._cur, 1))
                    return row
                raise StopIteration

        return Iter()

    def slice(self, start: int, end: int, copy: bool) -> "pyarrow.Table":
        view = self._table.slice(start, end - start)
        if copy:
            # TODO(ekl) there must be a cleaner way to force a copy of a table.
            copy = [c.to_pandas() for c in view.itercolumns()]
            return pyarrow.Table.from_arrays(copy, schema=self._table.schema)
        else:
            return view

    def random_shuffle(self, random_seed: Optional[int]) -> List[T]:
        random = np.random.RandomState(random_seed)
        return self._table.take(random.permutation(self.num_rows()))

    def schema(self) -> "pyarrow.lib.Schema":
        return self._table.schema

    def to_pandas(self) -> "pandas.DataFrame":
        return self._table.to_pandas()

    def to_arrow(self) -> "pyarrow.Table":
        return self._table

    def num_rows(self) -> int:
        return self._table.num_rows

    def size_bytes(self) -> int:
        return self._table.nbytes

    @staticmethod
    def builder() -> ArrowBlockBuilder[T]:
        return ArrowBlockBuilder()

    def sample(self, n_samples: int, key: SortKeyT) -> List[T]:
        if key is None or callable(key):
            raise NotImplementedError(
                "Arrow sort key must be a column name, was: {}".format(key))
        k = min(n_samples, self._table.num_rows)
        indices = random.sample(range(self._table.num_rows), k)
        return self._table.select([k[0] for k in key]).take(indices)

    def sort_and_partition(self, boundaries: List[T], key: SortKeyT,
                           descending: bool) -> List["Block[T]"]:
        if len(key) > 1:
            raise NotImplementedError(
                "sorting by multiple columns is not supported yet")

        import pyarrow.compute as pac

        indices = pac.sort_indices(self._table, sort_keys=key)
        table = self._table.take(indices)
        if len(boundaries) == 0:
            return [table]

        # For each boundary value, count the number of items that are less
        # than it. Since the block is sorted, these counts partition the items
        # such that boundaries[i] <= x < boundaries[i + 1] for each x in
        # partition[i]. If `descending` is true, `boundaries` would also be
        # in descending order and we only need to count the number of items
        # *greater than* the boundary value instead.
        col, _ = key[0]
        comp_fn = pac.greater if descending else pac.less
        boundary_indices = [
            pac.sum(comp_fn(table[col], b)).as_py() for b in boundaries
        ]
        ret = []
        prev_i = 0
        for i in boundary_indices:
            ret.append(table.slice(prev_i, i - prev_i))
            prev_i = i
        ret.append(table.slice(prev_i))
        return ret

    @staticmethod
    def merge_sorted_blocks(
            blocks: List[Block[T]], key: SortKeyT,
            _descending: bool) -> Tuple[Block[T], BlockMetadata]:
        ret = pyarrow.concat_tables(blocks)
        indices = pyarrow.compute.sort_indices(ret, sort_keys=key)
        ret = ret.take(indices)
        return ret, ArrowBlockAccessor(ret).get_metadata(None)
