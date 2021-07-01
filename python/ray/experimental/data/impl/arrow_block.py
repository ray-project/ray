import collections
from typing import Iterator, Union, Tuple, Any, TypeVar, TYPE_CHECKING

try:
    import pyarrow
except ImportError:
    pyarrow = None

from ray.experimental.data.impl.block import Block, BlockBuilder, \
    ListBlockBuilder

if TYPE_CHECKING:
    import pandas

T = TypeVar("T")


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
        return self.as_pydict().keys()

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
                    self._builder = ListBlockBuilder()
            else:
                self._builder = ListBlockBuilder()
        self._builder.add(item)

    def add_block(self, block: Block[T]) -> None:
        if self._builder is None:
            self._builder = block.builder()
        self._builder.add_block(block)

    def build(self) -> Block[T]:
        if self._builder is None:
            self._builder = ArrowBlockBuilder()
        return self._builder.build()


class ArrowBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        if pyarrow is None:
            raise ImportError("Run `pip install pyarrow` for Arrow support")
        self._columns = collections.defaultdict(list)

    def add(self, item: Union[dict, ArrowRow]) -> None:
        if isinstance(item, ArrowRow):
            item = item.as_pydict()
        if not isinstance(item, dict):
            raise ValueError(
                "Returned elements of an ArrowBlock must be of type `dict`, "
                "got {} (type {}).".format(item, type(item)))
        for key, value in item.items():
            self._columns[key].append(value)

    def add_block(self, block: "ArrowBlock[T]") -> None:
        other_cols = block._table.to_pydict()
        for k, vv in other_cols.items():
            self._columns[k].extend(vv)

    def build(self) -> "ArrowBlock[T]":
        return ArrowBlock(pyarrow.Table.from_pydict(self._columns))


class ArrowBlock(Block):
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

    def slice(self, start: int, end: int) -> "ArrowBlock[T]":
        return ArrowBlock(self._table.slice(start, end - start))

    def to_pandas(self) -> "pandas.DataFrame":
        return self._table.to_pandas()

    def num_rows(self) -> int:
        return self._table.num_rows

    def size_bytes(self) -> int:
        return self._table.nbytes

    @staticmethod
    def builder() -> ArrowBlockBuilder[T]:
        return ArrowBlockBuilder()
