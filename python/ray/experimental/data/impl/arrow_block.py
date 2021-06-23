import collections
from typing import Iterator, TypeVar, TYPE_CHECKING

import pyarrow as pa

from ray.experimental.data.impl.block import Block, BlockBuilder

if TYPE_CHECKING:
    import pandas

T = TypeVar("T")


class ArrowBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._columns = collections.defaultdict(list)

    def add(self, item: dict) -> None:
        if not isinstance(item, dict):
            raise ValueError(
                "Returned elements of an ArrowBlock must be of type `dict`, "
                "got {} (type {}).".format(item, type(item)))
        for key, value in item.items():
            self._columns[key].append(value)

    def build(self) -> "ArrowBlock[T]":
        return ArrowBlock(pa.Table.from_pydict(self._columns))


class ArrowBlock(Block):
    def __init__(self, table: pa.Table):
        self._table = table

    def iter_rows(self) -> Iterator[T]:
        outer = self

        class Iter:
            def __init__(self):
                self._cur = -1

            def __iter__(self):
                return self

            def __next__(self):
                self._cur += 1
                if self._cur < outer._table.num_rows:
                    row = outer._table.slice(self._cur, 1).to_pydict()
                    return {k: v[0] for k, v in row.items()}
                raise StopIteration

        return Iter()

    def to_pandas(self) -> "pandas.DataFrame":
        return self._table.to_pandas()

    def num_rows(self) -> int:
        return self._table.num_rows

    def size_bytes(self) -> int:
        return self._table.nbytes

    def serialize(self) -> dict:
        return self._table.to_pydict()

    @staticmethod
    def deserialize(value: dict) -> "ArrowBlock[T]":
        return ArrowBlock(pa.Table.from_pydict(value))

    @staticmethod
    def builder() -> ArrowBlockBuilder[T]:
        return ArrowBlockBuilder()
