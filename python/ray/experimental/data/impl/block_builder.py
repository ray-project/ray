import sys
from typing import Iterator, List, Generic, Any, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas
    import pyarrow

from ray.experimental.data.block import Block, BlockAccessor, T


class BlockBuilder(Generic[T]):
    """A builder class for blocks."""

    def add(self, item: T) -> None:
        """Append a single row to the block being built."""
        raise NotImplementedError

    def add_block(self, block: Block) -> None:
        """Append an entire block to the block being built."""
        raise NotImplementedError

    def build(self) -> Block:
        """Build the block."""
        raise NotImplementedError


class SimpleBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._items = []

    def add(self, item: T) -> None:
        self._items.append(item)

    def add_block(self, block: List[T]) -> None:
        assert isinstance(block, list), block
        self._items.extend(block)

    def build(self) -> Block:
        return list(self._items)


class SimpleBlockAccessor(BlockAccessor):
    def __init__(self, items):
        self._items = items

    def num_rows(self) -> int:
        return len(self._items)

    def iter_rows(self) -> Iterator[T]:
        return iter(self._items)

    def slice(self, start: int, end: int,
              copy: bool) -> "SimpleBlockAccessor[T]":
        view = self._items[start:end]
        if copy:
            view = view.copy()
        return view

    def to_pandas(self) -> "pandas.DataFrame":
        import pandas
        return pandas.DataFrame(self._items)

    def to_arrow_table(self) -> "pyarrow.Table":
        import pyarrow
        return pyarrow.Table.from_pandas(self.to_pandas())

    def size_bytes(self) -> int:
        return sys.getsizeof(self._items)

    def schema(self) -> Any:
        if self._items:
            return type(self._items[0])
        else:
            return None

    @staticmethod
    def builder() -> SimpleBlockBuilder[T]:
        return SimpleBlockBuilder()
