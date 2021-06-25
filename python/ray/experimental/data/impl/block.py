import sys
from typing import TypeVar, List, Generic, Iterator, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas

# TODO(ekl) shouldn't Ray provide an ObjectRef type natively?
ObjectRef = List
T = TypeVar("T")


class BlockBuilder(Generic[T]):
    def add(self, item: T) -> None:
        raise NotImplementedError

    def build(self) -> "Block[T]":
        raise NotImplementedError


class Block(Generic[T]):
    def num_rows(self) -> int:
        raise NotImplementedError

    def iter_rows(self) -> Iterator[T]:
        raise NotImplementedError

    def slice(self, start: int, end: int) -> "Block[T]":
        raise NotImplementedError

    def to_pandas(self) -> "pandas.DataFrame":
        raise NotImplementedError

    def size_bytes(self) -> int:
        raise NotImplementedError

    @staticmethod
    def builder() -> BlockBuilder[T]:
        raise NotImplementedError


class ListBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._items = []

    def add(self, item: T) -> None:
        self._items.append(item)

    def add_block(self, block: "ListBlock[T]") -> None:
        self._items.extend(block._items)

    def build(self) -> "ListBlock[T]":
        return ListBlock(self._items)


class ListBlock(Block):
    def __init__(self, items):
        self._items = items

    def num_rows(self) -> int:
        return len(self._items)

    def iter_rows(self) -> Iterator[T]:
        return self._items.__iter__()

    def slice(self, start: int, end: int) -> "ListBlock[T]":
        return ListBlock(self._items[start:end])

    def to_pandas(self) -> "pandas.DataFrame":
        import pandas
        return pandas.DataFrame(self._items)

    def size_bytes(self) -> int:
        return sys.getsizeof(self._items)

    @staticmethod
    def builder() -> ListBlockBuilder[T]:
        return ListBlockBuilder()
