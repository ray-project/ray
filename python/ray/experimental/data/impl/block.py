import sys
from typing import TypeVar, List, Generic, Any, Iterator, TYPE_CHECKING

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

    def to_pandas(self) -> "pandas.DataFrame":
        raise NotImplementedError

    def size_bytes(self) -> int:
        raise NotImplementedError

    def serialize(self) -> Any:
        raise NotImplementedError

    # TODO(ekl) should remove ser/de methods once ArrowBlock is natively
    # serializable in Ray.
    @staticmethod
    def deserialize(serialized: Any) -> "Block[T]":
        raise NotImplementedError

    @staticmethod
    def builder() -> BlockBuilder[T]:
        raise NotImplementedError


class ListBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._items = []

    def add(self, item: T) -> None:
        self._items.append(item)

    def build(self) -> "ListBlock[T]":
        return ListBlock(self._items)


class ListBlock(Block):
    def __init__(self, items):
        self._items = items

    def num_rows(self) -> int:
        return len(self._items)

    def iter_rows(self) -> Iterator[T]:
        return self._items.__iter__()

    def to_pandas(self) -> "pandas.DataFrame":
        import pandas
        return pandas.DataFrame(self._items)

    def size_bytes(self) -> int:
        return sys.getsizeof(self._items)

    def serialize(self) -> Any:
        return self

    @staticmethod
    def deserialize(value: Any) -> Any:
        return value

    @staticmethod
    def builder() -> ListBlockBuilder[T]:
        return ListBlockBuilder()
