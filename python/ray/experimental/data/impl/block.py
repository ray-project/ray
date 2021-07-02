import sys
from typing import TypeVar, List, Generic, Iterator, Any, Union, Optional, \
    TYPE_CHECKING

if TYPE_CHECKING:
    import pandas
    import pyarrow

# TODO(ekl) shouldn't Ray provide an ObjectRef type natively?
ObjectRef = List
T = TypeVar("T")


class BlockBuilder(Generic[T]):
    def add(self, item: T) -> None:
        raise NotImplementedError

    def build(self) -> "Block[T]":
        raise NotImplementedError


class BlockMetadata:
    def __init__(self, *, num_rows: Optional[int], size_bytes: Optional[int],
                 schema: Union[type, "pyarrow.lib.Schema"],
                 input_files: List[str]):
        if input_files is None:
            input_files = []
        self.num_rows: Optional[int] = num_rows
        self.size_bytes: Optional[int] = size_bytes
        self.schema: Optional[Any] = schema
        self.input_files: List[str] = input_files


class Block(Generic[T]):
    def num_rows(self) -> int:
        raise NotImplementedError

    def iter_rows(self) -> Iterator[T]:
        raise NotImplementedError

    def slice(self, start: int, end: int, copy: bool) -> "Block[T]":
        raise NotImplementedError

    def to_pandas(self) -> "pandas.DataFrame":
        raise NotImplementedError

    def size_bytes(self) -> int:
        raise NotImplementedError

    def schema(self) -> Any:
        raise NotImplementedError

    def get_metadata(self, input_files: List[str]) -> BlockMetadata:
        return BlockMetadata(
            num_rows=self.num_rows(),
            size_bytes=self.size_bytes(),
            schema=self.schema(),
            input_files=input_files)

    @staticmethod
    def builder() -> BlockBuilder[T]:
        raise NotImplementedError


class SimpleBlockBuilder(BlockBuilder[T]):
    def __init__(self):
        self._items = []

    def add(self, item: T) -> None:
        self._items.append(item)

    def add_block(self, block: "SimpleBlock[T]") -> None:
        self._items.extend(block._items)

    def build(self) -> "SimpleBlock[T]":
        return SimpleBlock(self._items)


class SimpleBlock(Block):
    def __init__(self, items):
        self._items = items

    def num_rows(self) -> int:
        return len(self._items)

    def iter_rows(self) -> Iterator[T]:
        return iter(self._items)

    def slice(self, start: int, end: int, copy: bool) -> "SimpleBlock[T]":
        view = self._items[start:end]
        if copy:
            view = view.copy()
        return SimpleBlock(view)

    def to_pandas(self) -> "pandas.DataFrame":
        import pandas
        return pandas.DataFrame(self._items)

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
