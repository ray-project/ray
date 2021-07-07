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
    """A builder class for blocks."""

    def add(self, item: T) -> None:
        """Append a single row to the block being built."""
        raise NotImplementedError

    def add_block(self, block: "Block[T]") -> None:
        """Append an entire block to the block being built."""
        raise NotImplementedError

    def build(self) -> "Block[T]":
        """Build the block."""
        raise NotImplementedError


class BlockMetadata:
    """Metadata about the block.

    Attributes:
        num_rows: The number of rows contained in this block, or None.
        size_bytes: The approximate size in bytes of this block, or None.
        schema: The pyarrow schema or types of the block elements, or None.
        input_files: The list of file paths used to generate this block, or
            the empty list if indeterminate.
    """

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
    """Represents a batch of rows to be stored in the Ray object store.

    There are two types of blocks: ``SimpleBlock``, which is backed by a plain
    Python list, and ``ArrowBlock``, which is backed by a ``pyarrow.Table``.
    """

    def num_rows(self) -> int:
        """Return the number of rows contained in this block."""
        raise NotImplementedError

    def iter_rows(self) -> Iterator[T]:
        """Iterate over the rows of this block."""
        raise NotImplementedError

    def slice(self, start: int, end: int, copy: bool) -> "Block[T]":
        """Return a slice of this block.

        Args:
            start: The starting index of the slice.
            end: The ending index of the slice.
            copy: Whether to perform a data copy for the slice.

        Returns:
            The sliced block result.
        """
        raise NotImplementedError

    def to_pandas(self) -> "pandas.DataFrame":
        """Convert this block into a Pandas dataframe."""
        raise NotImplementedError

    def to_arrow_table(self) -> "pyarrow.Table":
        """Convert this block into an Arrow table."""
        raise NotImplementedError

    def size_bytes(self) -> int:
        """Return the approximate size in bytes of this block."""
        raise NotImplementedError

    def schema(self) -> Union[type, "pyarrow.lib.Schema"]:
        """Return the Python type or pyarrow schema of this block."""
        raise NotImplementedError

    def get_metadata(self, input_files: List[str]) -> BlockMetadata:
        """Create a metadata object from this block."""
        return BlockMetadata(
            num_rows=self.num_rows(),
            size_bytes=self.size_bytes(),
            schema=self.schema(),
            input_files=input_files)

    @staticmethod
    def builder() -> BlockBuilder[T]:
        """Create a builder for this block type."""
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
