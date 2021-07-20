import sys
from typing import Iterator, List, Generic, Any, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas
    import pyarrow

from ray.experimental.data.block import Block, T


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

    def sort_and_partition(self, boundaries: List[T],
                           key: Any) -> List["Block[T]"]:
        items = sorted(self._items, key=key)
        if len(boundaries) == 0:
            return SimpleBlock(items)
        parts = []
        bound_i = 0
        i = 0
        prev_i = 0
        part_offset = None
        N = len(items)
        while i < N and bound_i < len(boundaries):
            bound = boundaries[bound_i]
            while i < N and items[i] < bound:
                i += 1
            if part_offset is not None:
                parts.append((part_offset, i - prev_i))
            part_offset = i
            bound_i += 1
            prev_i = i
        if part_offset is not None:
            parts.append((part_offset, N - prev_i))
        ret = [
            SimpleBlock(items[offset:offset + count])
            for offset, count in parts
        ]
        num_empty = len(boundaries) - len(ret)
        ret.extend([SimpleBlock([])] * num_empty)
        return ret

    @staticmethod
    def merge_simple_blocks(blocks: List[Block[T]], key=Any) -> Block[T]:
        ret = [x for block in blocks for x in block._items]
        ret.sort(key=key)
        ret_block = SimpleBlock(ret)
        return ret_block, ret_block.get_metadata(None)
