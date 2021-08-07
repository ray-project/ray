import random
import sys
from typing import Callable, Iterator, List, Tuple, Union, Any, Optional, \
    TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas
    import pyarrow

from ray.data.impl.block_builder import BlockBuilder
from ray.data.block import Block, BlockAccessor, BlockMetadata, T

# A simple block can be sorted by value (None) or a lambda function (Callable).
SortKeyT = Union[None, Callable[[T], Any]]


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
    def __init__(self, items: List[T]):
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

    def random_shuffle(self, random_seed: Optional[int]) -> List[T]:
        random = np.random.RandomState(random_seed)
        items = self._items.copy()
        random.shuffle(items)
        return items

    def to_pandas(self) -> "pandas.DataFrame":
        import pandas
        return pandas.DataFrame(self._items)

    def to_arrow(self) -> "pyarrow.Table":
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

    def sample(self, n_samples: int = 1, key: SortKeyT = None) -> List[T]:
        if not callable(key) and key is not None:
            raise NotImplementedError(
                "Python sort key must be either None or a callable "
                "function, was: {}".format(key))
        k = min(n_samples, len(self._items))
        ret = random.sample(self._items, k)
        if key is None:
            return ret
        return [key(x) for x in ret]

    def sort_and_partition(self, boundaries: List[T], key: SortKeyT,
                           descending: bool) -> List["Block[T]"]:
        items = sorted(self._items, key=key, reverse=descending)
        if len(boundaries) == 0:
            return [items]

        # For each boundary value, count the number of items that are less
        # than it. Since the block is sorted, these counts partition the items
        # such that boundaries[i] <= x < boundaries[i + 1] for each x in
        # partition[i]. If `descending` is true, `boundaries` would also be
        # in descending order and we only need to count the number of items
        # *greater than* the boundary value instead.
        key_fn = key if key else lambda x: x
        comp_fn = lambda x, b: key_fn(x) > b \
            if descending else lambda x, b: key_fn(x) < b  # noqa E731
        boundary_indices = [
            len([1 for x in items if comp_fn(x, b)]) for b in boundaries
        ]
        ret = []
        prev_i = 0
        for i in boundary_indices:
            ret.append(items[prev_i:i])
            prev_i = i
        ret.append(items[prev_i:])
        return ret

    @staticmethod
    def merge_sorted_blocks(
            blocks: List[Block[T]], key: SortKeyT,
            descending: bool) -> Tuple[Block[T], BlockMetadata]:
        ret = [x for block in blocks for x in block]
        ret.sort(key=key, reverse=descending)
        return ret, SimpleBlockAccessor(ret).get_metadata(None)
