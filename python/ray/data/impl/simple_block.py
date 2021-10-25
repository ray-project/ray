import random
import sys
import heapq
from typing import Callable, Iterator, List, Tuple, Union, Any, Optional, \
    TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    import pandas
    import pyarrow

from ray.data.impl.block_builder import BlockBuilder
from ray.data.block import Block, BlockAccessor, BlockMetadata, \
    T, U, KeyType, AggType

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

    def to_numpy(self, column: str = None) -> np.ndarray:
        if column:
            raise ValueError("`column` arg not supported for list block")
        return np.array(self._items)

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

    def zip(self, other: "Block[T]") -> "Block[T]":
        if not isinstance(other, list):
            raise ValueError("Cannot zip {} with block of type {}".format(
                type(self), type(other)))
        if len(other) != len(self._items):
            raise ValueError(
                "Cannot zip self (length {}) with block of length {}".format(
                    len(self), len(other)))
        return list(zip(self._items, other))

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

        # Compute the boundary indices in O(n) time via scan.
        boundary_indices = []
        remaining = boundaries.copy()
        for i, x in enumerate(items):
            while remaining and not comp_fn(x, remaining[0]):
                remaining.pop(0)
                boundary_indices.append(i)
        for _ in remaining:
            boundary_indices.append(len(items))
        assert len(boundary_indices) == len(boundaries)

        ret = []
        prev_i = 0
        for i in boundary_indices:
            ret.append(items[prev_i:i])
            prev_i = i
        ret.append(items[prev_i:])
        return ret

    def combine(self, key: Callable[[T], KeyType],
                init: Callable[[KeyType], AggType],
                accumulate: Callable[[KeyType, AggType, T], AggType]
                ) -> Block[Tuple[KeyType, AggType]]:
        """Combine rows with the same key into an accumulator.

        This assumes the block is already sorted by key in ascending order.

        Args:
            key: The key function that returns the key from the row.
            init: This is called once for each key to
                return the empty accumulator. For example, an empty accumulator
                for a sum would be 0.
            accumulate: This is called once per row of the same key.
                This combines the accumulator and the row,
                returns the updated accumulator.

        Returns:
            A sorted block of (k, v) pairs where k is the groupby key
            and v is the partially combined accumulator.
        """
        iter = self.iter_rows()
        next_row = None
        # Use a bool to indicate if next_row is valid
        # instead of checking if next_row is None
        # since a row can have None value.
        has_next_row = False
        ret = []
        while True:
            try:
                if not has_next_row:
                    next_row = next(iter)
                    has_next_row = True
                next_key = key(next_row)

                def gen():
                    nonlocal iter
                    nonlocal next_row
                    nonlocal has_next_row
                    assert has_next_row
                    while key(next_row) == next_key:
                        yield next_row
                        try:
                            next_row = next(iter)
                        except StopIteration:
                            has_next_row = False
                            next_row = None
                            break

                aggregator = init(next_key)
                for r in gen():
                    aggregator = accumulate(next_key, aggregator, r)
                ret.append((next_key, aggregator))
            except StopIteration:
                break
        return ret

    @staticmethod
    def merge_sorted_blocks(
            blocks: List[Block[T]], key: SortKeyT,
            descending: bool) -> Tuple[Block[T], BlockMetadata]:
        ret = [x for block in blocks for x in block]
        ret.sort(key=key, reverse=descending)
        return ret, SimpleBlockAccessor(ret).get_metadata(None)

    @staticmethod
    def aggregate_combined_blocks(
            blocks: List[Block[Tuple[KeyType, AggType]]],
            merge: Callable[[KeyType, AggType, AggType], AggType],
            finalize: Callable[[KeyType, AggType], U]
    ) -> Tuple[Block[Tuple[KeyType, U]], BlockMetadata]:
        """Aggregate sorted, partially combined blocks with the same key range.

        This assumes blocks are already sorted by key in ascending order,
        so we can do merge sort to get all the rows with the same key.

        Args:
            blocks: A list of partially combined and sorted blocks.
            merge: This may be called multiple times, each time to merge
                two accumulators into one.
            finalize: This is called once to compute the final
                aggregation result from the fully merged accumulator.

        Returns:
            A block of (k, v) pairs and its metadata where k is the groupby key
            and v is the corresponding aggregation result.
        """

        def key(r):
            return r[0]

        iter = heapq.merge(
            *[SimpleBlockAccessor(block).iter_rows() for block in blocks],
            key=key)
        next_row = None
        has_next_row = False
        ret = []
        while True:
            try:
                if not has_next_row:
                    next_row = next(iter)
                    has_next_row = True
                next_key = key(next_row)

                def gen():
                    nonlocal iter
                    nonlocal next_row
                    nonlocal has_next_row
                    assert has_next_row
                    while key(next_row) == next_key:
                        yield next_row
                        try:
                            next_row = next(iter)
                        except StopIteration:
                            has_next_row = False
                            next_row = None
                            break

                first = True
                aggregator = None
                for r in gen():
                    if first:
                        aggregator = r[1]
                        first = False
                    else:
                        aggregator = merge(next_key, aggregator, r[1])
                ret.append((next_key, finalize(next_key, aggregator)))
            except StopIteration:
                break

        return ret, SimpleBlockAccessor(ret).get_metadata(None)
