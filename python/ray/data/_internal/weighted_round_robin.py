import collections
from dataclasses import dataclass, field
from typing import Generic, List, Optional, TypeVar

T = TypeVar("T")


@dataclass
class _WeightedBucket(Generic[T]):
    """A bucket of weighted items."""

    items: List[T] = field(default_factory=list)
    weight: int = 0

    def add(
        self,
        item: T,
        weight: int,
    ):
        self.items.append(item)
        self.weight += weight

    def clear(self):
        self.items.clear()
        self.weight = 0


class WeightedRoundRobinPartitioner(Generic[T]):
    """Partitions weighted items into round-robin buckets.

    Each item has an optional weight. If a weight is missing, the item is still
    spread round-robin but doesn't contribute to bucket fullness.
    """

    def __init__(
        self,
        *,
        min_bucket_size: int,
        max_bucket_size: int,
        num_buckets: int,
        emit_before_overflow: bool = False,
    ):
        self._num_buckets = max(1, num_buckets)
        self._min_bucket_size = min_bucket_size
        self._max_bucket_size = max_bucket_size
        self._emit_before_overflow = emit_before_overflow

        self._buckets = [_WeightedBucket[T]() for _ in range(self._num_buckets)]
        self._current_bucket_index = 0
        self._output_queue: collections.deque[List[T]] = collections.deque()

    def add_item(self, item: T, weight: Optional[int]) -> None:
        current_bucket = self._current_bucket

        # If a weight estimate isn't available, add the item to the current
        # bucket and move on. This spreads unknown-size items evenly across
        # buckets without pretending to know their size.
        if weight is None:
            current_bucket.add(item, 0)
            self._advance_bucket()
            return

        weight = max(0, int(weight))
        while (
            self._emit_before_overflow
            and current_bucket.items
            and current_bucket.weight + weight > self._max_bucket_size
        ):
            self._emit_current_bucket()
            current_bucket = self._current_bucket

        current_bucket.add(item, weight)
        if current_bucket.weight >= self._max_bucket_size:
            self._emit_current_bucket()
        elif current_bucket.weight >= self._min_bucket_size:
            self._advance_bucket()

    def has_partition(self) -> bool:
        return len(self._output_queue) > 0

    def next_partition(self) -> List[T]:
        return self._output_queue.popleft()

    def finalize(self):
        for bucket in self._buckets:
            if bucket.items:
                self._output_queue.append(list(bucket.items))
                bucket.clear()

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    @property
    def _current_bucket(self) -> _WeightedBucket[T]:
        return self._buckets[self._current_bucket_index]

    def _advance_bucket(self):
        self._current_bucket_index = (
            self._current_bucket_index + 1
        ) % self._num_buckets

    def _emit_current_bucket(self):
        current_bucket = self._current_bucket
        self._output_queue.append(list(current_bucket.items))
        current_bucket.clear()
        self._advance_bucket()
