import logging
import math
from collections import deque

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    InMemorySizeEstimator,
)
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner

logger = logging.getLogger(__name__)

_DEFAULT_MAX_ITEMS_PER_BUCKET = 10_000


class RoundRobinPartitioner(FilePartitioner):
    """Partitions input paths into blocks based on the in-memory size of files.

    This partitioning ensures read tasks effectively utilize the cluster and
    produce appropriately-sized blocks

    **Steps:**
        1. Initialize empty buckets.
        2. Iterate through input blocks and add paths to buckets. For each path:
            - If the current bucket falls below `min_bucket_size`, add the path and don't move
              to the next bucket.
            - If the current bucket exceeds `min_bucket_size` but not `max_bucket_size`,
              add the path and move to the next bucket.
            - If the current bucket exceeds `max_bucket_size`, yield the paths as a block, clear
              the bucket, and move to the next bucket.
        3. Yield any remaining paths in the buckets as blocks.

    By default, this algorithm balances work across buckets and doesn't maintain
    input order. With ``preserve_order=True``, it instead emits contiguous,
    size-bounded partitions in input order. With ``enforce_num_buckets=True``,
    it globally targets that many contiguous partitions; safety limits may
    produce additional partitions.
    """

    def __init__(
        self,
        in_memory_size_estimator: InMemorySizeEstimator,
        *,
        min_bucket_size: int,
        max_bucket_size: int,
        num_buckets: int,
        preserve_order: bool = False,
        enforce_num_buckets: bool = False,
        max_items_per_bucket: int = _DEFAULT_MAX_ITEMS_PER_BUCKET,
    ):
        self._in_memory_size_estimator = in_memory_size_estimator
        self._num_buckets = max(1, num_buckets)
        self._enforce_num_buckets = enforce_num_buckets
        if enforce_num_buckets:
            # An exact global bucket target requires seeing the complete input.
            # Safety limits can still force additional partitions so one read-task
            # argument can't contain an unreasonable number of manifest rows.
            self._partitioner = _ContiguousTargetPartitioner(
                max_bucket_size=max_bucket_size,
                num_buckets=self._num_buckets,
                max_items_per_bucket=max_items_per_bucket,
            )
        elif preserve_order:
            self._partitioner = _SequentialPartitioner(
                max_bucket_size=max_bucket_size,
                max_items_per_bucket=max_items_per_bucket,
            )
        else:
            self._partitioner = WeightedRoundRobinPartitioner(
                min_bucket_size=min_bucket_size,
                max_bucket_size=max_bucket_size,
                num_buckets=self._num_buckets,
                max_items_per_bucket=max_items_per_bucket,
            )

    @property
    def requires_global_input(self) -> bool:
        return self._enforce_num_buckets

    def add_input(self, input_manifest: FileManifest):
        in_memory_size_estimates = (
            self._in_memory_size_estimator.estimate_in_memory_sizes(input_manifest)
        )
        for (
            file_path,
            file_size,
            file_chunk_metadata,
            in_memory_size_estimate,
        ) in zip(
            input_manifest.paths,
            input_manifest.file_sizes,
            input_manifest.file_chunk_metadatas,
            in_memory_size_estimates,
        ):
            self._partitioner.add_item(
                (file_path, file_size, file_chunk_metadata),
                in_memory_size_estimate,
            )

    def has_partition(self) -> bool:
        return self._partitioner.has_partition()

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    def next_partition(self) -> FileManifest:
        if self._enforce_num_buckets:
            partition, split_factor = self._partitioner.next_partition()
        else:
            partition = self._partitioner.next_partition()
            split_factor = 1
        paths, file_sizes, file_chunk_metadatas = zip(*partition)
        manifest = FileManifest.construct_manifest(
            paths=list(paths),
            sizes=list(file_sizes),
            chunk_metadatas=list(file_chunk_metadatas),
        )
        return (
            manifest.with_output_split_factor(split_factor)
            if split_factor > 1
            else manifest
        )

    def finalize(self):
        self._partitioner.finalize()


class _SizeBoundedBucket:
    """Accumulate items until a byte or item-count limit is reached."""

    def __init__(self, *, max_bucket_size: int, max_items_per_bucket: int):
        if max_items_per_bucket < 1:
            raise ValueError("max_items_per_bucket must be at least 1")
        self._max_bucket_size = max_bucket_size
        self._max_items_per_bucket = max_items_per_bucket
        self.items = []
        self.weight = 0.0

    def __len__(self) -> int:
        return len(self.items)

    def would_overflow(self, weight: float) -> bool:
        """Whether ``weight`` no longer fits and the bucket should flush first."""
        return bool(self.items) and (
            self.weight + weight > self._max_bucket_size
            or len(self.items) >= self._max_items_per_bucket
        )

    def add(self, item, weight: float) -> None:
        self.items.append(item)
        self.weight += weight

    def is_full(self) -> bool:
        return (
            self.weight >= self._max_bucket_size
            or len(self.items) >= self._max_items_per_bucket
        )

    def drain(self) -> list:
        items = self.items
        self.items = []
        self.weight = 0.0
        return items


class _SequentialPartitioner:
    """Size-bound contiguous partitions for order-preserving reads."""

    def __init__(self, *, max_bucket_size: int, max_items_per_bucket: int):
        self._bucket = _SizeBoundedBucket(
            max_bucket_size=max_bucket_size,
            max_items_per_bucket=max_items_per_bucket,
        )
        self._output_queue = deque()

    def add_item(self, item, weight) -> None:
        # An unknown weight can't safely share a size-bounded bucket. Emitting
        # it alone also prevents an unbounded list of unknown-size files from
        # accumulating while preserving their input order.
        if weight is None:
            self._flush()
            self._output_queue.append([item])
            return

        weight = max(0.0, weight)
        if self._bucket.would_overflow(weight):
            self._flush()
        self._bucket.add(item, weight)
        if self._bucket.is_full():
            self._flush()

    def has_partition(self) -> bool:
        return bool(self._output_queue)

    def next_partition(self):
        return self._output_queue.popleft()

    def finalize(self) -> None:
        self._flush()

    def _flush(self) -> None:
        if self._bucket:
            self._output_queue.append(self._bucket.drain())


class _ContiguousTargetPartitioner:
    """Globally target a number of contiguous, size-safe partitions.

    Inputs are retained so that, at finalization, they can be split like
    ``numpy.array_split`` into the requested number of contiguous read units.
    Byte and item-count limits take priority over the target: a bucket that
    reaches either limit is emitted as a "safety" partition right away. That
    bounds retained state to at most two buckets (``2 * max_items_per_bucket``
    manifest rows) no matter how many files are listed, so a block-count
    override never turns listing into an unbounded accumulator. The trade-off
    is that the target is met exactly only when the whole input fits within
    it; once safety partitions alone reach the target, the rest of the input
    streams out one bucket at a time and a message is logged, because the
    override can't be honored without producing oversized read tasks.
    """

    def __init__(
        self,
        *,
        max_bucket_size: int,
        num_buckets: int,
        max_items_per_bucket: int,
    ):
        self._bucket = _SizeBoundedBucket(
            max_bucket_size=max_bucket_size,
            max_items_per_bucket=max_items_per_bucket,
        )
        self._num_buckets = max(1, num_buckets)
        self._num_safety_partitions = 0
        self._pending_safety_partition = None
        self._output_queue = deque()

    def add_item(self, item, weight) -> None:
        normalized_weight = 0.0 if weight is None else max(0.0, weight)
        if self._bucket.would_overflow(normalized_weight):
            self._flush_safety_partition()
        self._bucket.add(item, normalized_weight)
        if self._bucket.is_full():
            self._flush_safety_partition()

    def has_partition(self) -> bool:
        return bool(self._output_queue)

    def next_partition(self):
        return self._output_queue.popleft()

    def finalize(self) -> None:
        items = self._bucket.drain()
        if self._pending_safety_partition is not None:
            if items:
                self._emit_safety_partition(self._pending_safety_partition)
            else:
                items = self._pending_safety_partition
            self._pending_safety_partition = None
        if not items:
            return

        remaining_target = max(1, self._num_buckets - self._num_safety_partitions)
        num_partitions = min(remaining_target, len(items))
        # If there are fewer read units than requested output blocks, let the
        # reader split its size-bounded output, just as V1 does. This doesn't
        # require reading or counting rows during listing.
        split_factor = math.ceil(remaining_target / num_partitions)
        start = 0
        for partition_index in range(num_partitions):
            items_left = len(items) - start
            partitions_left = num_partitions - partition_index
            partition_size = math.ceil(items_left / partitions_left)
            end = start + partition_size
            self._output_queue.append((items[start:end], split_factor))
            start = end

    def _flush_safety_partition(self) -> None:
        if not self._bucket:
            return
        # Keep at most one completed partition pending. At EOF it can carry
        # any remaining output split target, even when the last input exactly
        # filled a bucket. Earlier partitions continue streaming normally.
        if self._pending_safety_partition is not None:
            self._emit_safety_partition(self._pending_safety_partition)
        self._pending_safety_partition = self._bucket.drain()

    def _emit_safety_partition(self, partition: list) -> None:
        self._output_queue.append((partition, 1))
        self._num_safety_partitions += 1
        if self._num_safety_partitions == self._num_buckets:
            logger.info(
                "The requested number of read blocks (%d) can't be honored "
                "exactly: byte and row limits already produced that many read "
                "units, so the remaining files stream out in size-bounded "
                "partitions instead.",
                self._num_buckets,
            )
