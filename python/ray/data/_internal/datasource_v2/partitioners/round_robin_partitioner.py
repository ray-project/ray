import logging

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    InMemorySizeEstimator,
)
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner

logger = logging.getLogger(__name__)


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

    This algorithm ensures that each block contains [min_bucket_size, max_bucket_size]
    worth of files.  It's a deterministic algorithm, but it doesn't maintain the order
    of the input paths.
    """

    def __init__(
        self,
        in_memory_size_estimator: InMemorySizeEstimator,
        *,
        min_bucket_size: int,
        max_bucket_size: int,
        num_buckets: int,
    ):
        self._in_memory_size_estimator = in_memory_size_estimator
        self._partitioner = WeightedRoundRobinPartitioner(
            min_bucket_size=min_bucket_size,
            max_bucket_size=max_bucket_size,
            num_buckets=num_buckets,
        )

    def add_input(self, input_manifest: FileManifest):
        in_memory_size_estimates = (
            self._in_memory_size_estimator.estimate_in_memory_sizes(input_manifest)
        )
        for (file_path, file_size, in_memory_size_estimate,) in zip(
            input_manifest.paths,
            input_manifest.file_sizes,
            in_memory_size_estimates,
        ):
            self._partitioner.add_item(
                (file_path, file_size),
                in_memory_size_estimate,
            )

    def has_partition(self) -> bool:
        return self._partitioner.has_partition()

    def next_partition(self) -> FileManifest:
        partition = self._partitioner.next_partition()
        paths, file_sizes = zip(*partition)
        return FileManifest.construct_manifest(list(paths), list(file_sizes))

    def finalize(self):
        self._partitioner.finalize()
