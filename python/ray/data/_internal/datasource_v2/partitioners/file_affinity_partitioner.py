"""File-affinity partitioner for DataSourceV2.

Groups each file's chunks into size-bounded partitions, preserving **file
locality**: every emitted partition holds chunks of exactly one file, and a
file is split into multiple partitions of consecutive ("sister") row-group
chunks when its estimated in-memory size exceeds ``max_bucket_size``. This
gives read-task locality (one file per task -> one open + one footer read +
sequential I/O) plus sub-file parallelism for large files.

Contrast with :class:`RoundRobinPartitioner`, which spreads a file's chunks
across buckets and so can mix multiple files within a single partition.
"""
import collections
from typing import Dict, List, Optional

from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    InMemorySizeEstimator,
)


class _FileBucket:
    """Accumulates one file's chunks until it is flushed as a partition."""

    def __init__(self):
        self.paths: List[str] = []
        self.file_sizes: List[int] = []
        self.chunk_metadatas: List[Optional[dict]] = []
        self.sort_keys: List[int] = []
        self.in_memory_size: int = 0

    def add(self, path, file_size, chunk_metadata, in_memory_size, sort_key):
        self.paths.append(path)
        self.file_sizes.append(file_size)
        self.chunk_metadatas.append(chunk_metadata)
        self.sort_keys.append(sort_key)
        self.in_memory_size += in_memory_size

    def to_manifest(self) -> FileManifest:
        # Sort by row-group start so each partition is a clean ascending range:
        # deterministic output (the ``FilePartitioner`` contract) and lets the
        # reader coalesce contiguous row groups into a single scan.
        order = sorted(range(len(self.paths)), key=lambda i: self.sort_keys[i])
        return FileManifest.construct_manifest(
            [self.paths[i] for i in order],
            [self.file_sizes[i] for i in order],
            [self.chunk_metadatas[i] for i in order],
        )


class FileAffinityPartitioner(FilePartitioner):
    """Partitions chunks per file, bounded by ``max_bucket_size`` in-memory bytes.

    All chunks of a file go to partitions of that file only (never mixed with
    other files); a file whose estimated in-memory size exceeds
    ``max_bucket_size`` is split into multiple partitions of consecutive
    row-group chunks. ``num_buckets`` is intentionally absent -- the number of
    partitions is data-driven: ``sum over files of
    ceil(file_in_memory_size / max_bucket_size)``.

    Groups by **path** (not arrival position): the indexer may interleave
    different files' chunks via parallel footer reads, so contiguity in the
    input stream is not assumed.
    """

    def __init__(
        self,
        in_memory_size_estimator: InMemorySizeEstimator,
        *,
        max_bucket_size: Optional[int],
    ):
        self._in_memory_size_estimator = in_memory_size_estimator
        self._max_bucket_size = max_bucket_size
        # path -> bucket currently accumulating that file's chunks.
        self._open_buckets: Dict[str, _FileBucket] = {}
        self._output_queue: "collections.deque[FileManifest]" = collections.deque()

    def add_input(self, input_manifest: FileManifest):
        in_memory_sizes = self._in_memory_size_estimator.estimate_in_memory_sizes(
            input_manifest
        )
        for path, file_size, chunk_metadata, in_memory_size in zip(
            input_manifest.paths,
            input_manifest.file_sizes,
            input_manifest.file_chunk_metadatas,
            in_memory_sizes,
        ):
            bucket = self._open_buckets.get(path)
            if bucket is None:
                bucket = _FileBucket()
                self._open_buckets[path] = bucket
            sort_key = (
                int(chunk_metadata["row_group_start"])
                if chunk_metadata is not None and "row_group_start" in chunk_metadata
                else 0
            )
            bucket.add(
                path, int(file_size), chunk_metadata, int(in_memory_size or 0), sort_key
            )
            # Flush this file's bucket once it reaches the size cap. Subsequent
            # chunks of the same file start a fresh bucket, so each partition is
            # a consecutive range of one file's row groups.
            if (
                self._max_bucket_size is not None
                and bucket.in_memory_size >= self._max_bucket_size
            ):
                self._output_queue.append(bucket.to_manifest())
                del self._open_buckets[path]

    def has_partition(self) -> bool:
        return len(self._output_queue) > 0

    def next_partition(self) -> FileManifest:
        return self._output_queue.popleft()

    def finalize(self):
        # Flush each file's remaining chunks; sort by path for deterministic
        # output across retries.
        for path in sorted(self._open_buckets.keys()):
            bucket = self._open_buckets[path]
            if bucket.paths:
                self._output_queue.append(bucket.to_manifest())
        self._open_buckets.clear()
