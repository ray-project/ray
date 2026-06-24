"""File-affinity partitioner for DataSourceV2.

Groups each file's chunks into size-bounded partitions, preserving **file
locality**: every emitted partition holds chunks of exactly one file, and a
file is split into multiple partitions of consecutive ("sister") row-group
chunks when its estimated in-memory size exceeds ``max_bucket_size``. This
gives read-task locality (one file per task -> one open + one footer read +
sequential I/O) plus sub-file parallelism for large files.
"""
import collections
from typing import Dict, Optional, Tuple

from ray.data._internal.datasource_v2.chunkers.file_chunker import ChunkMetadata
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    InMemorySizeEstimator,
)
from ray.data._internal.weighted_round_robin import _WeightedBucket

# One accumulated chunk: (path, file_size, chunk_metadata, intra-file sort key).
_ChunkItem = Tuple[str, int, Optional[ChunkMetadata], int]


def _finite_int(value) -> int:
    """Coerce ``value`` to an int, mapping ``None`` and ``NaN`` to ``0``.

    File sizes can be ``None`` (e.g. ``HTTPFileSystem``) and in-memory size
    estimates can be ``NaN``; ``int(None)`` raises ``TypeError`` and ``int(NaN)``
    raises ``ValueError`` (and ``NaN or 0`` stays ``NaN`` since ``NaN`` is
    truthy), so guard both here.
    """
    if value is None or value != value:  # ``value != value`` is True only for NaN
        return 0
    return int(value)


def _chunk_sort_key(chunk_metadata: Optional[ChunkMetadata]) -> int:
    """Intra-file ordering key for a chunk, by metadata schema.

    Keeps a file's chunks in ascending on-disk order within its partition so the
    reader sees a clean ascending range: row-group index for Parquet, byte
    offset for line-delimited formats. Whole-file chunks (no positional
    metadata) fall back to ``0`` -- a stable sort then preserves input order.
    """
    if chunk_metadata is None:
        return 0
    # ``ChunkMetadata`` subclasses are ``TypedDict``s -- plain ``dict``s at
    # runtime with no distinct class -- so ``isinstance`` can't discriminate them
    # (it raises ``TypeError`` on a TypedDict). Discriminate on each schema's
    # positional key instead.
    if "row_group_start" in chunk_metadata:  # ParquetFileChunkMetadata
        return int(chunk_metadata["row_group_start"])
    if "chunk_byte_start_idx" in chunk_metadata:  # LineDelimitedFileChunkMetadata
        return int(chunk_metadata["chunk_byte_start_idx"])
    return 0


def _bucket_to_manifest(bucket: "_WeightedBucket[_ChunkItem]") -> FileManifest:
    # Sort by the intra-file key so each partition is a clean ascending range:
    # deterministic output (the ``FilePartitioner`` contract) and lets the reader
    # coalesce contiguous row groups into a single scan.
    items = sorted(bucket.items, key=lambda it: it[3])
    return FileManifest.construct_manifest(
        [it[0] for it in items],
        [it[1] for it in items],
        [it[2] for it in items],
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
        self._open_buckets: Dict[str, "_WeightedBucket[_ChunkItem]"] = {}
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
                bucket = _WeightedBucket()
                self._open_buckets[path] = bucket
            sort_key = _chunk_sort_key(chunk_metadata)
            bucket.add(
                (path, _finite_int(file_size), chunk_metadata, sort_key),
                _finite_int(in_memory_size),
            )
            # Flush this file's bucket once it reaches the size cap. Subsequent
            # chunks of the same file start a fresh bucket, so each partition is
            # a consecutive range of one file's row groups.
            if (
                self._max_bucket_size is not None
                and bucket.weight >= self._max_bucket_size
            ):
                self._output_queue.append(_bucket_to_manifest(bucket))
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
            if bucket.items:
                self._output_queue.append(_bucket_to_manifest(bucket))
        self._open_buckets.clear()
