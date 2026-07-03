"""File-affinity partitioner for DataSourceV2.

Groups each file's chunks into size-bounded partitions, preserving **file
locality**: a file is split into multiple partitions of consecutive
("sister") row-group chunks when its estimated in-memory size exceeds
``max_bucket_size``, giving read-task locality (one open + one footer read +
sequential I/O per file) plus sub-file parallelism for large files. By
default, multiple small files are additionally packed into a shared
partition so many tiny files don't each become their own read task -- see
the class docstring for details and the kill switch.
"""
import collections
from typing import Dict, Optional, Tuple

from ray._common.utils import env_bool
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
    """Coerce a file size to an int, mapping ``None`` and ``NaN`` to ``0``.

    File sizes can be ``None`` (e.g. ``HTTPFileSystem``); ``int(None)`` raises
    ``TypeError`` and ``int(NaN)`` raises ``ValueError`` (and ``NaN or 0`` stays
    ``NaN`` since ``NaN`` is truthy), so guard both here.
    """
    if value is None or value != value:  # ``value != value`` is True only for NaN
        return 0
    return int(value)


def _finite_float(value) -> float:
    """Coerce an in-memory size estimate to a float, mapping ``None``/``NaN`` to ``0.0``.

    Estimates are floats (e.g. on-disk size * encoding ratio); keep the
    fractional precision when accumulating them into ``_WeightedBucket.weight``
    rather than truncating each chunk to an int, which would make the bucket's
    running weight drift below the true total and flush late against
    ``max_bucket_size``.
    """
    if value is None or value != value:  # ``value != value`` is True only for NaN
        return 0.0
    return float(value)


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


def _flush_pending_pack(
    pending_pack: "_WeightedBucket[FileManifest]",
    output_queue: "collections.deque[FileManifest]",
) -> None:
    """Flush a pack of completed small-file manifests as one partition."""
    if pending_pack.items:
        output_queue.append(FileManifest.concat(pending_pack.items))
        pending_pack.clear()


class FileAffinityPartitioner(FilePartitioner):
    """Partitions chunks per file, bounded by ``max_bucket_size`` in-memory bytes.

    A file whose estimated in-memory size exceeds ``max_bucket_size`` is split
    into multiple partitions of consecutive row-group chunks -- a file's own
    chunks are never split across two *different* partitions unless the file
    alone exceeds the cap. ``num_buckets`` is intentionally absent -- the
    number of partitions is data-driven.

    By default (``RAY_DATA_PARTITIONER_PACK_FILES=1``), multiple small files
    (each individually under ``max_bucket_size``) are packed into a shared
    partition via next-fit bin-packing, so many tiny files don't each become
    their own read task. Disable via ``RAY_DATA_PARTITIONER_PACK_FILES=0`` for
    the pre-packing behavior: every partition holds chunks of exactly one file
    ("never mixed with other files").

    Groups by **path**. A single file's chunks always arrive contiguously in
    the input stream: the indexer yields one file's entire chunk list as one
    atomic unit through ``make_async_gen`` and the manifest batching preserves
    that order, so different files interleave only at file granularity, never
    chunk granularity (parallel footer reads may reorder *files* but never tear
    a file's chunk list apart). Contiguity lets a change of path mark the
    previous file complete: ``add_input`` flushes that file's bucket
    immediately (either standalone or into the pending pack), pipelining
    ``ReadFiles`` decoding with the listing task's remaining footer reads.
    ``finalize`` flushes the trailing open file and any pending pack, and
    preserves insertion (arrival) order for shuffle determinism.
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
        # Path of the file whose chunks are currently arriving. When a chunk
        # with a different path arrives, the previous file is complete (chunks
        # arrive contiguously -- see class docstring) and its bucket is flushed.
        # Instance state (not per-block) so a file straddling two manifest
        # blocks keeps accumulating into the same bucket.
        self._current_open_path: Optional[str] = None
        # Kill switch: when False, skip the per-file incremental flush and fall
        # back to flushing only at ``finalize`` (pre-pipelining behavior).
        self._pipeline_flush = env_bool("RAY_DATA_PARTITIONER_PIPELINE_FLUSH", True)
        # Kill switch: when False, every completed file emits its own
        # standalone partition (pre-packing behavior).
        self._pack_files = env_bool("RAY_DATA_PARTITIONER_PACK_FILES", True)
        # Completed small files (own weight < max_bucket_size) accumulate here
        # instead of emitting standalone, until the pack would overflow.
        self._pending_pack: "_WeightedBucket[FileManifest]" = _WeightedBucket()

    def _complete_file(self, bucket: "_WeightedBucket[_ChunkItem]") -> None:
        """Emit a fully-arrived file's bucket: standalone, or packed with others.

        A file that overflowed its own bucket (size-cap branch in ``add_input``)
        never reaches this method -- it self-emits directly. This method only
        sees files whose entire content fit under ``max_bucket_size`` on their
        own, so they're candidates for packing with other small files.
        """
        manifest = _bucket_to_manifest(bucket)
        if (
            not self._pack_files
            or self._max_bucket_size is None
            or bucket.weight >= self._max_bucket_size
        ):
            self._output_queue.append(manifest)
            return
        if self._pending_pack.weight + bucket.weight > self._max_bucket_size:
            _flush_pending_pack(self._pending_pack, self._output_queue)
        self._pending_pack.add(manifest, bucket.weight)

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
            # A change of path means the previous file is complete (its chunks
            # arrived contiguously): flush its still-open bucket now so
            # ReadFiles can decode it while later files' footers are still being
            # read. The size-cap overflow below may already have flushed and
            # removed that bucket, so ``pop`` with a default tolerates its
            # absence.
            if self._pipeline_flush and path != self._current_open_path:
                prev = self._current_open_path
                if prev is not None:
                    prev_bucket = self._open_buckets.pop(prev, None)
                    if prev_bucket is not None and prev_bucket.items:
                        self._complete_file(prev_bucket)
                self._current_open_path = path

            bucket = self._open_buckets.get(path)
            if bucket is None:
                bucket = _WeightedBucket()
                self._open_buckets[path] = bucket
            sort_key = _chunk_sort_key(chunk_metadata)
            bucket.add(
                (path, _finite_int(file_size), chunk_metadata, sort_key),
                _finite_float(in_memory_size),
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
        # Flush each file's remaining chunks in the order buckets were first
        # opened -- i.e. the order the input manifest arrived in. This must
        # NOT re-sort by path: an upstream shuffle (``shuffle_files``) already
        # produces a deterministic order (it sorts by path before permuting),
        # so re-sorting here would silently discard the permutation and
        # defeat any requested file shuffling. ``dict`` preserves insertion
        # order in Python 3.7+.
        for bucket in self._open_buckets.values():
            if bucket.items:
                self._complete_file(bucket)
        self._open_buckets.clear()
        self._current_open_path = None
        _flush_pending_pack(self._pending_pack, self._output_queue)
