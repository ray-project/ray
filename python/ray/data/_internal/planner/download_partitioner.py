from typing import Callable, Dict, Iterator, List, Optional, Protocol, Tuple

import numpy as np
import pyarrow as pa

from ray._common.utils import env_integer
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner

URI_METADATA_CHUNK_SIZE = max(
    1, env_integer("RAY_DATA_DOWNLOAD_METADATA_CHUNK_SIZE", 1024)
)
URI_PARTITION_NUM_BUCKETS = max(
    1, env_integer("RAY_DATA_DOWNLOAD_PARTITION_NUM_BUCKETS", 2)
)

UriColumnName = str
FileSizeBytes = int

SizesByColumn = Dict[UriColumnName, List[FileSizeBytes]]

SizeAnnotator = Callable[[pa.Table, SizesByColumn], pa.Table]
MetadataPredicate = Callable[[pa.Table], bool]


class _FileSizeProvider(Protocol):
    def get_file_sizes(self, uris: List[str]) -> List[Optional[int]]:
        ...


class _ExactDownloadPartitioner:
    """Partitions download blocks using exact file sizes.

    Metadata is fetched in bounded chunks. After each chunk is sized, rows are
    added to weighted round-robin buckets and emitted as buckets fill so the
    downstream download operator can overlap downloading with later metadata
    fetches.
    """

    def __init__(
        self,
        uri_column_names: List[str],
        target_nbytes: Optional[int],
        file_size_provider: _FileSizeProvider,
        *,
        annotate_sizes: Optional[SizeAnnotator] = None,
        fetch_metadata_without_target: Optional[MetadataPredicate] = None,
        target_min_nbytes: Optional[int] = None,
        num_buckets: int = URI_PARTITION_NUM_BUCKETS,
        metadata_chunk_size: int = URI_METADATA_CHUNK_SIZE,
    ):
        self._uri_column_names = uri_column_names
        self._target_nbytes = target_nbytes
        self._file_size_provider = file_size_provider
        self._annotate_sizes = annotate_sizes
        self._fetch_metadata_without_target = fetch_metadata_without_target
        self._target_min_nbytes = target_min_nbytes
        self._num_buckets = max(1, num_buckets)
        self._metadata_chunk_size = max(1, metadata_chunk_size)

    def partition(self, block: pa.Table) -> Iterator[pa.Table]:
        if block.num_rows == 0:
            yield block
            return

        if (
            self._target_nbytes is None
            and not self._should_fetch_metadata_without_target(block)
        ):
            yield block
            return

        # When there's no target size, every row stays in a single partition (the
        # whole block), so we skip the round-robin partitioner and just fetch
        # sizes to annotate. ``row_partitioner is None`` flags that mode below.
        row_partitioner: Optional[WeightedRoundRobinPartitioner[int]] = None
        if self._target_nbytes is not None:
            target_nbytes = max(1, self._target_nbytes)
            row_partitioner = WeightedRoundRobinPartitioner[int](
                min_bucket_size=self._min_partition_nbytes(target_nbytes),
                max_bucket_size=target_nbytes,
                num_buckets=self._num_buckets,
                emit_before_overflow=True,
            )
        # Full-block per-column size arrays, filled chunk by chunk. Row index is
        # the absolute block position, so these are indexed directly when
        # reconstructing per-partition sizes in ``_drain_row_partitions``.
        sizes_by_column: Dict[UriColumnName, np.ndarray] = {
            uri_column_name: np.zeros(block.num_rows, dtype=np.int64)
            for uri_column_name in self._uri_column_names
        }

        for chunk_start, chunk_end, chunk_sizes_by_column in self._iter_chunk_sizes(
            block
        ):
            # Vectorize the per-row totals: write each column's sizes into the
            # full array and accumulate the row sum in one pass per column.
            row_nbytes = np.zeros(chunk_end - chunk_start, dtype=np.int64)
            for uri_column_name in self._uri_column_names:
                column_sizes = chunk_sizes_by_column[uri_column_name]
                sizes_by_column[uri_column_name][chunk_start:chunk_end] = column_sizes
                row_nbytes += column_sizes

            if row_partitioner is None:
                # No target size: only accumulate full-block sizes; the block is
                # emitted whole, annotated, after the loop.
                continue

            row_nbytes_list = row_nbytes.tolist()
            for chunk_row in range(chunk_end - chunk_start):
                row_partitioner.add_item(
                    chunk_start + chunk_row,
                    row_nbytes_list[chunk_row] or None,
                )
                yield from self._drain_row_partitions(
                    block,
                    row_partitioner,
                    sizes_by_column,
                )

        if row_partitioner is None:
            yield self._annotate_partition(
                block,
                {
                    uri_column_name: column_sizes.tolist()
                    for uri_column_name, column_sizes in sizes_by_column.items()
                },
            )
            return

        row_partitioner.finalize()
        yield from self._drain_row_partitions(
            block,
            row_partitioner,
            sizes_by_column,
        )

    def _min_partition_nbytes(self, target_nbytes: int) -> int:
        if self._target_min_nbytes is not None:
            return min(target_nbytes, max(1, self._target_min_nbytes))
        return max(1, target_nbytes // 2)

    def _drain_row_partitions(
        self,
        block: pa.Table,
        row_partitioner: WeightedRoundRobinPartitioner[int],
        sizes_by_column: Dict[UriColumnName, np.ndarray],
    ) -> Iterator[pa.Table]:
        while row_partitioner.has_partition():
            row_indices = np.asarray(row_partitioner.next_partition(), dtype=np.int64)
            partition_sizes_by_column = {
                uri_column_name: sizes_by_column[uri_column_name][row_indices].tolist()
                for uri_column_name in self._uri_column_names
            }
            partition = block.take(row_indices)
            yield self._annotate_partition(partition, partition_sizes_by_column)

    def _should_fetch_metadata_without_target(self, block: pa.Table) -> bool:
        if self._fetch_metadata_without_target is None:
            return False
        return self._fetch_metadata_without_target(block)

    def _iter_chunk_sizes(
        self, block: pa.Table
    ) -> Iterator[Tuple[int, int, Dict[UriColumnName, np.ndarray]]]:
        """Yield ``(chunk_start, chunk_end, sizes_by_column)`` per metadata chunk."""
        for chunk_start in range(0, block.num_rows, self._metadata_chunk_size):
            chunk_end = min(chunk_start + self._metadata_chunk_size, block.num_rows)
            chunk = block.slice(chunk_start, chunk_end - chunk_start)
            yield chunk_start, chunk_end, self._get_chunk_sizes_by_column(chunk)

    def _get_chunk_sizes_by_column(
        self, chunk: pa.Table
    ) -> Dict[UriColumnName, np.ndarray]:
        sizes_by_column = {}
        for uri_column_name in self._uri_column_names:
            uris = chunk.column(uri_column_name).to_pylist()
            sizes_by_column[uri_column_name] = self._normalize_sizes(
                self._file_size_provider.get_file_sizes(uris), len(uris)
            )
        return sizes_by_column

    def _annotate_partition(
        self,
        partition: pa.Table,
        sizes_by_column: SizesByColumn,
    ) -> pa.Table:
        if self._annotate_sizes is not None:
            partition = self._annotate_sizes(partition, sizes_by_column)
        return partition

    @staticmethod
    def _normalize_sizes(
        sizes: List[Optional[FileSizeBytes]], expected_len: int
    ) -> np.ndarray:
        """Force raw provider sizes into a dense, row-aligned ``int64`` array.

        The file-size provider contract is loose (``List[Optional[int]]`` with no
        length guarantee), but every consumer downstream assumes the opposite, so
        this is the single boundary that enforces those preconditions:

        - **No ``None``.** A failed/unsupported lookup returns ``None``, which
          can't live in an ``int64`` array; the vectorized math in ``partition``
          (``np.fromiter``, ``+=``, slice assignment) would raise. Map it to 0.
        - **Exactly ``expected_len`` entries (one per row).** ``expected_len`` is
          the chunk's row count. The provider may return a different length (e.g.
          ``_resolve_paths_and_filesystem`` silently drops unresolvable URIs), so
          truncate/zero-pad to keep ``size[i]`` aligned with ``row[i]`` — a
          mismatch would crash the fixed-width slice assignment or, worse,
          misattribute sizes to the wrong rows.
        - **Non-negative.** ``0`` is a meaningful sentinel ("unknown size" → row
          gets ``None`` weight in the partitioner and routes to a plain GET, not
          a ranged download). Negatives are garbage; clamp them to ``0`` so they
          can't skew partition weights or the range-split decision.
        """
        # Coerce None -> 0 (numpy int arrays can't hold None), clamp negatives
        # to 0, and zero-pad/truncate to exactly ``expected_len`` entries.
        normalized = np.zeros(expected_len, dtype=np.int64)
        usable = sizes[:expected_len]
        if usable:
            values = np.fromiter(
                (size or 0 for size in usable), dtype=np.int64, count=len(usable)
            )
            normalized[: len(usable)] = np.clip(values, 0, None)
        return normalized
