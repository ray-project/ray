from typing import Callable, Dict, Iterator, List, Optional, Protocol

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
RowIndex = int
FileSizeBytes = int

SizesByColumn = Dict[UriColumnName, List[FileSizeBytes]]
SizesByRowIndex = Dict[RowIndex, Dict[UriColumnName, FileSizeBytes]]

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

        if self._target_nbytes is None:
            if self._should_fetch_metadata_without_target(block):
                yield self._annotate_partition(
                    block, self._get_block_sizes_by_column(block)
                )
            else:
                yield block
            return

        target_nbytes = max(1, self._target_nbytes)
        row_partitioner = WeightedRoundRobinPartitioner[int](
            min_bucket_size=self._min_partition_nbytes(target_nbytes),
            max_bucket_size=target_nbytes,
            num_buckets=self._num_buckets,
            emit_before_overflow=True,
        )
        sizes_by_row_index: SizesByRowIndex = {}

        for chunk_start in range(0, block.num_rows, self._metadata_chunk_size):
            chunk_end = min(chunk_start + self._metadata_chunk_size, block.num_rows)
            chunk = block.slice(chunk_start, chunk_end - chunk_start)
            chunk_sizes_by_column = self._get_chunk_sizes_by_column(chunk)

            for chunk_row in range(chunk.num_rows):
                row_index = chunk_start + chunk_row
                sizes_by_row_index[row_index] = {
                    uri_column_name: chunk_sizes_by_column[uri_column_name][chunk_row]
                    for uri_column_name in self._uri_column_names
                }
                row_nbytes = sum(sizes_by_row_index[row_index].values())
                row_partitioner.add_item(
                    row_index,
                    row_nbytes if row_nbytes > 0 else None,
                )
                yield from self._drain_row_partitions(
                    block,
                    row_partitioner,
                    sizes_by_row_index,
                )

        row_partitioner.finalize()
        yield from self._drain_row_partitions(
            block,
            row_partitioner,
            sizes_by_row_index,
        )

    def _min_partition_nbytes(self, target_nbytes: int) -> int:
        if self._target_min_nbytes is not None:
            return min(target_nbytes, max(1, self._target_min_nbytes))
        return max(1, target_nbytes // 2)

    def _drain_row_partitions(
        self,
        block: pa.Table,
        row_partitioner: WeightedRoundRobinPartitioner[int],
        sizes_by_row_index: SizesByRowIndex,
    ) -> Iterator[pa.Table]:
        while row_partitioner.has_partition():
            row_indices = row_partitioner.next_partition()
            sizes_by_column = {
                uri_column_name: [
                    sizes_by_row_index[row_index][uri_column_name]
                    for row_index in row_indices
                ]
                for uri_column_name in self._uri_column_names
            }
            for row_index in row_indices:
                sizes_by_row_index.pop(row_index, None)
            yield self._build_partition(block, row_indices, sizes_by_column)

    def _should_fetch_metadata_without_target(self, block: pa.Table) -> bool:
        if self._fetch_metadata_without_target is None:
            return False
        return self._fetch_metadata_without_target(block)

    def _get_block_sizes_by_column(self, block: pa.Table) -> SizesByColumn:
        sizes_by_column = {
            uri_column_name: [] for uri_column_name in self._uri_column_names
        }
        for chunk_start in range(0, block.num_rows, self._metadata_chunk_size):
            chunk_end = min(chunk_start + self._metadata_chunk_size, block.num_rows)
            chunk = block.slice(chunk_start, chunk_end - chunk_start)
            chunk_sizes_by_column = self._get_chunk_sizes_by_column(chunk)
            for uri_column_name in self._uri_column_names:
                sizes_by_column[uri_column_name].extend(
                    chunk_sizes_by_column[uri_column_name]
                )
        return sizes_by_column

    def _get_chunk_sizes_by_column(self, chunk: pa.Table) -> SizesByColumn:
        sizes_by_column = {}
        for uri_column_name in self._uri_column_names:
            uris = chunk.column(uri_column_name).to_pylist()
            sizes_by_column[uri_column_name] = self._normalize_sizes(
                self._file_size_provider.get_file_sizes(uris), len(uris)
            )
        return sizes_by_column

    def _build_partition(
        self,
        block: pa.Table,
        row_indices: List[RowIndex],
        sizes_by_column: SizesByColumn,
    ) -> pa.Table:
        partition = block.take(pa.array(row_indices, type=pa.int64()))
        return self._annotate_partition(partition, sizes_by_column)

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
    ) -> List[FileSizeBytes]:
        normalized = [
            size if size is not None and size > 0 else 0
            for size in sizes[:expected_len]
        ]
        if len(normalized) < expected_len:
            normalized.extend([0] * (expected_len - len(normalized)))
        return normalized
