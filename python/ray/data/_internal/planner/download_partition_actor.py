import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, Iterator, List, Optional, Protocol

import pyarrow as pa
import pyarrow.fs as pafs

from ray._common.utils import env_integer
from ray.data._internal.planner._obstore_download import (
    _FILE_SIZE_COLUMN_PREFIX,
    RAY_DATA_OBSTORE_RANGE_THRESHOLD,
    StoreRegistry,
    _extract_credentials_from_filesystem,
    _is_obstore_supported_url,
    _split_obstore_uri,
)
from ray.data._internal.util import RetryingPyFileSystem
from ray.data._internal.weighted_round_robin import WeightedRoundRobinPartitioner
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

logger = logging.getLogger(__name__)

URI_DOWNLOAD_MAX_WORKERS = 16
URI_HEAD_MAX_CONCURRENCY = 128
URI_METADATA_CHUNK_SIZE = max(
    1, env_integer("RAY_DATA_DOWNLOAD_METADATA_CHUNK_SIZE", 1024)
)
URI_PARTITION_NUM_BUCKETS = max(
    1, env_integer("RAY_DATA_DOWNLOAD_PARTITION_NUM_BUCKETS", 2)
)

SizeAnnotator = Callable[[pa.Table, Dict[str, List[int]]], pa.Table]
MetadataPredicate = Callable[[pa.Table], bool]


class _FileSizeProvider(Protocol):
    def get_file_sizes(self, uris: List[str]) -> List[Optional[int]]:
        ...


class _CloseableResource:
    """Shared lifecycle for resources that hold OS handles (threads, loops).

    Subclasses implement an idempotent ``close()``. This mixin wires up the
    context-manager protocol and a best-effort ``__del__`` fallback so callers
    can release resources deterministically instead of relying on ``__del__``,
    whose timing is non-deterministic in CPython.
    """

    def close(self) -> None:
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def __del__(self):
        # Best-effort fallback; prefer an explicit close()/context manager.
        self.close()


def _ensure_arrow_table(block: pa.Table) -> pa.Table:
    if not isinstance(block, pa.Table):
        return BlockAccessor.for_block(block).to_arrow()
    return block


def _validate_uri_columns(block: pa.Table, uri_column_names: List[str]) -> None:
    for uri_column_name in uri_column_names:
        if uri_column_name not in block.column_names:
            raise ValueError(
                "Ray Data tried to download URIs from a column named "
                f"{uri_column_name!r}, but a column with that name doesn't "
                "exist. Is the specified download column correct?"
            )


def _get_target_min_block_size(data_context: DataContext) -> Optional[int]:
    target_min_block_size = getattr(data_context, "target_min_block_size", None)
    return target_min_block_size if isinstance(target_min_block_size, int) else None


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
        sizes_by_row_index: Dict[int, Dict[str, int]] = {}

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
        sizes_by_row_index: Dict[int, Dict[str, int]],
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

    def _get_block_sizes_by_column(self, block: pa.Table) -> Dict[str, List[int]]:
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

    def _get_chunk_sizes_by_column(self, chunk: pa.Table) -> Dict[str, List[int]]:
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
        row_indices: List[int],
        sizes_by_column: Dict[str, List[int]],
    ) -> pa.Table:
        partition = block.take(pa.array(row_indices, type=pa.int64()))
        return self._annotate_partition(partition, sizes_by_column)

    def _annotate_partition(
        self,
        partition: pa.Table,
        sizes_by_column: Dict[str, List[int]],
    ) -> pa.Table:
        if self._annotate_sizes is not None:
            partition = self._annotate_sizes(partition, sizes_by_column)
        return partition

    @staticmethod
    def _normalize_sizes(sizes: List[Optional[int]], expected_len: int) -> List[int]:
        normalized = [
            size if size is not None and size > 0 else 0
            for size in sizes[:expected_len]
        ]
        if len(normalized) < expected_len:
            normalized.extend([0] * (expected_len - len(normalized)))
        return normalized


class _PyArrowFileSizeProvider(_CloseableResource):
    """Fetches exact file sizes through PyArrow filesystem metadata."""

    def __init__(
        self,
        data_context: DataContext,
        filesystem: Optional[pafs.FileSystem] = None,
    ):
        self._data_context = data_context
        self._filesystem = filesystem
        self._executor = ThreadPoolExecutor(max_workers=URI_DOWNLOAD_MAX_WORKERS)

    def close(self) -> None:
        """Shut down the thread pool. Idempotent and safe to call repeatedly."""
        # ThreadPoolExecutor.shutdown() is itself idempotent.
        self._executor.shutdown(wait=False)

    def get_file_sizes(self, uris: List[str]) -> List[Optional[int]]:
        def get_file_size(uri_path, fs):
            try:
                return fs.get_file_info(uri_path).size
            except Exception:
                return None

        if not uris:
            return []

        try:
            paths, fs = _resolve_paths_and_filesystem(uris, filesystem=self._filesystem)
            fs = RetryingPyFileSystem.wrap(
                fs, retryable_errors=self._data_context.retried_io_errors
            )
        except Exception as e:
            logger.warning(f"Failed to resolve URIs for exact size metadata: {e}")
            return [0] * len(uris)

        # _resolve_paths_and_filesystem silently drops URIs that fail. Fall
        # back to zeros rather than risk a length mismatch with the input block.
        if len(paths) != len(uris):
            logger.debug(
                "Path resolution dropped %d of %d URIs; returning size=0 for all.",
                len(uris) - len(paths),
                len(uris),
            )
            return [0] * len(uris)

        file_sizes: List[Optional[int]] = [None] * len(paths)
        future_to_file_index = {
            self._executor.submit(get_file_size, uri_path, fs): file_index
            for file_index, uri_path in enumerate(paths)
        }

        for future in as_completed(future_to_file_index):
            file_index = future_to_file_index[future]
            try:
                size = future.result()
                file_sizes[file_index] = size if size is not None else 0
            except Exception as e:
                logger.warning(f"Error fetching file size for download: {e}")
                file_sizes[file_index] = 0

        assert all(
            size is not None for size in file_sizes
        ), "File size metadata did not complete for all paths"
        return [size for size in file_sizes if size is not None]


class _ObstoreFileSizeProvider(_CloseableResource):
    """Fetches exact file sizes through obstore, falling back to PyArrow."""

    def __init__(
        self,
        registry: StoreRegistry,
        fallback_provider: _PyArrowFileSizeProvider,
    ):
        self._registry = registry
        self._fallback_provider = fallback_provider
        self._loop = asyncio.new_event_loop()
        self._sem = asyncio.Semaphore(URI_HEAD_MAX_CONCURRENCY)
        self._loop_thread = threading.Thread(
            target=self._run_loop,
            name="ray-data-obstore-file-size-provider",
            daemon=True,
        )
        self._loop_thread.start()

    def close(self) -> None:
        """Stop the event loop, join its thread, and close the fallback.

        Idempotent and safe to call repeatedly.
        """
        try:
            self._fallback_provider.close()
        finally:
            if self._loop.is_closed():
                return

            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
                if (
                    self._loop_thread.is_alive()
                    and self._loop_thread is not threading.current_thread()
                ):
                    self._loop_thread.join(timeout=1)
            else:
                self._loop.close()

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def get_file_sizes(self, uris: List[str]) -> List[Optional[int]]:
        """Fetch sizes for URIs using obstore's async HEAD API.

        For URI schemes not supported by obstore (e.g. hdfs://), falls back
        to PyArrow-threaded metadata fetching.
        """
        import obstore as obs

        if not uris:
            return []

        if not _is_obstore_supported_url(uris[0]):
            return self._fallback_provider.get_file_sizes(uris)

        async def _head_one(uri: str) -> int:
            try:
                store_url, path = _split_obstore_uri(uri)
                store = self._registry.get(store_url)
                async with self._sem:
                    meta = await obs.head_async(store, path)
                return meta["size"] if isinstance(meta, dict) else meta.size
            except Exception:
                return 0

        async def _head_uris() -> List[int]:
            sizes = await asyncio.gather(*[_head_one(u) for u in uris])
            failed = [uri for uri, size in zip(uris, sizes) if size == 0]
            if failed:
                logger.debug(
                    "obstore HEAD failed for %d URIs: %s",
                    len(failed),
                    failed,
                )
            return sizes

        future = asyncio.run_coroutine_threadsafe(_head_uris(), self._loop)
        return future.result()


class PartitionActor:
    """Actor that partitions downloads using exact PyArrow metadata."""

    def __init__(
        self,
        uri_column_names: List[str],
        data_context: DataContext,
        filesystem: Optional[pafs.FileSystem] = None,
    ):
        self._uri_column_names = uri_column_names
        self._size_provider = _PyArrowFileSizeProvider(data_context, filesystem)
        self._partitioner = _ExactDownloadPartitioner(
            uri_column_names,
            data_context.target_max_block_size,
            self._size_provider,
            target_min_nbytes=_get_target_min_block_size(data_context),
        )

    def __call__(self, block: pa.Table) -> Iterator[pa.Table]:
        block = _ensure_arrow_table(block)
        _validate_uri_columns(block, self._uri_column_names)
        yield from self._partitioner.partition(block)

    def close(self) -> None:
        """Release the size provider's resources (thread pool)."""
        self._size_provider.close()


class AsyncPartitionActor:
    """Actor that partitions downloads using exact obstore metadata."""

    def __init__(
        self,
        uri_column_names: List[str],
        data_context: DataContext,
        filesystem: Optional[pafs.FileSystem] = None,
    ):
        self._uri_column_names = uri_column_names
        fs_kwargs = _extract_credentials_from_filesystem(filesystem)
        if fs_kwargs is None:
            # Fail closed. ``plan_download_op`` routes filesystems we can't
            # extract credentials from to ``PartitionActor`` (PyArrow path),
            # so reaching ``AsyncPartitionActor`` with an unextractable FS is
            # a bug. Silently seeding an empty kwargs dict would hand the
            # user's filesystem over to obstore's ambient credential chain
            # (IMDS / env), which is exactly the silent-drop behavior the
            # routing was designed to prevent.
            raise RuntimeError(
                "AsyncPartitionActor was constructed with a filesystem whose "
                f"credentials cannot be statically extracted ({type(filesystem).__name__}). "
                "This indicates a dispatch bug: use PartitionActor for such "
                "filesystems so the user's credentials are not silently dropped."
            )
        self._registry = StoreRegistry(retry_config={"max_retries": 10}, **fs_kwargs)
        pyarrow_provider = _PyArrowFileSizeProvider(data_context, filesystem)
        self._size_provider = _ObstoreFileSizeProvider(self._registry, pyarrow_provider)
        self._partitioner = _ExactDownloadPartitioner(
            uri_column_names,
            data_context.target_max_block_size,
            self._size_provider,
            annotate_sizes=self._annotate_file_size_columns,
            fetch_metadata_without_target=self._should_annotate_file_size_columns,
            target_min_nbytes=_get_target_min_block_size(data_context),
        )

    def __call__(self, block: pa.Table) -> Iterator[pa.Table]:
        block = _ensure_arrow_table(block)
        _validate_uri_columns(block, self._uri_column_names)
        yield from self._partitioner.partition(block)

    def close(self) -> None:
        """Release the size provider's resources (event loop + thread pool)."""
        self._size_provider.close()

    def _annotate_file_size_columns(
        self,
        block: pa.Table,
        sizes_by_column: Dict[str, List[int]],
    ) -> pa.Table:
        if not self._should_annotate_file_size_columns(block):
            return block

        for uri_column_name in self._uri_column_names:
            size_col = f"{_FILE_SIZE_COLUMN_PREFIX}{uri_column_name}"
            if size_col in block.column_names:
                block = block.drop([size_col])

            block = block.append_column(
                size_col,
                pa.array(sizes_by_column[uri_column_name], type=pa.int64()),
            )
        return block

    def _should_annotate_file_size_columns(self, block: pa.Table) -> bool:
        if RAY_DATA_OBSTORE_RANGE_THRESHOLD <= 0 or block.num_rows == 0:
            return False

        first_uri = block.column(self._uri_column_names[0])[0].as_py()
        return _is_obstore_supported_url(first_uri)
