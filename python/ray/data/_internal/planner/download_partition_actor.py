import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterator, List, Optional, cast

import pyarrow as pa
import pyarrow.fs as pafs

from ray.data._internal.planner._obstore_download import (
    _FILE_SIZE_COLUMN_PREFIX,
    RAY_DATA_OBSTORE_RANGE_THRESHOLD,
    _is_obstore_supported_url,
)
from ray.data._internal.util import RetryingPyFileSystem, _arrow_batcher
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

logger = logging.getLogger(__name__)

URI_DOWNLOAD_MAX_WORKERS = 16


class PartitionActor:
    """Actor that partitions download operations based on estimated file sizes.

    For multiple URI columns, estimates the combined size across all columns.
    Uses threaded HEAD/metadata sampling only (no obstore-specific columns).
    """

    INIT_SAMPLE_BATCH_SIZE = 25

    def __init__(
        self,
        uri_column_names: List[str],
        data_context: DataContext,
        filesystem: Optional[pafs.FileSystem] = None,
    ):
        self._uri_column_names = uri_column_names
        self._data_context = data_context
        self._filesystem = filesystem
        self._batch_size_estimate = None

    def __call__(self, block: pa.Table) -> Iterator[pa.Table]:
        block = self._ensure_arrow_table(block)
        self._validate_uri_columns(block)
        yield from self._partition_and_yield(block)

    def _ensure_arrow_table(self, block: pa.Table) -> pa.Table:
        if not isinstance(block, pa.Table):
            return BlockAccessor.for_block(block).to_arrow()
        return block

    def _validate_uri_columns(self, block: pa.Table) -> None:
        for uri_column_name in self._uri_column_names:
            if uri_column_name not in block.column_names:
                raise ValueError(
                    "Ray Data tried to download URIs from a column named "
                    f"{uri_column_name!r}, but a column with that name doesn't "
                    "exist. Is the specified download column correct?"
                )

    def _partition_and_yield(self, block: pa.Table) -> Iterator[pa.Table]:
        if self._batch_size_estimate is None and block.num_rows > 0:
            self._batch_size_estimate = self._estimate_nrows_per_partition(block)
        yield from _arrow_batcher(block, self._batch_size_estimate or 1)

    def _sampled_file_sizes_for_partition_estimate(
        self, block: pa.Table, uri_column_name: str
    ) -> List[Optional[int]]:
        uris = block.column(uri_column_name).to_pylist()
        sample_uris = uris[: self.INIT_SAMPLE_BATCH_SIZE]
        # ``_sample_sizes`` returns concrete ``int``s; widen for this API.
        return cast(List[Optional[int]], self._sample_sizes(sample_uris))

    def _estimate_nrows_per_partition(self, block: pa.Table) -> int:
        sampled_file_sizes_by_column = {}
        for uri_column_name in self._uri_column_names:
            sampled_file_sizes = self._sampled_file_sizes_for_partition_estimate(
                block, uri_column_name
            )
            sampled_file_sizes_by_column[uri_column_name] = sampled_file_sizes

        sampled_file_sizes_by_column = {
            uri_column_name: [
                file_size if file_size is not None else 0
                for file_size in sampled_file_sizes
            ]
            for uri_column_name, sampled_file_sizes in sampled_file_sizes_by_column.items()
        }

        # This is some fancy Python code to compute the file size of each row.
        row_sizes = [
            sum(file_sizes_in_row)
            for file_sizes_in_row in zip[tuple[Any, ...]](
                *sampled_file_sizes_by_column.values()
            )
        ]

        target_nbytes_per_partition = self._data_context.target_max_block_size
        avg_nbytes_per_row = sum(row_sizes) / len(row_sizes)
        if avg_nbytes_per_row == 0:
            logger.warning(
                "Estimated average row size is 0. Falling back to using the number of "
                "rows in the block as the partition size."
            )
            return len(block)

        if target_nbytes_per_partition is None:
            # Target max block size is None--keep the whole block as one partition.
            return len(block)

        nrows_per_partition = math.floor(
            target_nbytes_per_partition / avg_nbytes_per_row
        )
        if nrows_per_partition == 0:
            # A single file exceeds target_max_block_size. Fall back to one row
            # per partition so _arrow_batcher doesn't crash on a zero step size.
            logger.warning(
                f"Estimated average file size ({avg_nbytes_per_row:.0f} bytes) "
                f"exceeds target_max_block_size ({target_nbytes_per_partition} bytes). "
                "Falling back to one row per partition; output blocks may be larger "
                "than the configured target."
            )
            return 1
        return nrows_per_partition

    def _sample_sizes(self, uris: List[str]) -> List[int]:
        """Fetch file sizes in parallel using ThreadPoolExecutor."""

        def get_file_size(uri_path, fs):
            try:
                return fs.get_file_info(uri_path).size
            except Exception:
                return None

        if not uris:
            return []

        # Get the filesystem from the URIs (assumes all URIs use same filesystem for sampling)
        # This is for sampling the file sizes which doesn't require a full resolution of the paths.
        try:
            paths, fs = _resolve_paths_and_filesystem(uris, filesystem=self._filesystem)
            fs = RetryingPyFileSystem.wrap(
                fs, retryable_errors=self._data_context.retried_io_errors
            )
        except Exception as e:
            logger.warning(f"Failed to resolve URIs for size sampling: {e}")
            return [0] * len(uris)

        # _resolve_paths_and_filesystem silently drops URIs that fail.
        # Fall back to zeros (triggers HEAD in the download path) rather than risk
        # a length mismatch with the input block.
        if len(paths) != len(uris):
            logger.debug(
                "Path resolution dropped %d of %d URIs; returning size=0 "
                "for all so the download path can issue HEAD requests.",
                len(uris) - len(paths),
                len(uris),
            )
            return [0] * len(uris)

        # Use ThreadPoolExecutor for concurrent size fetching
        file_sizes: List[Optional[int]] = [None] * len(paths)
        with ThreadPoolExecutor(max_workers=URI_DOWNLOAD_MAX_WORKERS) as executor:
            # Submit all size fetch tasks
            future_to_file_index = {
                executor.submit(get_file_size, uri_path, fs): file_index
                for file_index, uri_path in enumerate(paths)
            }

            # Collect results as they complete (order doesn't matter)
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
        ), "File size sampling did not complete for all paths"
        return [int(size) for size in file_sizes]


class AsyncPartitionActor(PartitionActor):
    """Partition actor for the obstore download path.

    When range splitting is enabled, attaches per-URI size columns (from metadata
    HEADs) so downstream obstore downloads can skip redundant HEAD requests.
    """

    def __call__(self, block: pa.Table) -> Iterator[pa.Table]:
        block = self._ensure_arrow_table(block)
        self._validate_uri_columns(block)

        if block.num_rows > 0 and RAY_DATA_OBSTORE_RANGE_THRESHOLD > 0:
            first_uri = block.column(self._uri_column_names[0])[0].as_py()
            if _is_obstore_supported_url(first_uri):
                block = self._attach_file_sizes(block)

        yield from self._partition_and_yield(block)

    def _sampled_file_sizes_for_partition_estimate(
        self, block: pa.Table, uri_column_name: str
    ) -> List[Optional[int]]:
        size_col = f"{_FILE_SIZE_COLUMN_PREFIX}{uri_column_name}"
        if size_col in block.column_names:
            all_sizes = block.column(size_col).to_pylist()
            return all_sizes[: self.INIT_SAMPLE_BATCH_SIZE]
        return super()._sampled_file_sizes_for_partition_estimate(
            block, uri_column_name
        )

    def _attach_file_sizes(self, block: pa.Table) -> pa.Table:
        """Fetch file sizes for all URIs and attach as hidden columns.

        Only called when obstore is available, range splitting is enabled
        (RAY_DATA_OBSTORE_RANGE_THRESHOLD > 0), and the URI scheme is
        supported by obstore.  The hidden columns are consumed by
        ``_download_uris_with_obstore`` and dropped before output.

        The hidden columns are consumed by ``_download_uris_with_obstore`` and
        dropped before output. For cloud URIs this uses cheap metadata lookups.
        For HTTP URIs where sizes are unavailable, stores 0 so the downstream
        download path falls back to HEAD via obstore.
        """
        for uri_column_name in self._uri_column_names:
            size_col = f"{_FILE_SIZE_COLUMN_PREFIX}{uri_column_name}"
            uris = block.column(uri_column_name).to_pylist()
            # Fetches all file sizes (not just a sample).
            sizes = self._sample_sizes(uris)
            block = block.append_column(size_col, pa.array(sizes, type=pa.int64()))
        return block
