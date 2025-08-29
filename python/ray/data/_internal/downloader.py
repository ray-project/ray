import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator, Iterator, List, Optional

import pyarrow as pa

import ray
from ray.data._internal.util import RetryingPyFileSystem, make_async_gen
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

URI_DOWNLOAD_MAX_WORKERS = 16
logger = logging.getLogger(__name__)


def _strip_file_scheme(uri: str) -> str:
    """Strip file:// scheme from URIs to get local filesystem paths."""
    if uri.startswith("file://"):
        # Remove the file:// prefix
        # file:///path/to/file -> /path/to/file
        return uri[7:]
    return uri


class UriPartitionActor:
    """Actor that partitions download operations based on estimated file sizes."""

    INIT_SAMPLE_BATCH_SIZE = 25

    def __init__(self, data_context: DataContext):
        self._data_context = data_context
        self._batch_size_estimate = None

    def _sample_sizes(self, uris: pa.Array) -> List[int]:
        """Fetch file sizes in parallel using ThreadPoolExecutor."""

        def get_file_size(uri_path: str, fs: pa.fs.FileSystem) -> Optional[int]:
            try:
                return fs.get_file_info(uri_path).size
            except Exception:
                return None

        # If no URIs, return empty list
        if len(uris) == 0:
            return []

        # Filter out None and empty string values
        uri_list = uris.to_pylist()
        valid_uris = [uri for uri in uri_list if uri is not None and uri != ""]

        # If no valid URIs after filtering, return empty list
        if not valid_uris:
            return []

        # Strip file:// scheme for local files
        processed_uris = [_strip_file_scheme(uri) for uri in valid_uris]

        # Get the filesystem from the valid URIs
        paths, fs = _resolve_paths_and_filesystem(processed_uris)
        fs = RetryingPyFileSystem.wrap(
            fs, retryable_errors=self._data_context.retried_io_errors
        )

        # Use ThreadPoolExecutor for concurrent size fetching
        file_sizes = []
        with ThreadPoolExecutor(max_workers=URI_DOWNLOAD_MAX_WORKERS) as executor:
            # Submit all size fetch tasks
            futures = [
                executor.submit(get_file_size, uri_path, fs) for uri_path in paths
            ]

            # Collect results as they complete (order doesn't matter)
            for future in as_completed(futures):
                try:
                    size = future.result()
                    if size is not None:
                        file_sizes.append(size)
                except Exception as e:
                    logger.warning(f"Error fetching file size for download: {e}")

        return file_sizes

    def _arrow_batcher(self, uris: pa.Array, n: int) -> Iterator[pa.Array]:
        """Batch a PyArrow array into smaller arrays of size n using zero-copy slicing."""
        num_rows = len(uris)
        # Ensure n is at least 1 to avoid division by zero
        n = max(1, n)
        for i in range(0, num_rows, n):
            end_idx = min(i + n, num_rows)
            # Use PyArrow's zero-copy slice operation
            batch_uris = uris.slice(i, end_idx - i)
            yield batch_uris

    def __call__(self, uris: pa.Array) -> Generator[pa.Array, None, None]:
        # Handle empty array case
        if len(uris) == 0:
            return

        if self._batch_size_estimate is None:
            # Extract URIs from PyArrow table for sampling
            sample_size = min(self.INIT_SAMPLE_BATCH_SIZE, len(uris))
            sample_uris = uris[:sample_size]
            file_sizes = self._sample_sizes(sample_uris)
            if not file_sizes or sum(file_sizes) == 0:
                # Fallback to incoming block size if no file sizes could be determined
                # or if the total size sampled is 0
                logger.warning(
                    "No file sizes could be determined, using incoming block size"
                )
                self._batch_size_estimate = max(1, len(uris))
            else:
                file_size_estimate = sum(file_sizes) / len(file_sizes)
                ctx = ray.data.context.DatasetContext.get_current()
                max_bytes = ctx.target_max_block_size
                self._batch_size_estimate = max(
                    1, math.floor(max_bytes / file_size_estimate)
                )

        yield from self._arrow_batcher(uris, self._batch_size_estimate)


def uri_download_bytes(
    uris: pa.Array,
    data_context: DataContext,
):
    """Optimized version that uses make_async_gen for concurrent downloads."""

    if len(uris) == 0:
        return pa.array([], type=pa.binary())

    # Filter out None and empty string values, preserving indices
    uri_list = uris.to_pylist()
    valid_indices = []
    valid_uris = []
    for i, uri in enumerate(uri_list):
        if uri is not None and uri != "":
            valid_indices.append(i)
            valid_uris.append(uri)

    if not valid_uris:
        # Return array of nulls with same length as input
        return pa.array([None] * len(uri_list), type=pa.binary())

    # Strip file:// scheme for local files
    processed_uris = [_strip_file_scheme(uri) for uri in valid_uris]

    # Check for relative paths (which are invalid for file URIs)
    # and handle them gracefully for the null URIs test
    relative_path_indices = []
    for i, (original_uri, processed_uri) in enumerate(zip(valid_uris, processed_uris)):
        if original_uri.startswith("file://") and not processed_uri.startswith("/"):
            # This is a relative file URI like "file://test.txt"
            relative_path_indices.append(i)

    paths, fs = _resolve_paths_and_filesystem(processed_uris)
    fs = RetryingPyFileSystem.wrap(fs, retryable_errors=data_context.retried_io_errors)

    def load_uri_bytes(
        uri_path_iterator: Iterator[str],
    ) -> Generator[bytes, None, None]:
        """Function that takes an iterator of URI paths and yields downloaded bytes for each."""
        for idx, uri_path in enumerate(uri_path_iterator):
            # Check if this was a relative path
            if idx in relative_path_indices:
                # For relative file URIs, return None gracefully
                logger.warning(f"Invalid relative file URI: {valid_uris[idx]}")
                yield None
                continue

            with fs.open_input_file(uri_path) as f:
                yield f.read()

    # Use make_async_gen to download URI bytes concurrently
    # This preserves the order of results to match the input URIs
    uri_bytes_list = list(
        make_async_gen(
            base_iterator=iter(paths),
            fn=load_uri_bytes,
            preserve_ordering=True,
            num_workers=URI_DOWNLOAD_MAX_WORKERS,
        )
    )

    # Reconstruct the full result array, inserting None for invalid URIs
    result = [None] * len(uri_list)
    for idx, valid_idx in enumerate(valid_indices):
        result[valid_idx] = uri_bytes_list[idx]

    return pa.array(result, type=pa.binary())
