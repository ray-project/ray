import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from urllib.parse import urlparse

import pyarrow as pa

import ray
from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators.one_to_one_operator import Download
from ray.data._internal.util import RetryingPyFileSystem, make_async_gen
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

logger = logging.getLogger(__name__)

URI_DOWNLOAD_MAX_WORKERS = 16


def plan_download_op(
    op: Download,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan the download operation with partitioning and downloading stages."""
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]
    uri_column_name = op.uri_column_name
    output_bytes_column_name = op.output_bytes_column_name
    ray_remote_args = op.ray_remote_args

    # Import _get_udf from the main planner file
    from ray.data._internal.planner.plan_udf_map_op import (
        _generate_transform_fn_for_map_batches,
        _get_udf,
    )

    # PartitionActor is a callable class, so we need ActorPoolStrategy
    partition_compute = ActorPoolStrategy(size=1)  # Use single actor for partitioning

    fn, init_fn = _get_udf(PartitionActor, (), {}, (uri_column_name, data_context), {})
    block_fn = _generate_transform_fn_for_map_batches(fn)
    partition_transform_fns = [
        BlockMapTransformFn(block_fn),
    ]
    partition_map_transformer = MapTransformer(partition_transform_fns, init_fn)
    partition_map_operator = MapOperator.create(
        partition_map_transformer,
        input_physical_dag,
        data_context,
        name="URIPartitioner",
        compute_strategy=partition_compute,  # Use actor-based compute for callable class
        ray_remote_args=ray_remote_args,
    )

    fn, init_fn = _get_udf(
        download_bytes_threaded,
        (uri_column_name, output_bytes_column_name, data_context),
        {},
        None,
        None,
    )
    download_transform_fn = _generate_transform_fn_for_map_batches(fn)
    transform_fns = [
        BlockMapTransformFn(download_transform_fn),
    ]
    download_map_transformer = MapTransformer(transform_fns, init_fn)
    download_compute = TaskPoolStrategy()
    download_map_operator = MapOperator.create(
        download_map_transformer,
        partition_map_operator,
        data_context,
        name="URIDownloader",
        compute_strategy=download_compute,
        ray_remote_args=ray_remote_args,
    )

    return download_map_operator


def uri_to_path(uri: str) -> str:
    """Convert a URI to a filesystem path."""
    # TODO(mowen): urlparse might be slow. in the future we could use a faster alternative.
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return parsed.path
    return parsed.netloc + parsed.path


def download_bytes_threaded(
    block,
    uri_column_name,
    output_bytes_column_name,
    data_context: DataContext,
):
    """Optimized version that uses make_async_gen for concurrent downloads."""
    if not isinstance(block, pa.Table):
        block = BlockAccessor.for_block(block).to_arrow()

    # Extract URIs from PyArrow table
    uris = block.column(uri_column_name).to_pylist()

    if len(uris) == 0:
        return block

    paths, fs = _resolve_paths_and_filesystem(uris)
    fs = RetryingPyFileSystem.wrap(fs, retryable_errors=data_context.retried_io_errors)

    def load_uri_bytes(uri_path_iterator):
        """Function that takes an iterator of URI paths and yields downloaded bytes for each."""
        for uri_path in uri_path_iterator:
            with fs.open_input_file(uri_path) as f:
                yield f.read()

    # Use make_async_gen to download URI bytes concurrently
    # This preserves the order of results to match the input URIs
    uri_bytes = list(
        make_async_gen(
            base_iterator=iter(paths),
            fn=load_uri_bytes,
            preserve_ordering=True,
            num_workers=URI_DOWNLOAD_MAX_WORKERS,
        )
    )

    # Add the new column to the PyArrow table
    return block.add_column(
        len(block.column_names), output_bytes_column_name, pa.array(uri_bytes)
    )


class PartitionActor:
    """Actor that partitions download operations based on estimated file sizes."""

    INIT_SAMPLE_BATCH_SIZE = 25

    def __init__(self, uri_column_name: str, data_context: DataContext):
        self._uri_column_name = uri_column_name
        self._data_context = data_context
        self._batch_size_estimate = None

    def _sample_sizes(self, uris):
        """Fetch file sizes in parallel using ThreadPoolExecutor."""

        def get_file_size(uri_path, fs):
            try:
                return fs.get_file_info(uri_path).size
            except Exception:
                return None

        # If no URIs, return empty list
        if not uris:
            return []

        # Get the filesystem from the first URI
        paths, fs = _resolve_paths_and_filesystem(uris)
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

    def _arrow_batcher(self, table, n):
        """Batch a PyArrow table into smaller tables of size n using zero-copy slicing."""
        num_rows = table.num_rows
        for i in range(0, num_rows, n):
            end_idx = min(i + n, num_rows)
            # Use PyArrow's zero-copy slice operation
            batch_table = table.slice(i, end_idx - i)
            yield batch_table

    def __call__(self, block):
        if not isinstance(block, pa.Table):
            block = BlockAccessor.for_block(block).to_arrow()

        if self._batch_size_estimate is None:
            # Extract URIs from PyArrow table for sampling
            uris = block.column(self._uri_column_name).to_pylist()
            sample_uris = uris[: self.INIT_SAMPLE_BATCH_SIZE]
            file_sizes = self._sample_sizes(sample_uris)
            if not file_sizes or sum(file_sizes) == 0:
                # Fallback to incoming block size if no file sizes could be determined
                # or if the total size sampled is 0
                logger.warning(
                    "No file sizes could be determined, using incoming block size"
                )
                self._batch_size_estimate = block.num_rows
            else:
                file_size_estimate = sum(file_sizes) / len(file_sizes)
                ctx = ray.data.context.DatasetContext.get_current()
                max_bytes = ctx.target_max_block_size
                self._batch_size_estimate = math.floor(max_bytes / file_size_estimate)

        yield from self._arrow_batcher(block, self._batch_size_estimate)
