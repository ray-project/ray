import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List
from urllib.parse import urlparse

import pyarrow as pa
from pyarrow.fs import FileSystem as pafs

import ray
from ray.data._internal.compute import ActorPoolStrategy, get_compute
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators.map_operator import Download
from ray.data._internal.util import make_async_gen
from ray.data.block import BlockAccessor
from ray.data.context import DataContext

logger = logging.getLogger(__name__)


def plan_download_op(
    op: Download,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    """Plan the download operation with partitioning and downloading stages."""
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]
    compute = get_compute(op._compute)
    uri_column_name = op.uri_column_name
    output_bytes_column_name = op.output_bytes_column_name

    # Import _get_udf from the main planner file
    from ray.data._internal.planner.plan_udf_map_op import (
        _generate_transform_fn_for_map_batches,
        _get_udf,
    )

    # PartitionActor is a callable class, so we need ActorPoolStrategy
    partition_compute = ActorPoolStrategy(size=1)  # Use single actor for partitioning

    fn, init_fn = _get_udf(PartitionActor, (), {}, (uri_column_name,), {})
    partition_transform_fn = _generate_transform_fn_for_map_batches(fn)
    partition_transform_fns = [
        BlockMapTransformFn(partition_transform_fn),
    ]
    partition_map_transformer = MapTransformer(partition_transform_fns, init_fn)
    partition_map_operator = MapOperator.create(
        partition_map_transformer,
        input_physical_dag,
        data_context,
        name="DownloadURIPartitioner",
        compute_strategy=partition_compute,  # Use actor-based compute for callable class
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
    )

    fn, init_fn = _get_udf(
        download_bytes_threaded,
        (uri_column_name, output_bytes_column_name),
        {},
        None,
        None,
    )
    download_transform_fn = _generate_transform_fn_for_map_batches(fn)
    transform_fns = [
        BlockMapTransformFn(download_transform_fn),
    ]
    download_map_transformer = MapTransformer(transform_fns, init_fn)
    download_map_operator = MapOperator.create(
        download_map_transformer,
        partition_map_operator,
        data_context,
        name="DownloadURIDownloader",
        compute_strategy=compute,
        ray_remote_args=op._ray_remote_args,
        ray_remote_args_fn=op._ray_remote_args_fn,
    )

    return download_map_operator


def uri_to_path(uri: str) -> str:
    """Convert a URI to a filesystem path."""
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return parsed.path
    return parsed.netloc + parsed.path


def download_bytes_threaded(
    block, uri_column_name, output_bytes_column_name, max_workers=16
):
    """Optimized version that uses make_async_gen for concurrent downloads."""
    if not isinstance(block, pa.Table):
        block = BlockAccessor.for_block(block).to_arrow()

    # Extract URIs from PyArrow table
    uris = block.column(uri_column_name).to_pylist()

    if len(uris) == 0:
        return block

    fs, _ = pafs.from_uri(uris[0])  # from_uri returns (filesystem, path)

    def download_uris(uri_iterator):
        """Function that takes an iterator of URIs and yields downloaded bytes for each."""
        for uri in uri_iterator:
            try:
                with fs.open_input_file(uri_to_path(uri)) as f:
                    yield f.read()
            except Exception as e:
                logger.warning(f"Failed to download {uri}: {e}")
                yield None

    # Use make_async_gen to download URIs concurrently
    # This preserves the order of results to match the input URIs
    uri_bytes = list(
        make_async_gen(
            base_iterator=iter(uris),
            fn=download_uris,
            preserve_ordering=True,
            num_workers=max_workers,
        )
    )

    # Add the new column to the PyArrow table
    return block.add_column(
        len(block.column_names), output_bytes_column_name, pa.array(uri_bytes)
    )


class PartitionActor:
    """Actor that partitions download operations based on estimated file sizes."""

    INIT_SAMPLE_BATCH_SIZE = 25

    def __init__(self, uri_column_name: str):
        self._uri_column_name = uri_column_name
        self._batch_size_estimate = None

    def _sample_sizes(self, uri_list, max_workers=16):
        """Fetch file sizes in parallel using ThreadPoolExecutor."""

        def get_file_size(uri, fs):
            try:
                return fs.get_file_info(uri_to_path(uri)).size
            except Exception:
                return None

        # If no URIs, return empty list
        if not uri_list:
            return []

        # Get the filesystem from the first URI
        fs, _ = pafs.from_uri(uri_list[0])

        # Use ThreadPoolExecutor for concurrent size fetching
        file_sizes = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all size fetch tasks
            futures = [executor.submit(get_file_size, uri, fs) for uri in uri_list]

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
