import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator, List
from urllib.parse import urlparse

import pyarrow as pa

import ray
from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
)
from ray.data._internal.logical.operators.one_to_one_operator import Download
from ray.data._internal.output_buffer import OutputBlockSizeOption
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

    upstream_op_is_download = False
    if len(input_physical_dag._logical_operators) == 1 and isinstance(
        input_physical_dag._logical_operators[0], Download
    ):
        upstream_op_is_download = True

    uri_column_names = op.uri_column_names
    uri_column_names_str = ", ".join(uri_column_names)
    output_bytes_column_names = op.output_bytes_column_names
    ray_remote_args = op.ray_remote_args

    # Import _get_udf from the main planner file
    from ray.data._internal.planner.plan_udf_map_op import (
        _generate_transform_fn_for_map_batches,
        _get_udf,
    )

    # If we have multiple download operators in a row, we should only include the partition actor
    # at the start of the chain. This is primarily done to prevent partition actors from bottlenecking
    # the chain becuase the interleaved operators would be a single actor. As a result, the
    # URIDownloader physical operator is responsible for outputting appropriately sized blocks.
    partition_map_operator = None
    if not upstream_op_is_download:
        # PartitionActor is a callable class, so we need ActorPoolStrategy
        partition_compute = ActorPoolStrategy(
            size=1, enable_true_multi_threading=True
        )  # Use single actor for partitioning

        fn, init_fn = _get_udf(
            PartitionActor,
            (),
            {},
            (uri_column_names, data_context),
            {},
            compute=partition_compute,
        )
        block_fn = _generate_transform_fn_for_map_batches(fn)

        partition_transform_fns = [
            BlockMapTransformFn(
                block_fn,
                # NOTE: Disable block-shaping to produce blocks as is
                disable_block_shaping=True,
            ),
        ]
        partition_map_transformer = MapTransformer(
            partition_transform_fns,
            init_fn=init_fn,
        )

        partition_map_operator = ActorPoolMapOperator(
            partition_map_transformer,
            input_physical_dag,
            data_context,
            name=f"Partition({uri_column_names_str})",
            # NOTE: Partition actor doesn't use the user-provided `ray_remote_args`
            #       since those only apply to the actual download tasks. Partitioning is
            #       a lightweight internal operation that doesn't need custom resource
            #       requirements.
            ray_remote_args=None,
            compute_strategy=partition_compute,  # Use actor-based compute for callable class
            # NOTE: We set `_generator_backpressure_num_objects` to -1 to unblock
            #       backpressure since partitioning is extremely fast. Without this, the
            #       partition actor gets bottlenecked by the Ray Data scheduler, which
            #       can prevent Ray Data from launching enough download tasks.
            ray_actor_task_remote_args={"_generator_backpressure_num_objects": -1},
        )

    fn, init_fn = _get_udf(
        download_bytes_threaded,
        (uri_column_names, output_bytes_column_names, data_context),
        {},
        None,
        None,
        None,
    )

    download_transform_fn = _generate_transform_fn_for_map_batches(fn)
    transform_fns = [
        BlockMapTransformFn(
            download_transform_fn,
            output_block_size_option=OutputBlockSizeOption.of(
                target_max_block_size=data_context.target_max_block_size
            ),
        ),
    ]

    download_compute = TaskPoolStrategy()
    download_map_transformer = MapTransformer(
        transform_fns,
        init_fn=init_fn,
    )

    download_map_operator = MapOperator.create(
        download_map_transformer,
        partition_map_operator if partition_map_operator else input_physical_dag,
        data_context,
        name=f"Download({uri_column_names_str})",
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


def _arrow_batcher(table: pa.Table, output_batch_size: int):
    """Batch a PyArrow table into smaller tables of size n using zero-copy slicing."""
    num_rows = table.num_rows
    for i in range(0, num_rows, output_batch_size):
        end_idx = min(i + output_batch_size, num_rows)
        # Use PyArrow's zero-copy slice operation
        batch_table = table.slice(i, end_idx - i)
        yield batch_table


def download_bytes_threaded(
    block: pa.Table,
    uri_column_names: List[str],
    output_bytes_column_names: List[str],
    data_context: DataContext,
) -> Iterator[pa.Table]:
    """Optimized version that uses make_async_gen for concurrent downloads.

    Supports downloading from multiple URI columns in a single operation.
    """
    if not isinstance(block, pa.Table):
        block = BlockAccessor.for_block(block).to_arrow()

    output_block = block

    # Download each URI column and add it to the output block
    for uri_column_name, output_bytes_column_name in zip(
        uri_column_names, output_bytes_column_names
    ):
        # Extract URIs from PyArrow table
        uris = output_block.column(uri_column_name).to_pylist()

        if len(uris) == 0:
            continue

        def load_uri_bytes(uri_iterator):
            """Resolve filesystem and download bytes for each URI.

            Takes an iterator of URIs and yields bytes for each.
            Uses lazy filesystem resolution - resolves once and reuses for subsequent URIs.
            """
            cached_fs = None
            for uri in uri_iterator:
                read_bytes = None
                try:
                    # Use cached FS if available, otherwise resolve the filesystem for the uri.
                    resolved_paths, resolved_fs = _resolve_paths_and_filesystem(
                        uri, filesystem=cached_fs
                    )
                    cached_fs = resolved_fs

                    # Wrap with retrying filesystem
                    fs = RetryingPyFileSystem.wrap(
                        resolved_fs, retryable_errors=data_context.retried_io_errors
                    )
                    # We only pass one uri to resolve and unwrap it from the list of resolved paths,
                    # if fails, we will catch the index error and log it.
                    resolved_path = resolved_paths[0]
                    if resolved_path is None:
                        continue

                    # Download bytes
                    # Use open_input_stream to handle the rare scenario where the data source is not seekable.
                    with fs.open_input_stream(resolved_path) as f:
                        read_bytes = f.read()
                except OSError as e:
                    logger.debug(
                        f"OSError reading uri '{uri}' for column '{uri_column_name}': {e}"
                    )
                except Exception as e:
                    # Catch unexpected errors like pyarrow.lib.ArrowInvalid caused by an invalid uri like
                    # `foo://bar` to avoid failing because of one invalid uri.
                    logger.warning(
                        f"Unexpected error reading uri '{uri}' for column '{uri_column_name}': {e}"
                    )
                finally:
                    yield read_bytes

        # Use make_async_gen to resolve and download URI bytes concurrently
        # preserve_ordering=True ensures results are returned in the same order as input URIs
        uri_bytes = list(
            make_async_gen(
                base_iterator=iter(uris),
                fn=load_uri_bytes,
                preserve_ordering=True,
                num_workers=URI_DOWNLOAD_MAX_WORKERS,
            )
        )

        # Add the new column to the PyArrow table
        output_block = output_block.add_column(
            len(output_block.column_names),
            output_bytes_column_name,
            pa.array(uri_bytes),
        )

    output_block_size = output_block.nbytes
    ctx = ray.data.context.DatasetContext.get_current()
    max_bytes = ctx.target_max_block_size
    if max_bytes is not None and output_block_size > max_bytes:
        num_blocks = math.ceil(output_block_size / max_bytes)
        num_rows = output_block.num_rows
        yield from _arrow_batcher(output_block, int(math.ceil(num_rows / num_blocks)))
    else:
        yield output_block


class PartitionActor:
    """Actor that partitions download operations based on estimated file sizes.

    For multiple URI columns, estimates the combined size across all columns.
    """

    INIT_SAMPLE_BATCH_SIZE = 25

    def __init__(self, uri_column_names: List[str], data_context: DataContext):
        self._uri_column_names = uri_column_names
        self._data_context = data_context
        self._batch_size_estimate = None

    def __call__(self, block: pa.Table) -> Iterator[pa.Table]:
        if not isinstance(block, pa.Table):
            block = BlockAccessor.for_block(block).to_arrow()

        # Validate all URI columns exist
        for uri_column_name in self._uri_column_names:
            if uri_column_name not in block.column_names:
                raise ValueError(
                    "Ray Data tried to download URIs from a column named "
                    f"{uri_column_name!r}, but a column with that name doesn't "
                    "exist. Is the specified download column correct?"
                )

        if self._batch_size_estimate is None:
            self._batch_size_estimate = self._estimate_nrows_per_partition(block)

        yield from _arrow_batcher(block, self._batch_size_estimate)

    def _estimate_nrows_per_partition(self, block: pa.Table) -> int:
        sampled_file_sizes_by_column = {}
        for uri_column_name in self._uri_column_names:
            # Extract URIs from PyArrow table for sampling
            uris = block.column(uri_column_name).to_pylist()
            sample_uris = uris[: self.INIT_SAMPLE_BATCH_SIZE]
            sampled_file_sizes = self._sample_sizes(sample_uris)
            sampled_file_sizes_by_column[uri_column_name] = sampled_file_sizes

        # If we sample HTTP URIs, or if an error occurs during sampling, then the file
        # sizes might be `None`. In these cases, we replace the `file_size` with 0.
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
            for file_sizes_in_row in zip(*sampled_file_sizes_by_column.values())
        ]

        target_nbytes_per_partition = self._data_context.target_max_block_size
        avg_nbytes_per_row = sum(row_sizes) / len(row_sizes)
        if avg_nbytes_per_row == 0:
            logger.warning(
                "Estimated average row size is 0. Falling back to using the number of "
                "rows in the block as the partition size."
            )
            return len(block)

        nrows_per_partition = math.floor(
            target_nbytes_per_partition / avg_nbytes_per_row
        )
        return nrows_per_partition

    def _sample_sizes(self, uris: List[str]) -> List[int]:
        """Fetch file sizes in parallel using ThreadPoolExecutor."""

        def get_file_size(uri_path, fs):
            try:
                return fs.get_file_info(uri_path).size
            except Exception:
                return None

        # If no URIs, return empty list
        if not uris:
            return []

        # Get the filesystem from the URIs (assumes all URIs use same filesystem for sampling)
        # This is for sampling the file sizes which doesn't require a full resolution of the paths.
        try:
            paths, fs = _resolve_paths_and_filesystem(uris)
            fs = RetryingPyFileSystem.wrap(
                fs, retryable_errors=self._data_context.retried_io_errors
            )
        except Exception as e:
            logger.warning(f"Failed to resolve URIs for size sampling: {e}")
            # Return zeros for all URIs if resolution fails
            return [0] * len(uris)

        # Use ThreadPoolExecutor for concurrent size fetching
        file_sizes = [None] * len(paths)
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
            fs is not None for fs in file_sizes
        ), "File size sampling did not complete for all paths"
        return file_sizes
