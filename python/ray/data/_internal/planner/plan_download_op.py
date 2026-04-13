import logging
from typing import Iterator, List, Optional

import pyarrow as pa

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
from ray.data._internal.logical.operators import Download
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.planner._obstore_download import (
    OBSTORE_AVAILABLE,
    _log_fallback_warning,
    download_bytes_async,
)
from ray.data._internal.planner.download_partition_actor import (
    URI_DOWNLOAD_MAX_WORKERS,
    AsyncPartitionActor,
    PartitionActor,
)
from ray.data._internal.util import (
    RetryingPyFileSystem,
    _iter_arrow_table_for_target_max_block_size,
    make_async_gen,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

logger = logging.getLogger(__name__)


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
    filesystem = op.filesystem

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
        partition_cls = AsyncPartitionActor if OBSTORE_AVAILABLE else PartitionActor
        # PartitionActor / AsyncPartitionActor are callable classes, so we need
        # ActorPoolStrategy.
        partition_compute = ActorPoolStrategy(
            size=1, enable_true_multi_threading=True
        )  # Use single actor for partitioning

        fn, init_fn = _get_udf(
            partition_cls,
            (),
            {},
            (uri_column_names, data_context, filesystem),
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

    if OBSTORE_AVAILABLE:
        download_fn = download_bytes_async
        logger.debug("Using obstore async download path.")
    else:
        download_fn = download_bytes_threaded
        _log_fallback_warning()

    fn, init_fn = _get_udf(
        download_fn,
        (uri_column_names, output_bytes_column_names, data_context, filesystem),
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


def download_bytes_threaded(
    block: pa.Table,
    uri_column_names: List[str],
    output_bytes_column_names: List[str],
    data_context: DataContext,
    filesystem: Optional["pa.fs.FileSystem"] = None,
) -> Iterator[pa.Table]:
    """Optimized version that uses make_async_gen for concurrent downloads.

    Supports downloading from multiple URI columns in a single operation.

    Args:
        block: Input PyArrow table containing URI columns.
        uri_column_names: Names of columns containing URIs to download.
        output_bytes_column_names: Names for the output columns containing downloaded bytes.
        data_context: Ray Data context for configuration.
        filesystem: PyArrow filesystem to use for reading remote files.
            If None, the filesystem is auto-detected from the path scheme.

    Yields:
        pa.Table: PyArrow table with the downloaded bytes added as new columns.
    """
    if not isinstance(block, pa.Table):
        block = BlockAccessor.for_block(block).to_arrow()

    output_block = block

    # Download each URI column and add it to the output block
    for uri_column_name, output_bytes_column_name in zip(
        uri_column_names, output_bytes_column_names, strict=False
    ):
        # Extract URIs from PyArrow table
        uris = output_block.column(uri_column_name).to_pylist()

        if len(uris) == 0:
            continue

        def load_uri_bytes(uri_iterator):
            """Resolve filesystem and download bytes for each URI.

            Takes an iterator of URIs and yields bytes for each.
            Uses lazy filesystem resolution - resolves once and reuses for subsequent URIs.
            If a filesystem was provided explicitly, it will be used for all URIs.
            """
            cached_fs = filesystem
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

    yield from _iter_arrow_table_for_target_max_block_size(
        output_block, data_context.target_max_block_size
    )
