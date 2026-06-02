import logging
from functools import partial
from typing import Iterator, List, Optional

import pyarrow as pa

from ray._common.utils import env_integer
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
    _plan_obstore_routing,
    download_bytes_async,
)
from ray.data._internal.planner.download_partition_actor import (
    URI_DOWNLOAD_MAX_WORKERS,
    AsyncPartitionActor,
    PartitionActor,
)
from ray.data._internal.util import (
    RetryingPyFileSystem,
    _arrow_batcher,
    _iter_arrow_table_for_target_max_block_size,
    make_async_gen,
)
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.path_util import (
    _resolve_paths_and_filesystem,
    _validate_and_wrap_filesystem,
)

logger = logging.getLogger(__name__)

URI_SPLIT_MAX_ROWS = max(1, env_integer("RAY_DATA_DOWNLOAD_URI_SHARD_NUM_ROWS", 8192))
URI_METADATA_FETCH_MAX_ACTORS = max(
    1, env_integer("RAY_DATA_DOWNLOAD_PARTITION_MAX_ACTORS", 16)
)


def split_download_uri_blocks(
    blocks: Iterator[pa.Table],
    _,
    *,
    max_rows_per_block: int,
) -> Iterator[pa.Table]:
    """Split large URI-string blocks before exact metadata fetching."""
    max_rows_per_block = max(1, max_rows_per_block)
    for block in blocks:
        if not isinstance(block, pa.Table):
            block = BlockAccessor.for_block(block).to_arrow()
        if block.num_rows == 0:
            yield block
            continue
        yield from _arrow_batcher(block, max_rows_per_block)


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

    # If we have multiple download operators in a row, only include the
    # metadata planning stage at the start of the chain. The downstream download
    # operators then use normal block shaping.
    # Decide obstore vs threaded upfront. For fsspec-S3 filesystems backed by
    # a session we can't statically introspect (Okta / STS / profile-based),
    # _plan_obstore_routing emits a warning and returns use_obstore=False so
    # we fall back to the threaded PyArrow path — which uses the user's
    # filesystem directly and resolves credentials correctly.
    use_obstore_path = False
    if OBSTORE_AVAILABLE:
        use_obstore_path, _ = _plan_obstore_routing(filesystem)

    metadata_map_operator = None
    if not upstream_op_is_download:
        split_transformer = MapTransformer(
            [
                BlockMapTransformFn(
                    partial(
                        split_download_uri_blocks,
                        max_rows_per_block=URI_SPLIT_MAX_ROWS,
                    ),
                    disable_block_shaping=True,
                )
            ]
        )
        split_map_operator = MapOperator.create(
            split_transformer,
            input_physical_dag,
            data_context,
            name=f"SplitDownloadURIs({uri_column_names_str})",
            compute_strategy=TaskPoolStrategy(),
            supports_fusion=False,
        )

        partition_cls = AsyncPartitionActor if use_obstore_path else PartitionActor
        # PartitionActor / AsyncPartitionActor are callable classes, so we need
        # ActorPoolStrategy. Let the actor pool autoscale over row-split URI
        # blocks, but cap it so S3 HEAD concurrency is bounded by default.
        metadata_compute = ActorPoolStrategy(
            max_size=URI_METADATA_FETCH_MAX_ACTORS,
            enable_true_multi_threading=True,
        )

        fn, init_fn = _get_udf(
            partition_cls,
            (),
            {},
            (uri_column_names, data_context, filesystem),
            {},
            compute=metadata_compute,
        )
        block_fn = _generate_transform_fn_for_map_batches(fn)

        metadata_transform_fns = [
            BlockMapTransformFn(
                block_fn,
                # NOTE: Disable block-shaping to produce blocks as is
                disable_block_shaping=True,
            ),
        ]
        metadata_map_transformer = MapTransformer(
            metadata_transform_fns,
            init_fn=init_fn,
        )

        metadata_map_operator = ActorPoolMapOperator(
            metadata_map_transformer,
            split_map_operator,
            data_context,
            name=f"PlanDownloadBlocks({uri_column_names_str})",
            # NOTE: The metadata planning actor doesn't use the user-provided
            #       `ray_remote_args` since those only apply to the actual
            #       download tasks. Planning is a lightweight internal operation
            #       that doesn't need custom resource requirements.
            ray_remote_args=None,
            compute_strategy=metadata_compute,
            # NOTE: Let each metadata actor stream emitted download blocks without
            #       generator-object backpressure. Downstream operator
            #       backpressure still controls the end-to-end pipeline.
            ray_actor_task_remote_args={"_generator_backpressure_num_objects": -1},
        )

    if use_obstore_path:
        download_fn = download_bytes_async
        logger.debug("Using obstore async download path.")
    else:
        download_fn = download_bytes_threaded
        # The "obstore not installed" warning is only relevant when obstore is
        # missing entirely. When obstore is available but the filesystem can't
        # be routed through it, _plan_obstore_routing already logged the reason
        # (a WARNING for fsspec-S3-unextractable, DEBUG otherwise).
        if not OBSTORE_AVAILABLE:
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
        metadata_map_operator if metadata_map_operator else input_physical_dag,
        data_context,
        name=f"DownloadURIBytes({uri_column_names_str})",
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
        uri_column_names, output_bytes_column_names
    ):
        # Extract URIs from PyArrow table
        uris = output_block.column(uri_column_name).to_pylist()

        if len(uris) == 0:
            continue

        # Resolve the filesystem once before spawning workers; otherwise each
        # worker infers its own S3FileSystem and fires a duplicate IMDS
        # credential fetch. Normalize fsspec inputs so RetryingPyFileSystem.wrap
        # can forward open_input_stream.
        resolved_fs = _validate_and_wrap_filesystem(filesystem)
        if resolved_fs is None:
            for probe_uri in uris:
                if probe_uri is None:
                    continue
                try:
                    paths, candidate_fs = _resolve_paths_and_filesystem(probe_uri, None)
                except Exception as e:
                    logger.debug(f"Could not infer filesystem from '{probe_uri}': {e}")
                    continue
                # Skip results that drop the URI (([], ...)) or yield no FS.
                if paths and candidate_fs is not None:
                    resolved_fs = candidate_fs
                    break

        if resolved_fs is None:
            # No URI resolved a filesystem; workers would only repeat the same
            # failed inference. Yield None for every row and skip the pool.
            logger.warning(
                "Could not resolve a filesystem from any URI in column "
                f"{uri_column_name!r} ({len(uris)} URIs). Yielding None for "
                "all rows."
            )
            output_block = output_block.add_column(
                len(output_block.column_names),
                output_bytes_column_name,
                pa.array([None] * len(uris), type=pa.binary()),
            )
            continue

        wrapped_fs = RetryingPyFileSystem.wrap(
            resolved_fs, retryable_errors=data_context.retried_io_errors
        )

        def load_uri_bytes(
            uri_iterator,
            wrapped_fs=wrapped_fs,
            resolved_fs=resolved_fs,
            uri_column_name=uri_column_name,
        ):
            """Download bytes for each URI using the pre-resolved filesystem."""
            for uri in uri_iterator:
                read_bytes = None
                try:
                    if uri is None:
                        continue
                    # Normalize the path only; FS is supplied so no network I/O.
                    resolved_paths, _ = _resolve_paths_and_filesystem(
                        uri, filesystem=resolved_fs
                    )
                    resolved_path = resolved_paths[0] if resolved_paths else None
                    if resolved_path is None:
                        continue
                    with wrapped_fs.open_input_stream(resolved_path) as f:
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
