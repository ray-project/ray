import asyncio
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterator, List, Optional
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
from ray.data._internal.logical.operators import Download
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.util import RetryingPyFileSystem, make_async_gen
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.path_util import (
    _resolve_paths_and_filesystem,
    _split_uri,
)

logger = logging.getLogger(__name__)

URI_DOWNLOAD_MAX_WORKERS = 16

RAY_DATA_USE_OBSTORE = os.environ.get("RAY_DATA_USE_OBSTORE", "1") == "1"

# Range-split downloads: files above this threshold (bytes) are downloaded as
# parallel get_range requests instead of a single get.  0 = disabled (default).
RAY_DATA_OBSTORE_RANGE_THRESHOLD = int(
    os.environ.get("RAY_DATA_OBSTORE_RANGE_THRESHOLD", "0")
)
RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE = int(
    os.environ.get("RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE", str(8 * 1024 * 1024))
)
RAY_DATA_OBSTORE_MAX_CONCURRENCY = int(
    os.environ.get("RAY_DATA_OBSTORE_MAX_CONCURRENCY", "128")
)
_FILE_SIZE_COLUMN_PREFIX = "__ray_file_size__"

try:
    from obstore import parse_scheme as obstore_parse_scheme

    OBSTORE_AVAILABLE = RAY_DATA_USE_OBSTORE
except ImportError:
    OBSTORE_AVAILABLE = False
    obstore_parse_scheme = None

_obstore_fallback_warned = False


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
        # PartitionActor is a callable class, so we need ActorPoolStrategy
        partition_compute = ActorPoolStrategy(
            size=1, enable_true_multi_threading=True
        )  # Use single actor for partitioning

        fn, init_fn = _get_udf(
            PartitionActor,
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

    fn, init_fn = _get_udf(
        download_bytes_threaded,
        (
            uri_column_names,
            output_bytes_column_names,
            data_context,
            filesystem,
            OBSTORE_AVAILABLE,
        ),
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


def _extract_credentials_from_filesystem(
    filesystem: Optional["pa.fs.FileSystem"],
) -> Dict[str, Any]:
    """Extract credentials from a PyArrow filesystem for use with obstore.

    Maps PyArrow filesystem configuration to obstore keyword arguments.
    See obstore docs for available options per store type.

    Only native PyArrow filesystems (S3FileSystem, GcsFileSystem,
    AzureFileSystem) are recognized. fsspec-backed filesystems arrive here as
    a PyArrow ``PyFileSystem`` wrapper and are **not** handled — their
    credentials are silently ignored and obstore will use its own credential
    chain (environment variables, instance metadata, etc.) instead.

    Args:
        filesystem: A PyArrow filesystem instance.

    Returns:
        A dict of keyword arguments to pass to obstore's ``from_url``.
    """
    if filesystem is None:
        return {}

    # Unwrap RetryingPyFileSystem if present
    if isinstance(filesystem, RetryingPyFileSystem):
        filesystem = filesystem.unwrap()

    kwargs: Dict[str, Any] = {}

    # Import filesystem types for isinstance checks
    try:
        from pyarrow.fs import S3FileSystem
    except ImportError:
        S3FileSystem = None

    try:
        from pyarrow.fs import GcsFileSystem
    except ImportError:
        GcsFileSystem = None

    try:
        from pyarrow.fs import AzureFileSystem
    except ImportError:
        AzureFileSystem = None

    if S3FileSystem is not None and isinstance(filesystem, S3FileSystem):
        if hasattr(filesystem, "region") and filesystem.region:
            kwargs["region"] = filesystem.region
        if hasattr(filesystem, "access_key") and filesystem.access_key:
            kwargs["access_key_id"] = filesystem.access_key
        if hasattr(filesystem, "secret_key") and filesystem.secret_key:
            kwargs["secret_access_key"] = filesystem.secret_key
        if hasattr(filesystem, "session_token") and filesystem.session_token:
            kwargs["session_token"] = filesystem.session_token
        if hasattr(filesystem, "endpoint_override") and filesystem.endpoint_override:
            kwargs["endpoint"] = filesystem.endpoint_override
        if hasattr(filesystem, "anonymous") and filesystem.anonymous:
            kwargs["skip_signature"] = True

    elif GcsFileSystem is not None and isinstance(filesystem, GcsFileSystem):
        # obstore GCSConfig does not have a project_id field. The only useful
        # attribute PyArrow exposes on GcsFileSystem is `anonymous`, which maps
        # to obstore's `skip_signature`. All other credentials (service account,
        # application default credentials) are resolved by obstore from the
        # environment automatically.
        if hasattr(filesystem, "anonymous") and filesystem.anonymous:
            kwargs["skip_signature"] = True

    elif AzureFileSystem is not None and isinstance(filesystem, AzureFileSystem):
        if hasattr(filesystem, "account_name") and filesystem.account_name:
            kwargs["account_name"] = filesystem.account_name
        if hasattr(filesystem, "account_key") and filesystem.account_key:
            kwargs["account_key"] = filesystem.account_key

    return kwargs


def _is_obstore_supported_url(path: str) -> bool:
    """Check if *path* is a URL that obstore can handle.

    obstore supports ``s3://``, ``gs://``, ``az://``, ``file://``,
    ``http://``, and ``https://`` schemes.  Uses obstore's native
    ``parse_scheme`` when available.

    Returns ``False`` for relative paths or unsupported schemes.
    This function should only be called when obstore is known to be available.
    """
    try:
        obstore_parse_scheme(path)
        return True
    except Exception:
        return False


async def _download_uris_with_obstore(
    uris: List[str],
    uri_column_name: str,
    filesystem: Optional["pa.fs.FileSystem"] = None,
    file_sizes: Optional[List[Optional[int]]] = None,
) -> List[Optional[bytes]]:
    """Download URIs concurrently using obstore's async API.

    Creates and caches one ``ObjectStore`` per unique (scheme, authority)
    combination, then fires all downloads concurrently via asyncio.gather.
    If a PyArrow *filesystem* is provided, its credentials are extracted
    and forwarded to obstore's store construction.

    When ``RAY_DATA_OBSTORE_RANGE_THRESHOLD`` is set to a positive value,
    files larger than the threshold are downloaded as parallel range chunks
    via ``get_range_async``.  The *file_sizes* list, when provided by the
    upstream ``PartitionActor``, lets the function skip the HEAD request
    for files whose size is already known.

    Args:
        uris: URIs to download.
        uri_column_name: Column name (used only for error logging).
        filesystem: Optional PyArrow filesystem whose credentials are
            forwarded to the obstore store.
        file_sizes: Optional per-URI file sizes from the PartitionActor.
            ``0`` or ``None`` entries trigger a HEAD request when range
            splitting is enabled.

    Returns:
        Downloaded bytes in the same order as *uris*.  ``None`` entries
        indicate failed downloads.
    """
    import obstore as obs
    from obstore.store import from_url

    store_cache: dict = {}
    retry_config = {"max_retries": 10}
    fs_kwargs = _extract_credentials_from_filesystem(filesystem)

    range_threshold = RAY_DATA_OBSTORE_RANGE_THRESHOLD
    range_chunk_size = RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE
    max_conc = RAY_DATA_OBSTORE_MAX_CONCURRENCY
    if range_threshold > 0 and max_conc <= 0:
        logger.warning(
            "RAY_DATA_OBSTORE_RANGE_THRESHOLD is set but "
            "RAY_DATA_OBSTORE_MAX_CONCURRENCY=%d is invalid. "
            "Range downloads require a positive concurrency limit to avoid "
            "socket exhaustion. Disabling range splitting.",
            max_conc,
        )
        range_threshold = 0
    sem = (
        asyncio.Semaphore(max_conc) if (range_threshold > 0 and max_conc > 0) else None
    )

    # obstore's reqwest client rejects http:// by default. Auto-enable it
    # to maintain parity with PyArrow (which accepts http:// via fsspec),
    # but warn about unencrypted traffic.
    if any(uri.startswith("http://") for uri in uris):
        fs_kwargs["client_options"] = {"allow_http": True}
        logger.warning(
            "Downloading over unencrypted HTTP. " "Consider using https:// instead."
        )

    def _get_store(store_url: str):
        if store_url not in store_cache:
            store_cache[store_url] = from_url(
                store_url, retry_config=retry_config, **fs_kwargs
            )
        return store_cache[store_url]

    async def _download_one_simple(uri: str) -> Optional[bytes]:
        try:
            store_url, path = _split_uri(uri)
            store = _get_store(store_url)
            if sem is not None:
                async with sem:
                    result = await obs.get_async(store, path)
                    return bytes(await result.bytes_async())
            else:
                # No semaphore, indicate we not using range splitting.
                # Number of concurrent connections controlled by PartitionActor.
                result = await obs.get_async(store, path)
                return bytes(await result.bytes_async())
        except OSError as e:
            logger.debug(
                f"OSError reading uri '{uri}' for column '{uri_column_name}': {e}"
            )
            return None
        except Exception as e:
            logger.warning(
                f"Unexpected error reading uri '{uri}' for column "
                f"'{uri_column_name}': {e}"
            )
            return None

    async def _download_one_ranged(uri: str, size: int) -> Optional[bytes]:
        """Download a single large file using parallel range requests."""
        try:
            store_url, path = _split_uri(uri)
            store = _get_store(store_url)
            result = bytearray(size)

            async def _fetch_range(start: int, end: int) -> None:
                async with sem:
                    chunk = await obs.get_range_async(store, path, start=start, end=end)
                    result[start:end] = chunk

            ranges = [
                (start, min(start + range_chunk_size, size))
                for start in range(0, size, range_chunk_size)
            ]
            await asyncio.gather(*[_fetch_range(s, e) for s, e in ranges])
            return bytes(result)
        except OSError as e:
            logger.debug(
                f"OSError reading uri '{uri}' for column '{uri_column_name}': {e}"
            )
            return None
        except Exception as e:
            logger.warning(
                f"Unexpected error reading uri '{uri}' for column "
                f"'{uri_column_name}': {e}"
            )
            return None

    if range_threshold <= 0:
        tasks = [_download_one_simple(uri) for uri in uris]
        return list(await asyncio.gather(*tasks))

    # --- Range-split path ---
    sizes = list(file_sizes) if file_sizes is not None else [0] * len(uris)

    # Resolve unknown file sizes via HEAD. The cost is one concurrent RTT
    # regardless of batch size, which is negligible compared to the speedup
    # from correctly routing large files to ranged download.
    unknown_indices = [i for i, s in enumerate(sizes) if not s or s <= 0]
    if unknown_indices:

        async def _head(idx: int):
            try:
                store_url, path = _split_uri(uris[idx])
                store = _get_store(store_url)
                if sem is not None:
                    async with sem:
                        meta = await obs.head_async(store, path)
                else:
                    meta = await obs.head_async(store, path)
                sizes[idx] = meta["size"] if isinstance(meta, dict) else meta.size
            except Exception:
                sizes[idx] = 0

        await asyncio.gather(*[_head(i) for i in unknown_indices])

    tasks = []
    for uri, size in zip(uris, sizes):
        if size and size > range_threshold:
            tasks.append(_download_one_ranged(uri, size))
        else:
            tasks.append(_download_one_simple(uri))
    return list(await asyncio.gather(*tasks))


def _download_uris_with_pyarrow(
    uris: List[str],
    uri_column_name: str,
    data_context: DataContext,
    filesystem: Optional["pa.fs.FileSystem"] = None,
) -> List[Optional[bytes]]:
    """Download URIs concurrently using a PyArrow filesystem thread pool.

    Uses lazy filesystem resolution: the filesystem is resolved from the first
    URI and reused for all subsequent URIs in the batch. Downloads run in
    parallel across ``URI_DOWNLOAD_MAX_WORKERS`` threads via ``make_async_gen``,
    with ordering preserved to match the input URI list.

    Args:
        uris: URIs to download.
        uri_column_name: Column name (used only for error logging).
        data_context: Ray Data context, used for retryable error configuration.
        filesystem: PyArrow filesystem to use. If None, auto-detected from the
            URI scheme.

    Returns:
        Downloaded bytes in the same order as *uris*. ``None`` entries indicate
        failed downloads.
    """

    def load_uri_bytes(uri_iterator):
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
                    resolved_fs,
                    retryable_errors=data_context.retried_io_errors,
                )
                # We only pass one uri to resolve and unwrap it from the list of resolved paths,
                # if fails, we will catch the index error and log it.
                resolved_path = resolved_paths[0]
                if resolved_path is None:
                    continue

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

    return list(
        make_async_gen(
            base_iterator=iter(uris),
            fn=load_uri_bytes,
            preserve_ordering=True,
            num_workers=URI_DOWNLOAD_MAX_WORKERS,
        )
    )


def download_bytes_threaded(
    block: pa.Table,
    uri_column_names: List[str],
    output_bytes_column_names: List[str],
    data_context: DataContext,
    filesystem: Optional["pa.fs.FileSystem"] = None,
    use_obstore: bool = False,
) -> Iterator[pa.Table]:
    """Download bytes for URI columns, appending them as new columns.

    Supports downloading from multiple URI columns in a single operation.
    When ``use_obstore`` is True and obstore is installed, uses obstore's async
    API for higher concurrency and Rust-level connection pooling. Otherwise
    falls back to threaded downloads through PyArrow's filesystem abstraction.

    Args:
        block: Input PyArrow table containing URI columns.
        uri_column_names: Names of columns containing URIs to download.
        output_bytes_column_names: Names for the output columns containing
            downloaded bytes.
        data_context: Ray Data context for configuration.
        filesystem: PyArrow filesystem to use for reading remote files.
            If None, the filesystem is auto-detected from the path scheme.
        use_obstore: Whether to use obstore's async download path.

    Yields:
        pa.Table: PyArrow table with the downloaded bytes added as new columns.
    """
    if not isinstance(block, pa.Table):
        block = BlockAccessor.for_block(block).to_arrow()

    output_block = block

    for uri_column_name, output_bytes_column_name in zip(
        uri_column_names, output_bytes_column_names
    ):
        uris = output_block.column(uri_column_name).to_pylist()

        if len(uris) == 0:
            continue

        # Read pre-computed file sizes from the PartitionActor if available.
        size_col = f"{_FILE_SIZE_COLUMN_PREFIX}{uri_column_name}"
        file_sizes = None
        if size_col in output_block.column_names:
            file_sizes = output_block.column(size_col).to_pylist()

        # Intentionally check only the first URI to determine the download backend.
        # Checking every URI would add O(n) overhead per batch for negligible practical benefit.
        #
        # NOTE: asyncio.run() creates and tears down a new event loop per batch.
        # This will raise RuntimeError if called from within an already-running event loop.
        if use_obstore and _is_obstore_supported_url(uris[0]):
            uri_bytes = asyncio.run(
                _download_uris_with_obstore(
                    uris,
                    uri_column_name,
                    filesystem=filesystem,
                    file_sizes=file_sizes,
                )
            )
        else:
            global _obstore_fallback_warned
            if not _obstore_fallback_warned and not use_obstore:
                _obstore_fallback_warned = True
                logger.warning(
                    "obstore is not installed — falling back to the PyArrow "
                    "download path, which is significantly slower for large "
                    "or numerous files. Install it with: pip install obstore"
                )
            uri_bytes = _download_uris_with_pyarrow(
                uris, uri_column_name, data_context, filesystem=filesystem
            )

        # Add the new column to the PyArrow table
        output_block = output_block.add_column(
            len(output_block.column_names),
            output_bytes_column_name,
            pa.array(uri_bytes),
        )

    # Drop internal file-size columns before yielding output.
    for uri_column_name in uri_column_names:
        size_col = f"{_FILE_SIZE_COLUMN_PREFIX}{uri_column_name}"
        if size_col in output_block.column_names:
            idx = output_block.column_names.index(size_col)
            output_block = output_block.remove_column(idx)

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

    def __init__(
        self,
        uri_column_names: List[str],
        data_context: DataContext,
        filesystem: Optional["pa.fs.FileSystem"] = None,
    ):
        self._uri_column_names = uri_column_names
        self._data_context = data_context
        self._filesystem = filesystem
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

        if RAY_DATA_OBSTORE_RANGE_THRESHOLD > 0:
            block = self._attach_file_sizes(block)

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

    def _attach_file_sizes(self, block: pa.Table) -> pa.Table:
        """Fetch file sizes for all URIs and attach as hidden columns.

        For cloud URIs (S3/GCS/Azure) this uses cheap metadata lookups.
        For HTTP URIs where sizes are unavailable, stores 0 so the
        downstream download function falls back to HEAD via obstore.
        """
        for uri_column_name in self._uri_column_names:
            col_name = f"{_FILE_SIZE_COLUMN_PREFIX}{uri_column_name}"
            uris = block.column(uri_column_name).to_pylist()
            sizes = self._sample_sizes(
                uris
            )  # This fetches all file sizes instead of just sampling
            block = block.append_column(col_name, pa.array(sizes, type=pa.int64()))
        return block

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
            paths, fs = _resolve_paths_and_filesystem(uris, filesystem=self._filesystem)
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
