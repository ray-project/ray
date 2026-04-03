import asyncio
import logging
import os
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

if TYPE_CHECKING:
    from ray.data.context import DataContext

import pyarrow as pa
import pyarrow.fs

from ray.data._internal.util import (
    RetryingPyFileSystem,
    _iter_arrow_table_for_target_max_block_size,
)
from ray.data.block import BlockAccessor
from ray.data.datasource.path_util import _split_uri

try:
    from obstore import parse_scheme as obstore_parse_scheme
except ImportError:
    obstore_parse_scheme = None

logger = logging.getLogger(__name__)


def _parse_obstore_int_env(var_name: str, default: int) -> int:
    """Parse a non-negative Ray Data obstore env var without breaking import.

    Malformed values log a warning and fall back to default so bad config in
    the environment cannot prevent ``_obstore_download`` from loading.
    """
    raw = os.environ.get(var_name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip(), 10)
    except ValueError:
        logger.warning(
            "Ignoring invalid integer for %s=%r — using default %s",
            var_name,
            raw,
            default,
        )
        return default


# Constants & configuration
RAY_DATA_USE_OBSTORE = os.environ.get("RAY_DATA_USE_OBSTORE", "1") == "1"
OBSTORE_AVAILABLE = RAY_DATA_USE_OBSTORE and obstore_parse_scheme is not None

_DEFAULT_RANGE_THRESHOLD_INT = 4 * 1024 * 1024  # 4 MB
_DEFAULT_RANGE_CHUNK_SIZE_INT = 8 * 1024 * 1024  # 8 MB
_DEFAULT_MAX_CONCURRENCY_INT = 128

# Range-split downloads: files above this threshold (bytes) are downloaded as
# parallel get_range requests instead of a single get.  Default 4 MB — below
# the crossover where ranged overhead exceeds the single-GET time, but above
# it large/skewed workloads see 10-25x speedup.  Set to 0 to disable.
RAY_DATA_OBSTORE_RANGE_THRESHOLD = _parse_obstore_int_env(
    "RAY_DATA_OBSTORE_RANGE_THRESHOLD", _DEFAULT_RANGE_THRESHOLD_INT
)
RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE = _parse_obstore_int_env(
    "RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE", _DEFAULT_RANGE_CHUNK_SIZE_INT
)
RAY_DATA_OBSTORE_MAX_CONCURRENCY = _parse_obstore_int_env(
    "RAY_DATA_OBSTORE_MAX_CONCURRENCY", _DEFAULT_MAX_CONCURRENCY_INT
)

_FILE_SIZE_COLUMN_PREFIX = "__ray_file_size__"

# Public utilities (exported to plan_download_op.py)
_fallback_warned = False


def _log_fallback_warning() -> None:
    """Log a one-time warning when falling back to the PyArrow download path."""
    global _fallback_warned
    if _fallback_warned:
        return
    _fallback_warned = True
    if not RAY_DATA_USE_OBSTORE:
        logger.info(
            "obstore disabled via RAY_DATA_USE_OBSTORE=0 — "
            "using the PyArrow download path."
        )
    else:
        logger.warning(
            "obstore is not installed — falling back to the "
            "PyArrow download path, which is significantly "
            "slower for large or numerous files. "
            "Install it with: pip install obstore"
        )


def _is_obstore_supported_url(path: str) -> bool:
    """Check if *path* is a URL that obstore can handle.

    obstore supports ``s3://``, ``gs://``, ``az://``, ``file://``,
    ``http://``, and ``https://`` schemes.  Uses obstore's native
    ``parse_scheme`` when available.

    Returns ``False`` for relative paths or unsupported schemes.
    This function should only be called when obstore is known to be available.
    """
    if obstore_parse_scheme is None:
        return False
    try:
        obstore_parse_scheme(path)
        return True
    except Exception:
        return False


# Credential extraction & store management
def _extract_credentials_from_filesystem(
    filesystem: Optional["pyarrow.fs.FileSystem"],
) -> Dict[str, Any]:
    """Extract credentials from a PyArrow filesystem for use with obstore.

    Maps PyArrow filesystem configuration to obstore keyword arguments.
    See obstore docs for available options per store type.

    **Native S3 (``S3FileSystem``):** PyArrow's implementation is backed by the
    AWS C++ SDK; Python only exposes a subset of options. ``region`` is typically
    readable, while ``access_key``, ``secret_key``, ``session_token``, and
    ``anonymous`` may not appear as Python attributes even when configured at
    construction. Unavailable attributes are skipped silently, so forwarding to
    obstore can degrade to the default AWS credential chain (env, IMDS, etc.)
    without raising.

    **fsspec S3 (``PyFileSystem`` + ``FSSpecHandler``):** For ``s3`` / ``s3a``
    (e.g. ``s3fs`` with STS/Okta/custom endpoints), credentials are read from
    ``storage_options`` / common instance attributes so obstore can use the
    same keys the user passed to fsspec. ``anon`` maps to ``skip_signature``;
    ``region_name`` may appear in ``storage_options`` or ``client_kwargs``.

    Other ``PyFileSystem`` handlers (non-fsspec or non-S3 fsspec protocols) are
    not converted here; see :func:`_obstore_filesystem_requires_threaded_download`.

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

    S3FileSystem = getattr(pyarrow.fs, "S3FileSystem", None)
    GcsFileSystem = getattr(pyarrow.fs, "GcsFileSystem", None)
    AzureFileSystem = getattr(pyarrow.fs, "AzureFileSystem", None)
    PyFileSystem = getattr(pyarrow.fs, "PyFileSystem", None)
    FSSpecHandler = getattr(pyarrow.fs, "FSSpecHandler", None)

    if S3FileSystem is not None and isinstance(filesystem, S3FileSystem):
        # NOTE: See docstring — many credential fields are not exposed on the
        # Python S3FileSystem object; hasattr/getattr only forwards what exists.
        for pa_attr, ob_key in [
            ("region", "region"),
            ("access_key", "access_key_id"),
            ("secret_key", "secret_access_key"),
            ("session_token", "session_token"),
            ("endpoint_override", "endpoint"),
        ]:
            val = getattr(filesystem, pa_attr, None)
            if val:
                kwargs[ob_key] = val
        if getattr(filesystem, "anonymous", False):
            kwargs["skip_signature"] = True

    elif GcsFileSystem is not None and isinstance(filesystem, GcsFileSystem):
        # obstore GCSConfig does not have a project_id field. The only useful
        # attribute PyArrow exposes on GcsFileSystem is `anonymous`, which maps
        # to obstore's `skip_signature`. All other credentials (service account,
        # application default credentials) are resolved by obstore from the
        # environment automatically.
        if getattr(filesystem, "anonymous", False):
            kwargs["skip_signature"] = True

    elif AzureFileSystem is not None and isinstance(filesystem, AzureFileSystem):
        for attr in ("account_name", "account_key"):
            val = getattr(filesystem, attr, None)
            if val:
                kwargs[attr] = val

    elif (
        PyFileSystem is not None
        and FSSpecHandler is not None
        and isinstance(filesystem, PyFileSystem)
        and isinstance(filesystem.handler, FSSpecHandler)
    ):
        # fsspec-backed FS (e.g. s3fs with Okta/STS) wrapped for PyArrow.
        fsspec_fs = getattr(filesystem.handler, "fs", None)
        if fsspec_fs is None:
            return {}
        protocol = getattr(fsspec_fs, "protocol", None)
        if isinstance(protocol, tuple):
            protocol = protocol[0] if protocol else None
        if protocol in ("s3", "s3a"):
            opts = getattr(fsspec_fs, "storage_options", None) or {}
            if not isinstance(opts, dict):
                opts = {}
            key = opts.get("key") or getattr(fsspec_fs, "key", None)
            secret = opts.get("secret") or getattr(fsspec_fs, "secret", None)
            token = opts.get("token") or getattr(fsspec_fs, "token", None)
            endpoint = opts.get("endpoint_url") or getattr(
                fsspec_fs, "endpoint_url", None
            )
            client_kwargs = opts.get("client_kwargs") or getattr(
                fsspec_fs, "client_kwargs", None
            )
            if isinstance(client_kwargs, dict):
                endpoint = endpoint or client_kwargs.get("endpoint_url")
                region_name = client_kwargs.get("region_name")
                if region_name:
                    kwargs["region"] = region_name
            region_opt = opts.get("region_name")
            if region_opt:
                kwargs["region"] = region_opt
            if key:
                kwargs["access_key_id"] = key
            if secret:
                kwargs["secret_access_key"] = secret
            if token:
                kwargs["session_token"] = token
            if endpoint:
                kwargs["endpoint"] = endpoint
            if opts.get("anon") or getattr(fsspec_fs, "anon", False):
                kwargs["skip_signature"] = True
    return kwargs


def _obstore_filesystem_requires_threaded_download(
    filesystem: Optional["pyarrow.fs.FileSystem"],
) -> bool:
    """Return True if obstore must not be used so the user's FS stays authoritative.

    ``PyFileSystem`` instances that are not ``FSSpecHandler`` wrapping ``s3`` /
    ``s3a`` cannot have credentials merged into obstore's ``from_url`` in a
    reliable way. Using obstore with empty kwargs would ignore the user's
    filesystem (Okta, STS, custom protocols) and tends to produce confusing
    403s. The threaded download path passes the filesystem through to PyArrow
    ``open_input_stream``, matching pre-obstore behavior.

    ``None`` means no explicit filesystem (URI-driven resolution) — obstore is OK.
    """
    if filesystem is None:
        return False

    if isinstance(filesystem, RetryingPyFileSystem):
        filesystem = filesystem.unwrap()

    PyFileSystem = getattr(pyarrow.fs, "PyFileSystem", None)
    FSSpecHandler = getattr(pyarrow.fs, "FSSpecHandler", None)
    if PyFileSystem is None or not isinstance(filesystem, PyFileSystem):
        return False
    if FSSpecHandler is None or not isinstance(filesystem.handler, FSSpecHandler):
        return True

    fsspec_fs = getattr(filesystem.handler, "fs", None)
    if fsspec_fs is None:
        return True

    protocol = getattr(fsspec_fs, "protocol", None)
    if isinstance(protocol, tuple):
        protocol = protocol[0] if protocol else None
    if protocol in ("s3", "s3a"):
        return False
    return True


class StoreRegistry:
    """Cache of store_url -> ObjectStore instances.

    Ensures one store is created per unique (scheme, authority) combination
    and reuses it for all subsequent requests to the same host.
    """

    def __init__(
        self,
        retry_config: Optional[Dict[str, Any]] = None,
        **filesystem_kwargs: Any,
    ):
        from obstore.store import from_url

        self._from_url = from_url
        self._retry_config = retry_config or {}
        self._filesystem_kwargs = filesystem_kwargs
        self._cache: Dict[str, Any] = {}

    def get(self, store_url: str) -> Any:
        if store_url not in self._cache:
            kwargs = dict(self._filesystem_kwargs)
            if store_url.startswith("http://"):
                # obstore's reqwest client rejects http:// by default. Auto-enable it
                # to maintain parity with PyArrow (which accepts http:// via fsspec),
                # but warn about unencrypted traffic.
                co = {**kwargs.get("client_options", {}), "allow_http": True}
                kwargs["client_options"] = co
                if not getattr(self, "_warned_http", False):
                    self._warned_http = True
                    logger.warning(
                        "Downloading over unencrypted HTTP. "
                        "Consider using https:// instead."
                    )
            self._cache[store_url] = self._from_url(
                store_url,
                retry_config=self._retry_config,
                **kwargs,
            )
        return self._cache[store_url]


def _yield_threaded_download_bytes(
    block: pa.Table,
    uri_column_names: List[str],
    output_bytes_column_names: List[str],
    data_context: "DataContext",
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
) -> Iterator[pa.Table]:
    """Delegate to ``download_bytes_threaded`` after dropping internal size columns.

    ``AsyncPartitionActor`` may attach ``__ray_file_size__*`` columns that the
    threaded downloader does not handle.
    """
    size_cols = [
        f"{_FILE_SIZE_COLUMN_PREFIX}{name}"
        for name in uri_column_names
        if f"{_FILE_SIZE_COLUMN_PREFIX}{name}" in block.column_names
    ]
    if size_cols:
        block = block.drop(size_cols)

    from ray.data._internal.planner.plan_download_op import download_bytes_threaded

    yield from download_bytes_threaded(
        block,
        uri_column_names,
        output_bytes_column_names,
        data_context,
        filesystem,
    )


# Public entry point
def download_bytes_async(
    block: pa.Table,
    uri_column_names: List[str],
    output_bytes_column_names: List[str],
    data_context: "DataContext",
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
) -> Iterator[pa.Table]:
    """Download bytes for URI columns using obstore's async API.

    For URI schemes not supported by obstore, falls back to the threaded
    PyArrow download path via lazy import.

    Args:
        block: Input PyArrow table containing URI columns.
        uri_column_names: Names of columns containing URIs to download.
        output_bytes_column_names: Names for the output columns containing
            downloaded bytes.
        data_context: Ray Data context for configuration.
        filesystem: PyArrow filesystem to use for reading remote files.
            If None, the filesystem is auto-detected from the path scheme.

    Yields:
        pa.Table: PyArrow table with the downloaded bytes added as new columns.
    """
    if not isinstance(block, pa.Table):
        block = BlockAccessor.for_block(block).to_arrow()

    first_uris = block.column(uri_column_names[0]).to_pylist()
    if not first_uris:
        yield block
        return

    # Fall back to PyArrow for URI schemes obstore doesn't handle.
    if not _is_obstore_supported_url(first_uris[0]):
        logger.debug(
            "URI scheme not supported by obstore (first URI: %s); "
            "falling back to PyArrow threaded download.",
            first_uris[0],
        )
        yield from _yield_threaded_download_bytes(
            block,
            uri_column_names,
            output_bytes_column_names,
            data_context,
            filesystem,
        )
        return

    if _obstore_filesystem_requires_threaded_download(filesystem):
        logger.debug(
            "PyArrow PyFileSystem with a non-S3 fsspec backend (or unknown handler); "
            "using threaded PyArrow download to preserve user filesystem credentials."
        )
        yield from _yield_threaded_download_bytes(
            block,
            uri_column_names,
            output_bytes_column_names,
            data_context,
            filesystem,
        )
        return

    output_block = block

    for uri_column_name, output_bytes_column_name in zip(
        uri_column_names, output_bytes_column_names
    ):
        uris = output_block.column(uri_column_name).to_pylist()

        if not uris:
            continue

        # Read pre-computed file sizes from AsyncPartitionActor if available.
        size_col = f"{_FILE_SIZE_COLUMN_PREFIX}{uri_column_name}"
        file_sizes = None
        if size_col in output_block.column_names:
            file_sizes = output_block.column(size_col).to_pylist()

        uri_bytes = asyncio.run(
            _download_uris_with_obstore(
                uris, uri_column_name, filesystem=filesystem, file_sizes=file_sizes
            )
        )

        output_block = output_block.append_column(
            output_bytes_column_name, pa.array(uri_bytes)
        )

    # Drop internal file-size columns before yielding output.
    size_cols = [
        f"{_FILE_SIZE_COLUMN_PREFIX}{name}"
        for name in uri_column_names
        if f"{_FILE_SIZE_COLUMN_PREFIX}{name}" in output_block.column_names
    ]
    if size_cols:
        output_block = output_block.drop(size_cols)

    yield from _iter_arrow_table_for_target_max_block_size(
        output_block, data_context.target_max_block_size
    )


# Core async download logic
async def _download_uris_with_obstore(
    uris: List[str],
    uri_column_name: str,
    filesystem: Optional["pyarrow.fs.FileSystem"] = None,
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
    upstream ``AsyncPartitionActor``, lets the function skip the HEAD request
    for files whose size is already known.

    Args:
        uris: URIs to download.
        uri_column_name: Column name (used only for error logging).
        filesystem: Optional PyArrow filesystem whose credentials are
            forwarded to the obstore store.
        file_sizes: Optional per-URI file sizes from AsyncPartitionActor.
            ``0`` or ``None`` entries trigger a HEAD request when range
            splitting is enabled.

    Returns:
        Downloaded bytes in the same order as *uris*.  ``None`` entries
        indicate failed downloads.
    """
    fs_kwargs = _extract_credentials_from_filesystem(filesystem)

    range_threshold = RAY_DATA_OBSTORE_RANGE_THRESHOLD
    range_chunk_size = RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE
    max_conc = RAY_DATA_OBSTORE_MAX_CONCURRENCY
    if range_threshold > 0:
        if max_conc <= 0:
            logger.warning(
                "RAY_DATA_OBSTORE_RANGE_THRESHOLD is set but "
                "RAY_DATA_OBSTORE_MAX_CONCURRENCY=%d is invalid. "
                "Range downloads require a positive concurrency limit to avoid "
                "socket exhaustion. Disabling range splitting.",
                max_conc,
            )
            range_threshold = 0
        elif range_chunk_size <= 0:
            logger.warning(
                "RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE=%d is invalid (must be > 0). "
                "Disabling range splitting to avoid empty range tasks and "
                "zero-filled bogus downloads.",
                range_chunk_size,
            )
            range_threshold = 0
    sem = asyncio.Semaphore(max_conc) if max_conc > 0 else None

    registry = StoreRegistry(retry_config={"max_retries": 10}, **fs_kwargs)

    if range_threshold <= 0:
        logger.debug(
            "Range splitting disabled (threshold=%d); downloading %d URIs via simple GET.",
            range_threshold,
            len(uris),
        )
        tasks = [_fetch_whole(uri, registry, uri_column_name, sem) for uri in uris]
        return list(await asyncio.gather(*tasks))

    # --- Range-split path ---
    assert sem is not None
    # 0 = unknown size; these will be resolved via HEAD below.
    sizes = list(file_sizes) if file_sizes is not None else [0] * len(uris)

    # Resolve unknown file sizes via HEAD. The cost is one concurrent RTT
    # regardless of batch size, which is negligible compared to the speedup
    # from correctly routing large files to ranged download.
    unknown_indices = [i for i, s in enumerate(sizes) if not s or s <= 0]
    if unknown_indices:
        resolved = await asyncio.gather(
            *[_resolve_size(uris[i], registry, sem) for i in unknown_indices]
        )
        for i, sz in zip(unknown_indices, resolved):
            sizes[i] = sz

    tasks = []
    for uri, size in zip(uris, sizes):
        if size and size > range_threshold:
            tasks.append(
                _fetch_ranged(
                    uri, size, registry, uri_column_name, sem, range_chunk_size
                )
            )
        else:
            tasks.append(_fetch_whole(uri, registry, uri_column_name, sem))
    return list(await asyncio.gather(*tasks))


# Async helpers
async def _resolve_size(
    uri: str,
    registry: StoreRegistry,
    semaphore: asyncio.Semaphore,
) -> int:
    """Return the file size in bytes via a HEAD request, or 0 on failure.

    Returning 0 on failure is intentional: callers treat 0 as "unknown size",
    which routes the file to a simple GET instead of ranged download.
    """
    import obstore as obs

    try:
        store_url, path = _split_uri(uri)
        store = registry.get(store_url)
        async with semaphore:
            meta = await obs.head_async(store, path)
        return meta["size"] if isinstance(meta, dict) else meta.size
    except Exception:
        return 0


async def _fetch_whole(
    uri: str,
    registry: StoreRegistry,
    uri_column_name: str,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> Optional[bytes]:
    """Download a single URI, returning ``None`` on failure."""
    try:
        if semaphore is not None:
            async with semaphore:
                return await _fetch(uri, registry)
        # No semaphore (RAY_DATA_OBSTORE_MAX_CONCURRENCY=0).
        # Concurrency is bounded only by the partition actor batch size.
        return await _fetch(uri, registry)
    except OSError as e:
        logger.debug(
            "OSError reading uri %r for column %r: %s", uri, uri_column_name, e
        )
    except Exception as e:
        logger.warning(
            "Unexpected error reading uri %r for column %r: %s",
            uri,
            uri_column_name,
            e,
        )
    return None


async def _fetch_ranged(
    uri: str,
    size: int,
    registry: StoreRegistry,
    uri_column_name: str,
    semaphore: asyncio.Semaphore,
    range_chunk_size: int,
) -> Optional[bytes]:
    """Download a single large file using parallel range requests.

    Falls back to a simple GET if any range chunk fails, so the
    pipeline never loses a file due to a transient range error.
    """
    try:
        store_url, path = _split_uri(uri)
        store = registry.get(store_url)
        result = bytearray(size)

        tasks = [
            asyncio.ensure_future(
                _fetch_chunk(
                    uri,
                    store,
                    path,
                    chunk_start,
                    min(chunk_start + range_chunk_size, size),
                    result,
                    semaphore,
                )
            )
            for chunk_start in range(0, size, range_chunk_size)
        ]

        try:
            await asyncio.gather(*tasks)
        except Exception:
            # Cancel remaining chunk tasks to release semaphore permits
            # and avoid wasted downloads into a bytearray we'll discard.
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        return bytes(result)
    except Exception as e:
        logger.debug(
            "Ranged download failed for %r, falling back to simple GET: %s", uri, e
        )
        return await _fetch_whole(uri, registry, uri_column_name, semaphore)


async def _fetch_chunk(
    uri: str,
    store: Any,
    path: str,
    start: int,
    end: int,
    result: bytearray,
    semaphore: asyncio.Semaphore,
) -> None:
    """Download a single byte range and write it into *result* in place."""
    import obstore as obs

    async with semaphore:
        chunk = await obs.get_range_async(store, path, start=start, end=end)
        expected = end - start
        if len(chunk) != expected:
            raise IOError(
                f"Range request for {uri!r} returned {len(chunk)} "
                f"bytes, expected {expected}"
            )
        result[start:end] = chunk


async def _fetch(uri: str, registry: StoreRegistry) -> bytes:
    """Download a single URI as a whole-file GET and return raw bytes."""
    import obstore as obs

    store_url, path = _split_uri(uri)
    result = await obs.get_async(registry.get(store_url), path)
    return bytes(await result.bytes_async())
