import asyncio
import logging
import os
from typing import Any, Dict, List, Optional

import pyarrow as pa

from ray.data._internal.util import RetryingPyFileSystem
from ray.data.datasource.path_util import _split_uri

logger = logging.getLogger(__name__)

RAY_DATA_USE_OBSTORE = os.environ.get("RAY_DATA_USE_OBSTORE", "1") == "1"

_DEFAULT_RANGE_THRESHOLD = str(4 * 1024 * 1024)  # 4 MB
_DEFAULT_RANGE_CHUNK_SIZE = str(8 * 1024 * 1024)  # 8 MB

# Range-split downloads: files above this threshold (bytes) are downloaded as
# parallel get_range requests instead of a single get.  Default 4 MB — below
# the crossover where ranged overhead exceeds the single-GET time, but above
# it large/skewed workloads see 10-25x speedup.  Set to 0 to disable.
RAY_DATA_OBSTORE_RANGE_THRESHOLD = int(
    os.environ.get("RAY_DATA_OBSTORE_RANGE_THRESHOLD", _DEFAULT_RANGE_THRESHOLD)
)
RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE = int(
    os.environ.get("RAY_DATA_OBSTORE_RANGE_CHUNK_SIZE", _DEFAULT_RANGE_CHUNK_SIZE)
)
RAY_DATA_OBSTORE_MAX_CONCURRENCY = int(
    os.environ.get("RAY_DATA_OBSTORE_MAX_CONCURRENCY", "128")
)

try:
    from obstore import parse_scheme as obstore_parse_scheme

    OBSTORE_AVAILABLE = RAY_DATA_USE_OBSTORE
except ImportError:
    OBSTORE_AVAILABLE = False
    obstore_parse_scheme = None


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
            self._cache[store_url] = self._from_url(
                store_url,
                retry_config=self._retry_config,
                **self._filesystem_kwargs,
            )
        return self._cache[store_url]


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


async def _fetch(uri: str, registry: StoreRegistry) -> bytes:
    """Download a single URI as a whole-file GET and return raw bytes."""
    import obstore as obs

    store_url, path = _split_uri(uri)
    result = await obs.get_async(registry.get(store_url), path)
    return bytes(await result.bytes_async())


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
        # No semaphore, indicate we not using range splitting.
        # Number of concurrent connections controlled by PartitionActor.
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

        ranges = [
            (chunk_start, min(chunk_start + range_chunk_size, size))
            for chunk_start in range(0, size, range_chunk_size)
        ]
        tasks = [
            asyncio.ensure_future(
                _fetch_chunk(uri, store, path, s, e, result, semaphore)
            )
            for s, e in ranges
        ]
        try:
            await asyncio.gather(*tasks)
        except Exception:
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


async def _resolve_size(
    uri: str,
    registry: StoreRegistry,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> int:
    """Return the file size in bytes via a HEAD request, or 0 on failure."""
    import obstore as obs

    try:
        store_url, path = _split_uri(uri)
        store = registry.get(store_url)
        if semaphore is not None:
            async with semaphore:
                meta = await obs.head_async(store, path)
        else:
            meta = await obs.head_async(store, path)
        return meta["size"] if isinstance(meta, dict) else meta.size
    except Exception:
        return 0


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

    registry = StoreRegistry(retry_config={"max_retries": 10}, **fs_kwargs)

    if range_threshold <= 0:
        tasks = [_fetch_whole(uri, registry, uri_column_name, sem) for uri in uris]
        return list(await asyncio.gather(*tasks))

    # --- Range-split path ---
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
