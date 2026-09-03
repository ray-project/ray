"""Shared helpers for reading Parquet footers through the arrow-rs native crate.

Used by the read path (``ArrowRsParquetFileReader``), which reads each file's
footer via the crate so a supported file is opened by arrow-rs *end to end* — no
PyArrow footer read for Local/S3 files when
``DataContext.use_arrow_rs_parquet_reader`` is on. The logic is factored out here
(filesystem eligibility, S3 config bridging, and the actual ``read_metadata``
call) rather than inlined in the reader because it is also what a listing-stage
caller would need.

**There is currently only one caller.** This module's docstring previously named
``ParquetFileChunker`` as a second one; that was never true on any branch. The
listing stage does now read footers — the footer-chunking path reads every file's
footer on a pool of ``FooterReader`` actors to prune and bin-pack row groups — but
it does so through PyArrow, not through this module. Wiring it to the crate is a
separate, unstarted piece of work; until then, a native read still pays one footer
read here on top of the one ``ListFiles`` already did.
"""

import os
import threading
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem, S3FileSystem


# --------------------------------------------------------------------------- #
# Process-level native S3 client cache (findings M96/M97)
# --------------------------------------------------------------------------- #
# Client construction is the expensive part of a native S3 read's setup: a
# fresh `object_store` client is a fresh connection pool, so the first requests
# pay DNS + full TLS handshakes with no session reuse. When the planner emits
# single-row-group tasks (64 MiB bins, the release `read_large_parquet`
# regime), a per-TASK client meant ~5.8k cold client builds per job for 104
# distinct files — measured as read workers spending ~2/3 of their wall time
# blocked in `open_file`'s metadata fetch while the box sat idle (M97).
# Caching per process gives the client the same lifetime pyarrow's serialized
# `S3FileSystem` client already has in a reused Ray worker. Staleness is
# handled by the KEY, not a TTL: the key includes the full connection config,
# credentials included, so rotated credentials miss the cache and build a
# fresh client while the stale entry ages out of the size-capped table.
_S3_STORE_CACHE: Dict[Tuple, Any] = {}
_S3_STORE_CACHE_LOCK = threading.Lock()
# Real jobs hold one or two entries (one per bucket x config); the cap only
# bounds a long-lived worker against credential-rotation churn.
_S3_STORE_CACHE_MAX_ENTRIES = 8


def native_metadata_supported_filesystem(
    filesystem: Optional["FileSystem"],
) -> bool:
    """Whether the crate's ``read_metadata`` can read footers on this filesystem.

    The crate's ``object_store`` backend is compiled with the local and AWS
    features only, so it can footer-read local files and S3 objects. ``None``
    means the caller will use the default local filesystem. Any other
    filesystem (GCS, Azure, HDFS, an fsspec wrapper) must stay on PyArrow.
    """
    from pyarrow.fs import LocalFileSystem, S3FileSystem

    return filesystem is None or isinstance(filesystem, (LocalFileSystem, S3FileSystem))


def s3_config(fs: "S3FileSystem") -> dict:
    """Recover the full S3 connection config from a pyarrow ``S3FileSystem`` so
    the native crate connects *identically* — same endpoint, credentials, region,
    addressing style — instead of rebuilding a default client from the ambient env
    (which would silently ignore an explicit endpoint override or static creds and
    break MinIO / moto / custom-endpoint / credentialed buckets).

    pyarrow round-trips the whole config through ``__reduce__`` (verified to include
    ``secret_key``/``session_token``/``endpoint_override``/``scheme`` across the
    pyarrow versions Ray supports), so that is the source of truth. Empty strings
    (pyarrow's "unset" sentinel) are normalized to ``None``.
    """
    try:
        opts = fs.__reduce__()[1][0]
    except Exception:
        opts = {}

    def _val(key):
        v = opts.get(key)
        return v if v else None

    endpoint = _val("endpoint_override")
    # object_store refuses plain-HTTP endpoints unless explicitly allowed
    # (moto / MinIO are http). pyarrow's `scheme` defaults to "https" even when
    # the endpoint override is an http:// URL, so trust the endpoint URL first.
    allow_http = (str(endpoint).startswith("http://")) or opts.get("scheme") == "http"

    return {
        "region": _val("region") or "us-east-1",
        "anonymous": bool(opts.get("anonymous", False)),
        "endpoint": endpoint,
        "access_key_id": _val("access_key"),
        "secret_access_key": _val("secret_key"),
        "session_token": _val("session_token"),
        "allow_http": allow_http,
        "virtual_hosted_style": bool(opts.get("force_virtual_addressing", False)),
    }


def split_s3_path(path: str) -> tuple:
    """Split a pyarrow-style S3 path into ``(bucket, key)``.

    pyarrow filesystem paths are normally scheme-less (``bucket/key``), but a
    leading ``s3://`` is stripped defensively so it can never be split into a
    bogus ``s3:`` bucket.
    """
    if path.startswith("s3://"):
        path = path[len("s3://") :]
    bucket, _, key = path.partition("/")
    return bucket, key


def connect_native_s3(bucket: str, filesystem: "S3FileSystem"):
    """Build or reuse the crate's per-bucket S3 client (``NativeS3Store``),
    configured identically to the pyarrow ``S3FileSystem`` (see
    :func:`s3_config`).

    Fresh-client construction is the expensive per-call setup the original
    ``read_metadata_s3`` / ``read_row_groups_s3`` entry points paid on *every*
    call (new HTTP client, no TLS session reuse — findings T10). Clients are
    cached per process, keyed by (bucket, full connection config including
    credentials) — see the cache comment above for why per-task scoping was
    not enough (M97) and why the key handles credential rotation. The store is
    an immutable ``Arc<dyn ObjectStore>`` handle, safe to share across tasks.
    ``RAY_DATA_ARROW_RS_S3_CLIENT_CACHE=0`` restores the uncached behavior.
    """
    import ray_data_arrow_rs

    cfg = s3_config(filesystem)

    def _build():
        return ray_data_arrow_rs.connect_s3(
            bucket,
            cfg["region"],
            cfg["anonymous"],
            endpoint=cfg["endpoint"],
            access_key_id=cfg["access_key_id"],
            secret_access_key=cfg["secret_access_key"],
            session_token=cfg["session_token"],
            allow_http=cfg["allow_http"],
            virtual_hosted_style=cfg["virtual_hosted_style"],
        )

    if os.environ.get("RAY_DATA_ARROW_RS_S3_CLIENT_CACHE", "1") == "0":
        return _build()

    key = (bucket, tuple(sorted(cfg.items(), key=lambda kv: kv[0])))
    with _S3_STORE_CACHE_LOCK:
        store = _S3_STORE_CACHE.get(key)
    if store is not None:
        return store
    store = _build()  # outside the lock: connect does network setup
    with _S3_STORE_CACHE_LOCK:
        existing = _S3_STORE_CACHE.get(key)
        if existing is not None:
            return existing  # benign build race: reuse the winner
        while len(_S3_STORE_CACHE) >= _S3_STORE_CACHE_MAX_ENTRIES:
            _S3_STORE_CACHE.pop(next(iter(_S3_STORE_CACHE)))
        _S3_STORE_CACHE[key] = store
    return store


def read_native_metadata(path: str, filesystem: Optional["FileSystem"]):
    """Read one Parquet file's footer via the native crate.

    Returns the crate's ``ParquetFileMetadata`` pyclass (exposing ``num_rows``,
    ``num_row_groups``, ``row_group_num_rows``, ``row_group_byte_sizes``,
    ``row_group_compressed_sizes``, and ``__arrow_c_schema__``). Raises on a
    missing extension or any footer-read failure — the caller decides whether to
    fall back. ``filesystem`` must already be native-eligible (see
    :func:`native_metadata_supported_filesystem`).
    """
    import ray_data_arrow_rs
    from pyarrow.fs import S3FileSystem

    if isinstance(filesystem, S3FileSystem):
        # pyarrow filesystem paths are normally scheme-less ("bucket/key"), but
        # strip a leading "s3://" defensively so we never split it into a bogus
        # "s3:" bucket.
        if path.startswith("s3://"):
            path = path[len("s3://") :]
        bucket, _, key = path.partition("/")
        cfg = s3_config(filesystem)
        return ray_data_arrow_rs.read_metadata_s3(
            bucket,
            key,
            cfg["region"],
            cfg["anonymous"],
            endpoint=cfg["endpoint"],
            access_key_id=cfg["access_key_id"],
            secret_access_key=cfg["secret_access_key"],
            session_token=cfg["session_token"],
            allow_http=cfg["allow_http"],
            virtual_hosted_style=cfg["virtual_hosted_style"],
        )
    return ray_data_arrow_rs.read_metadata(path)
