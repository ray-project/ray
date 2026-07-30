"""Shared helpers for reading Parquet footers through the arrow-rs native crate.

Both the read path (``ArrowRsParquetFileReader``) and the listing-stage chunker
(``ParquetFileChunker``) need to read a file's footer via the native crate so a
supported file's footer is read by arrow-rs *end to end* — no PyArrow footer read
for Local/S3 files when ``DataContext.use_arrow_rs_parquet_reader`` is on. This
module holds the logic they share (filesystem eligibility, S3 config bridging,
and the actual ``read_metadata`` call) so neither layer imports the other.
"""

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem, S3FileSystem


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
