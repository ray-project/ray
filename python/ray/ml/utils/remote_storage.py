import re
from typing import Optional, Tuple

try:
    import fsspec
except ImportError:
    fsspec = None

try:
    import pyarrow
    import pyarrow.fs
except (ImportError, ModuleNotFoundError):
    pyarrow = None

from ray import logger

URI_PROTOCOL_REGEX = r"^(\w+)://(.+)$"
S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
HDFS_PREFIX = "hdfs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX, HDFS_PREFIX)


def _assert_pyarrow_installed():
    if pyarrow is None:
        raise RuntimeError(
            "Uploading, downloading, and deleting from cloud storage "
            "requires pyarrow to be installed. Install with: "
            "`pip install pyarrow`. Subsequent calls to cloud operations "
            "will be ignored."
        )


def _split_protocol_path(uri: str) -> Tuple[Optional[str], Optional[str]]:
    protocol_match = re.match(URI_PROTOCOL_REGEX, uri)
    if not protocol_match:
        return None, None

    return protocol_match.group(1), protocol_match.group(2)


def fs_hint(uri: str) -> str:
    """Return a hint how to install required filesystem package"""
    if pyarrow is None:
        return "Please make sure PyArrow is installed: `pip install pyarrow`."
    if fsspec is None:
        return "Try installing fsspec: `pip install fsspec`."

    from fsspec.registry import known_implementations

    protocol, _path = _split_protocol_path(uri)

    if protocol in known_implementations:
        return known_implementations[protocol]["err"]

    return "Make sure to install and register your fsspec-compatible filesystem."


def is_cloud_target(uri: str) -> bool:
    """Check if target URI is a cloud target"""
    if uri.startswith("file://") or not re.match(URI_PROTOCOL_REGEX, uri):
        return False

    if bool(get_fs_and_path(uri)[0]):
        return True
    # Keep manual check for prefixes for backwards compatibility with the
    # TrialCheckpoint class. Remove once fully deprecated.
    if any(uri.startswith(p) for p in ALLOWED_REMOTE_PREFIXES):
        return True
    return False


# Cache fs objects
_cached_fs = {}


def get_fs_and_path(
    uri: str,
) -> Tuple[Optional["pyarrow.fs.FileSystem"], Optional[str]]:
    if not pyarrow:
        return None, None

    protocol, path = _split_protocol_path(uri)
    if protocol in _cached_fs:
        fs = _cached_fs[protocol]
        return fs, path

    try:
        fs, path = pyarrow.fs.FileSystem.from_uri(uri)
        _cached_fs[protocol] = fs

        return fs, path
    except pyarrow.lib.ArrowInvalid:
        # Raised when URI not recognized
        if not fsspec:
            # Only return if fsspec is not installed
            return None, None

    # Else, try to resolve protocol via fsspec
    try:
        fsspec_fs = fsspec.filesystem(protocol)
    except ValueError:
        # Raised when protocol not known
        return None, None

    fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fsspec_fs))
    _cached_fs[protocol] = fs

    return fs, path


def clear_bucket(bucket: str):
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(bucket)
    if not fs:
        raise ValueError(
            f"Could not clear bucket contents: "
            f"Bucket `{bucket}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(bucket)}"
        )

    try:
        fs.delete_dir(bucket_path)
    except Exception as e:
        logger.warning(f"Caught exception when clearing bucket `{bucket}`: {e}")


def download_from_bucket(bucket: str, local_path: str):
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(bucket)
    if not fs:
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(bucket)}"
        )

    pyarrow.fs.copy_files(bucket_path, local_path, source_filesystem=fs)


def upload_to_bucket(bucket: str, local_path: str):
    _assert_pyarrow_installed()

    fs, bucket_path = get_fs_and_path(bucket)
    if not fs:
        raise ValueError(
            f"Could not upload to bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target. "
            f"Hint: {fs_hint(bucket)}"
        )

    pyarrow.fs.copy_files(local_path, bucket_path, destination_filesystem=fs)
