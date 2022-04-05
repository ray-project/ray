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


def _warn_if_pyarrow_not_installed() -> bool:
    if pyarrow is None:
        logger.warning(
            "Uploading, downloading, and deleting from cloud storage "
            "requires pyarrow to be installed. Install with: "
            "`pip install pyarrow`"
        )
    return False


def is_cloud_target(target: str) -> bool:
    if bool(get_fs_and_path(target)[0]):
        return True
    # Keep manual check for prefixes for backwards compatibility with the
    # TrialCheckpoint class. Remove once fully deprecated.
    if any(target.startswith(p) for p in ALLOWED_REMOTE_PREFIXES):
        return True
    return False


def get_fs_and_path(
    target: str,
) -> Tuple[Optional["pyarrow.fs.FileSystem"], Optional[str]]:
    if not pyarrow:
        return None, None

    try:
        fs, path = pyarrow.fs.FileSystem.from_uri(target)
        return fs, path
    except pyarrow.lib.ArrowInvalid:
        # Raised when URI not recognized
        if not fsspec:
            # Only return if fsspec is not installed
            return None, None

    # Else, try to resolve protocol via fsspec
    protocol_match = re.match(URI_PROTOCOL_REGEX, target)
    if not protocol_match:
        return None, None

    protocol = protocol_match.group(1)
    path = protocol_match.group(2)
    try:
        fsspec_fs = fsspec.filesystem(protocol)
    except ValueError:
        # Raised when protocol not known
        return None, None

    if not path.startswith("/"):
        path = f"/{path}"

    return pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fsspec_fs)), path


def clear_bucket(bucket: str):
    if _warn_if_pyarrow_not_installed():
        return

    fs, bucket_path = get_fs_and_path(bucket)
    if not fs:
        raise ValueError(
            f"Could not clear bucket contents: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        )

    try:
        fs.delete_dir(bucket_path)
    except Exception as e:
        logger.warning(f"Caught exception when clearing bucket `{bucket}`: {e}")


def download_from_bucket(bucket: str, local_path: str):
    if _warn_if_pyarrow_not_installed():
        return

    fs, bucket_path = get_fs_and_path(bucket)
    if not fs:
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        )

    pyarrow.fs.copy_files(bucket_path, local_path, source_filesystem=fs)


def upload_to_bucket(bucket: str, local_path: str):
    if _warn_if_pyarrow_not_installed():
        return

    fs, bucket_path = get_fs_and_path(bucket)
    if not fs:
        raise ValueError(
            f"Could not upload to bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        )

    pyarrow.fs.copy_files(local_path, bucket_path, destination_filesystem=fs)
