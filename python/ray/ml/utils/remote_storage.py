import re
from typing import Optional, Tuple

from fsspec import filesystem
from pyarrow.lib import ArrowInvalid
from pyarrow.fs import copy_files, FileSystem, FSSpecHandler, PyFileSystem

from ray import logger

URI_PROTOCOL_REGEX = r"^(\w+)://(.+)$"
S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
HDFS_PREFIX = "hdfs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX, HDFS_PREFIX)


def is_cloud_target(target: str) -> bool:
    return bool(get_fs_and_path(target)[0])


def get_fs_and_path(target: str) -> Tuple[Optional[FileSystem], Optional[str]]:
    try:
        fs, path = FileSystem.from_uri(target)
        return fs, path
    except ArrowInvalid:
        # Raised when URI not recognized
        pass

    protocol_match = re.match(URI_PROTOCOL_REGEX, target)
    if not protocol_match:
        return None, None

    protocol = protocol_match.group(1)
    path = protocol_match.group(2)
    try:
        fsspec_fs = filesystem(protocol)
    except ValueError:
        # Raised when protocol not known
        return None, None

    if not path.startswith("/"):
        path = f"/{path}"

    return PyFileSystem(FSSpecHandler(fsspec_fs)), path


def clear_bucket(bucket: str):
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
    fs, bucket_path = get_fs_and_path(bucket)
    if not fs:
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        )

    copy_files(bucket_path, local_path, source_filesystem=fs)


def upload_to_bucket(bucket: str, local_path: str):
    fs, bucket_path = get_fs_and_path(bucket)
    if not fs:
        raise ValueError(
            f"Could not upload to bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        )

    copy_files(local_path, bucket_path, destination_filesystem=fs)
