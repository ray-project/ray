import abc
import re
import subprocess
from typing import Dict

from ray import logger
from ray.util.annotations import DeveloperAPI

VALID_URI_REGEX = r"^\w+://$"
S3_PREFIX = "s3://"
GS_PREFIX = "gs://"
HDFS_PREFIX = "hdfs://"
ALLOWED_REMOTE_PREFIXES = (S3_PREFIX, GS_PREFIX, HDFS_PREFIX)


class RemoteStorage(abc.ABC):
    """Base class for remote storage providers.

    Classes inheriting from this provide implementations for
    methods to upload, download, and delete files and
    directories to and from remote storage.
    """

    def upload(self, local_source: str, remote_target: str, **kwargs) -> None:
        """Upload local path to remote target.

        Args:
            local_source: Path to local source file or directory.
            remote_target: URI to remote target file or directory.
            kwargs: Optional provider-specific arguments.
        """
        raise NotImplementedError

    def download(self, remote_source: str, local_target: str, **kwargs) -> None:
        """Download remote source to local target.

        Args:
            remote_source: URI to remote source file or directory.
            local_target: Path to local target file or directory.
            kwargs: Optional provider-specific arguments.
        """
        raise NotImplementedError

    def delete(self, remote_target: str, **kwargs) -> None:
        """Delete remote target file or directory..

        Args:
            remote_target: URI to remote target file or directory.
            kwargs: Optional provider-specific arguments.
        """
        raise NotImplementedError


class S3Storage(RemoteStorage):
    def upload(self, local_source: str, remote_target: str, **kwargs) -> None:
        subprocess.check_call(
            ["aws", "s3", "cp", "--recursive", "--quiet", local_source, remote_target]
        )

    def download(self, remote_source: str, local_target: str, **kwargs) -> None:
        subprocess.check_call(
            ["aws", "s3", "cp", "--recursive", "--quiet", remote_source, local_target]
        )

    def delete(self, remote_target: str, **kwargs) -> None:
        subprocess.check_call(
            ["aws", "s3", "rm", "--recursive", "--quiet", remote_target]
        )


class GSStorage(RemoteStorage):
    def upload(self, local_source: str, remote_target: str, **kwargs) -> None:
        subprocess.check_call(["gsutil", "-m", "cp", "-r", local_source, remote_target])

    def download(self, remote_source: str, local_target: str, **kwargs) -> None:
        subprocess.check_call(["gsutil", "-m", "cp", "-r", remote_source, local_target])

    def delete(self, remote_target: str, **kwargs) -> None:
        subprocess.check_call(["gsutil", "-m", "rm", "-f", "-r", remote_target])


class HDFSStorage(RemoteStorage):
    def upload(self, local_source: str, remote_target: str, **kwargs) -> None:
        subprocess.check_call(["hdfs", "dfs", "-put", local_source, remote_target])

    def download(self, remote_source: str, local_target: str, **kwargs) -> None:
        subprocess.check_call(["hdfs", "dfs", "-get", remote_source, local_target])

    def delete(self, remote_target: str, **kwargs) -> None:
        subprocess.check_call(["hdfs", "dfs", "-rm", "-r", remote_target])


_registered_storages: Dict[str, RemoteStorage] = {}


@DeveloperAPI
def get_remote_storage(uri: str) -> RemoteStorage:
    """Get remote storage provider for a bucket URI.

    Example:

        storage = get_remote_storage("s3://test/bucket")
        assert isinstance(storage, S3Storage)


    Args:
        uri: Bucket URI, e.g. ``s3://bucket/path``

    Returns: ``Storage`` class.

    Raises: ValueError if no remote storage class is found.
    """
    global _registered_storages
    for prefix, storage in _registered_storages.items():
        if uri.startswith(prefix):
            return storage
    raise ValueError(f"No remote storage provider found for URI: {uri}")


@DeveloperAPI
def register_remote_storage(
    prefix: str, storage: RemoteStorage, override: bool = True
) -> None:
    """Register storage provider.

    If a prefix is already registered, it will be overwritten without warning,
    except when ``override=False``, in which case it is ignored without
    warning.

    Args:
        prefix: String prefix to identify storage URI, e.g. ``prefix://``.
        storage: Storage instance.
        override: If ``True`` (default), will silently override existing
            storage providers.
    """
    global _registered_storages
    if not re.match(VALID_URI_REGEX, prefix):
        raise ValueError(
            f"Invalid prefix: `{prefix}`. Your prefix should be of the form "
            f"`prefix://`"
        )
    if not override and prefix in _registered_storages:
        return
    _registered_storages[prefix] = storage


# Register default storages. Do not override if there are e.g.
# user-provided overrides for these storages.
register_remote_storage(S3_PREFIX, S3Storage(), override=False)
register_remote_storage(GS_PREFIX, GSStorage(), override=False)
register_remote_storage(HDFS_PREFIX, HDFSStorage(), override=False)


def is_cloud_target(target: str):
    global _registered_storages
    return any(target.startswith(prefix) for prefix in _registered_storages.keys())


def clear_bucket(bucket: str):
    try:
        storage = get_remote_storage(bucket)
    except Exception as e:
        raise ValueError(
            f"Could not clear bucket contents: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        ) from e

    try:
        storage.delete(bucket)
    except Exception as e:
        logger.warning(f"Caught exception when clearing bucket `{bucket}`: {e}")


def download_from_bucket(bucket: str, local_path: str):
    try:
        storage = get_remote_storage(bucket)
    except Exception as e:
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        ) from e

    storage.download(bucket, local_path)


def upload_to_bucket(bucket: str, local_path: str):
    try:
        storage = get_remote_storage(bucket)
    except Exception as e:
        raise ValueError(
            f"Could not download from bucket: "
            f"Bucket `{bucket}` is not a valid or supported cloud target."
        ) from e

    storage.upload(local_path, bucket)
