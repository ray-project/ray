import os
import logging
import urllib.parse as parse
from ray.experimental.workflow.storage.base import Storage
from ray.experimental.workflow.storage.base import DataLoadError, DataSaveError
from ray.experimental.workflow.storage.filesystem import FilesystemStorageImpl
from ray.experimental.workflow.storage.s3 import S3StorageImpl

logger = logging.getLogger(__name__)


def create_storage(storage_url: str) -> Storage:
    """A factory function that creates different type of storage according
    to the URL.

    Args:
        storage_url: A URL indicates the storage type and root path.
        Currently only two types of storages are supported: local fs and s3
        For local fs, a path is needed, it can be either a URI with scheme
        file:// or just a local path, i.e.:
           file:///local_path
           local_path

        For s3, bucket, path are necessary. In the meantime, other parameters
        can be passed as well, like credientials or regions, i.e.:
           s3://bucket/path?region_name=str&endpoint_url=str&aws_access_key_id=str&
               aws_secret_access_key=str&aws_session_token=str

        All parameters are optional and have the same meaning as boto3.client

    Returns:
        A storage instance.
    """
    parsed_url = parse.urlparse(storage_url)
    if parsed_url.scheme == "file" or parsed_url.scheme == "":
        return FilesystemStorageImpl(parsed_url.path)
    elif parsed_url.scheme == "s3":
        bucket = parsed_url.netloc
        s3_path = parsed_url.path
        if not s3_path:
            raise ValueError(f"Invalid s3 path: {s3_path}")
        params = dict({
            tuple(param.split("=", 1))
            for param in str(parsed_url.query).split("&")
        })
        return S3StorageImpl(bucket, s3_path, **params)
    else:
        raise ValueError(f"Invalid url: {storage_url}")


# the default storage is a local filesystem storage with a hidden directory
_global_storage = None


def get_global_storage() -> Storage:
    global _global_storage
    if _global_storage is None:
        storage_url = os.environ.get("RAY_WORKFLOW_STORAGE")
        if storage_url is None:
            # We should use get_temp_dir_path, but for ray client, we don't
            # have this one. We need a flag to tell whether it's a client
            # or a driver to use the right dir.
            # For now, just use /tmp/ray/workflow_data
            logger.warning(
                "Using default local dir: `/tmp/ray/workflow_data`. "
                "This should only be used for testing purposes.")
            storage_url = "file:///tmp/ray/workflow_data"
        _global_storage = create_storage(storage_url)
    return _global_storage


def set_global_storage(storage: Storage) -> None:
    global _global_storage
    _global_storage = storage


__all__ = ("Storage", "get_global_storage", "create_storage",
           "set_global_storage", "DataLoadError", "DataSaveError")
