import logging
import os
import urllib.parse as parse
from ray.workflow.storage.base import Storage
from ray.workflow.storage.base import DataLoadError, DataSaveError, KeyNotFoundError

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
        from ray.workflow.storage.filesystem import FilesystemStorageImpl

        return FilesystemStorageImpl(parsed_url.path)
    elif parsed_url.scheme == "s3":
        from ray.workflow.storage.s3 import S3StorageImpl

        bucket = parsed_url.netloc
        s3_path = parsed_url.path.lstrip("/")
        if not s3_path:
            raise ValueError(f"Invalid s3 path: {s3_path}")
        params = dict(parse.parse_qsl(parsed_url.query))
        return S3StorageImpl(bucket, s3_path, **params)
    elif parsed_url.scheme == "debug":
        from ray.workflow.storage.debug import DebugStorage

        params = dict(parse.parse_qsl(parsed_url.query))
        return DebugStorage(create_storage(params["storage"]), path=parsed_url.path)
    else:
        extra_msg = ""
        if os.name == "nt":
            extra_msg = (
                " Try using file://{} or file:///{} for Windows file paths.".format(
                    storage_url, storage_url
                )
            )
        raise ValueError(f"Invalid url: {storage_url}." + extra_msg)


# the default storage is a local filesystem storage with a hidden directory
_global_storage = None


def get_global_storage() -> Storage:
    global _global_storage
    if _global_storage is None:
        raise RuntimeError(
            "`workflow.init()` must be called prior to using the workflows API."
        )
    return _global_storage


def set_global_storage(storage: Storage) -> None:
    global _global_storage
    _global_storage = storage


__all__ = (
    "Storage",
    "get_global_storage",
    "create_storage",
    "set_global_storage",
    "DataLoadError",
    "DataSaveError",
    "KeyNotFoundError",
)
