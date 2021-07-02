import os

from ray.experimental.workflow.storage.base import Storage
from ray.experimental.workflow.storage.base import DataLoadError, DataSaveError
from ray.experimental.workflow.storage.filesystem import FilesystemStorageImpl


def create_storage(storage_url: str) -> Storage:
    """A factory function that creates different type of storage according
    to the URL.

    Args:
        storage_url: A URL indicates the storage type and root path.

    Returns:
        A storage instance.
    """
    # TODO(suquark): in the future we need to support general URLs for
    # different storages, e.g. "s3://xxxx/xxx". Currently we just use
    # 'pathlib.Path' for convenience.
    return FilesystemStorageImpl(storage_url)


# the default storage is a local filesystem storage with a hidden directory
_global_storage = create_storage(os.path.join(os.path.curdir, ".rayflow"))


def get_global_storage() -> Storage:
    return _global_storage


def set_global_storage(storage: Storage) -> None:
    global _global_storage
    _global_storage = storage


__all__ = ("Storage", "create_storage", "get_global_storage",
           "set_global_storage", "DataLoadError", "DataSaveError")
