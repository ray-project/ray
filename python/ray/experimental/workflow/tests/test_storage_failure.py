from typing import Any
from ray.experimental.workflow.storage.filesystem import FilesystemStorageImpl


class LoggedFileSystemStorage(FilesystemStorageImpl):
    async def put(self, key: str, data: Any, is_json: bool = False) -> None:
        await super().put(key, data, is_json)
