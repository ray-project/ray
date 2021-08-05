from typing import Any
from ray.experimental.workflow.storage.filesystem import FilesystemStorageImpl


class LoggedFileSystemStorage(FilesystemStorageImpl):
    def __init__(self, workflow_root_dir: str):
        super().__init__(workflow_root_dir)
        self._log_dir = self._workflow_root_dir / "log"
        if not self._log_dir.exists():
            self._log_dir.mkdir()

    async def put(self, key: str, data: Any, is_json: bool = False) -> None:
        await super().put(key, data, is_json)
