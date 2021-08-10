import json
from typing import Any, List
from urllib import parse
import pathlib
from filelock import FileLock
from ray.experimental.workflow.storage.base import Storage
from ray.experimental.workflow.storage.filesystem import FilesystemStorageImpl
import ray.cloudpickle
from ray.experimental.workflow import serialization_context


class LoggedStorage(FilesystemStorageImpl):
    """A storage records all writing to storage sequentially."""

    def __init__(self, workflow_root_dir: str):
        super().__init__(workflow_root_dir)
        self._log_dir = self._workflow_root_dir
        self._count = self._log_dir / "count.log"
        if not self._log_dir.exists():
            self._log_dir.mkdir()
        # only one process initializes the count
        with FileLock(str(self._workflow_root_dir / ".lock")):
            if not self._count.exists():
                with open(self._count, "x") as f:
                    f.write("0")

    async def put(self, key: str, data: Any, is_json: bool = False) -> None:
        with FileLock(str(self._log_dir / ".lock")):
            with open(self._count, "r") as f:
                count = int(f.read())
            k1 = self._log_dir / f"{count}.metadata.json"
            k2 = self._log_dir / f"{count}.value"
            await super().put(
                str(k1), {
                    "operation": "put",
                    "key": key,
                    "is_json": is_json
                },
                is_json=True)
            await super().put(str(k2), data, is_json=is_json)
            with open(self._count, "w") as f:
                f.write(str(count + 1))

    async def delete_prefix(self, key: str) -> None:
        with FileLock(str(self._log_dir / ".lock")):
            with open(self._count, "r") as f:
                count = int(f.read())
            k1 = self._log_dir / f"{count}.metadata.json"
            await super().put(
                str(k1), {
                    "operation": "delete_prefix",
                    "key": key
                },
                is_json=True)
            with open(self._count, "w") as f:
                f.write(str(count + 1))

    def get_metadata(self, index: int) -> Any:
        with open(self._log_dir / f"{index}.metadata.json") as f:
            return json.load(f)

    def get_value(self, index: int, is_json: bool) -> Any:
        path = self._log_dir / f"{index}.value"
        if is_json:
            with open(path) as f:
                return json.load(f)
        else:
            with open(path, "rb") as f:
                with serialization_context.workflow_args_keeping_context():
                    return ray.cloudpickle.load(f)

    def __len__(self):
        with open(self._count, "r") as f:
            return int(f.read())


class DebugStorage(Storage):
    """A storage for debugging purpose."""

    def __init__(self, wrapped_storage: "Storage", path: str):
        self._path = path
        self._wrapped_storage = wrapped_storage
        log_path = pathlib.Path(path)
        parsed = parse.urlparse(wrapped_storage.storage_url)
        log_path = (log_path / parsed.scheme.strip("/") /
                    parsed.netloc.strip("/") / parsed.path.strip("/"))
        if not log_path.exists():
            log_path.mkdir(parents=True)
        self._logged_storage = LoggedStorage(str(log_path))

    def make_key(self, *names: str) -> str:
        return self._wrapped_storage.make_key(*names)

    async def get(self, key: str, is_json: bool = False) -> Any:
        return await self._wrapped_storage.get(key, is_json)

    async def put(self, key: str, data: Any, is_json: bool = False) -> None:
        await self._logged_storage.put(key, data, is_json)
        await self._wrapped_storage.put(key, data, is_json)

    async def delete_prefix(self, prefix: str) -> None:
        await self._logged_storage.delete_prefix(prefix)
        await self._wrapped_storage.delete_prefix(prefix)

    async def scan_prefix(self, key_prefix: str) -> List[str]:
        return await self._wrapped_storage.scan_prefix(key_prefix)

    @property
    def storage_url(self) -> str:
        store_url = parse.quote_plus(self._wrapped_storage.storage_url)
        parsed_url = parse.ParseResult(
            scheme="debug",
            path=str(pathlib.Path(self._path).absolute()),
            netloc="",
            params="",
            query=f"storage={store_url}",
            fragment="")
        return parse.urlunparse(parsed_url)

    def __reduce__(self):
        return DebugStorage, (self._wrapped_storage, self._path)

    @property
    def wrapped_storage(self) -> "Storage":
        """Get wrapped storage."""
        return self._wrapped_storage

    async def replay(self, index: int) -> None:
        """Replay the a record to the storage.

        Args:
            index: The index of the recorded log to replay.
        """
        log = self.get_log(index)
        op = log["operation"]
        if op == "put":
            is_json = log["is_json"]
            data = self.get_value(index, is_json)
            await self._wrapped_storage.put(log["key"], data, is_json)
        elif op == "delete_prefix":
            await self._wrapped_storage.delete_prefix(log["key"])
        else:
            raise ValueError(f"Unknown operation '{op}'.")

    def get_log(self, index: int) -> Any:
        return self._logged_storage.get_metadata(index)

    def get_value(self, index: int, is_json: bool) -> Any:
        return self._logged_storage.get_value(index, is_json)

    def __len__(self):
        return len(self._logged_storage)
