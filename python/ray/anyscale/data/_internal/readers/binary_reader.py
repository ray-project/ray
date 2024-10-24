from typing import TYPE_CHECKING, Iterable, List

from .native_file_reader import NativeFileReader
from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow


class BinaryReader(NativeFileReader):
    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        yield {"bytes": [file.readall()]}

    def estimate_in_memory_size(self, path: str, file_size: int, *, filesystem) -> int:
        return file_size

    def count_rows(self, paths: List[str], *, filesystem) -> int:
        return len(paths)

    def supports_count_rows(self) -> bool:
        return True
