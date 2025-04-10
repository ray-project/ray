from typing import TYPE_CHECKING, Iterable, List

from ray.data.block import DataBatch

from .in_memory_size_estimator import InMemorySizeEstimator
from .native_file_reader import NativeFileReader

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


class BinaryInMemorySizeEstimator(InMemorySizeEstimator):
    def estimate_in_memory_size(
        self, path: str, file_size: int, *, filesystem: "pyarrow.fs.FileSystem"
    ) -> int:
        # NOTE: This method assumes that the file isn't compressed.
        return file_size
