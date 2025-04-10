from typing import TYPE_CHECKING, Iterable, List, Optional

from ray.data.block import DataBatch

from .in_memory_size_estimator import InMemorySizeEstimator
from .native_file_reader import NativeFileReader
from .supports_row_counting import SupportsRowCounting

if TYPE_CHECKING:
    import pyarrow


class BinaryReader(NativeFileReader, SupportsRowCounting):
    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        yield {"bytes": [file.readall()]}

    def estimate_in_memory_size(self, path: str, file_size: int, *, filesystem) -> int:
        return file_size

    def count_rows(self, paths: List[str], *, filesystem) -> int:
        return len(paths)

    def can_count_rows(self) -> bool:
        return True

    def count_rows_batch_size(self) -> Optional[int]:
        # Since we just return the number of paths, we don't need to batch.
        return None


class BinaryInMemorySizeEstimator(InMemorySizeEstimator):
    def estimate_in_memory_size(
        self, path: str, file_size: int, *, filesystem: "pyarrow.fs.FileSystem"
    ) -> int:
        # NOTE: This method assumes that the file isn't compressed.
        return file_size
