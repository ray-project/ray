from typing import TYPE_CHECKING, Iterable

from .native_file_reader import NativeFileReader
from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow


class BinaryReader(NativeFileReader):
    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        yield {"bytes": [file.readall()]}
