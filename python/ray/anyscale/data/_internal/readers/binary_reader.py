from typing import TYPE_CHECKING, Iterable

from .file_reader import FileReader
from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow


class BinaryReader(FileReader):
    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        yield {"bytes": [file.readall()]}
