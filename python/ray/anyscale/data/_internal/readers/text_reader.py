from typing import TYPE_CHECKING, Iterable

from .native_file_reader import NativeFileReader
from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow


class TextReader(NativeFileReader):
    def __init__(
        self,
        *,
        drop_empty_lines: bool = False,
        encoding: str = "utf-8",
        **file_reader_kwargs
    ):
        super().__init__(**file_reader_kwargs)

        self._drop_empty_lines = drop_empty_lines
        self._encoding = encoding

    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        data = file.readall()
        lines = data.decode(self._encoding).split("\n")
        if self._drop_empty_lines:
            lines = [line for line in lines if line.strip() != ""]
        yield {"text": lines}

    def estimate_in_memory_size(self, path: str, file_size: int, *, filesystem) -> int:
        return file_size
