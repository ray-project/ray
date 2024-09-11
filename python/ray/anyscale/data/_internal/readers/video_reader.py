from typing import Iterable

import pyarrow

from .file_reader import FileReader
from ray.data._internal.util import _check_import
from ray.data.block import DataBatch


class VideoReader(FileReader):
    def __init__(
        self,
        **file_reader_kwargs,
    ):
        super().__init__(**file_reader_kwargs)

        _check_import(self, module="decord", package="decord")

    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        from decord import VideoReader

        reader = VideoReader(file)

        for frame_index, frame in enumerate(reader):
            yield {"frame": [frame.asnumpy()], "frame_index": [frame_index]}
