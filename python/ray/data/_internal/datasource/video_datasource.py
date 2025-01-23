import logging
from typing import TYPE_CHECKING, List, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


class VideoDatasource(FileBasedDatasource):
    _FILE_EXTENSIONS = [
        "mp4",
        "mkv",
        "mov",
        "avi",
        "wmv",
        "flv",
        "webm",
        "m4v",
        "3gp",
        "mpeg",
        "mpg",
        "ts",
        "ogv",
        "rm",
        "rmvb",
        "vob",
        "asf",
        "f4v",
        "m2ts",
        "mts",
        "divx",
        "xvid",
        "mxf",
    ]

    def __init__(
        self,
        paths: Union[str, List[str]],
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="decord", package="decord")

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        from decord import VideoReader

        reader = VideoReader(f)

        for frame_index, frame in enumerate(reader):
            item = {"frame": frame.asnumpy(), "frame_index": frame_index}
            builder = DelegatingBlockBuilder()
            builder.add(item)
            yield builder.build()
