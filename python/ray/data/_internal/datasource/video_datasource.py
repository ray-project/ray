import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

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
        include_timestamps=False,
        decord_load_args: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="decord", package="decord")

        self.include_timestamps = include_timestamps
        if decord_load_args is None:
            self.decord_load_args = {}
        else:
            self.decord_load_args = decord_load_args

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        from decord import VideoReader

        reader = VideoReader(f, **self.decord_load_args)

        for frame_index, frame in enumerate(reader):
            item = {"frame": frame.asnumpy(), "frame_index": frame_index}
            if self.include_timestamps is True:
                item["frame_timestamp"] = reader.get_frame_timestamp(frame_index)

            builder = DelegatingBlockBuilder()
            builder.add(item)
            yield builder.build()
