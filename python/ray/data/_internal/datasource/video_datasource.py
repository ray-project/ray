import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

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
        fps: Optional[int] = None,
        resize: Optional[Tuple[int, int]] = None,
        decord_load_args: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="decord", package="decord")

        if fps is not None and fps <= 0:
            raise ValueError(
                f"Expected `fps` to be a positive integer, but got {fps} instead."
            )

        if resize is not None:
            if len(resize) != 2:
                raise ValueError(
                    "Expected `resize` to contain two integers for height and "
                    f"width, but got {len(resize)} integers instead."
                )
            if resize[0] <= 0 or resize[1] <= 0:
                raise ValueError(
                    "Expected `resize` to contain positive integers, but got "
                    f"{resize} instead."
                )

        self.include_timestamps = include_timestamps
        self.fps = fps
        self.resize = resize
        if decord_load_args is None:
            self.decord_load_args = {}
        else:
            self.decord_load_args = decord_load_args

        if resize is not None and (
            "width" in self.decord_load_args or "height" in self.decord_load_args
        ):
            raise ValueError(
                "Can't specify `resize` together with `width` or `height` in "
                "`decord_load_args`."
            )

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        from decord import VideoReader

        decord_load_args = self.decord_load_args
        if self.resize is not None:
            height, width = self.resize
            decord_load_args = {**decord_load_args, "height": height, "width": width}

        reader = VideoReader(f, **decord_load_args)

        stride = 1
        if self.fps is not None:
            source_fps = reader.get_avg_fps()
            if source_fps:
                stride = max(1, round(source_fps / self.fps))

        for frame_index, frame in enumerate(reader):
            if frame_index % stride != 0:
                continue

            item = {"frame": frame.asnumpy(), "frame_index": frame_index}
            if self.include_timestamps is True:
                item["frame_timestamp"] = reader.get_frame_timestamp(frame_index)

            builder = DelegatingBlockBuilder()
            builder.add(item)
            yield builder.build()
