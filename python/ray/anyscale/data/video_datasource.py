# Copyright (2023 and onwards) Anyscale, Inc.
import logging
from typing import TYPE_CHECKING, List, Optional, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import Deprecated

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


@Deprecated
class VideoDatasource(FileBasedDatasource):
    def __init__(
        self,
        paths: Union[str, List[str]],
        include_paths: bool = False,
        **file_based_datasource_kwargs
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="decord", package="decord")

        logger.warning("`VideoDatasource` is deprecated. Call `read_audio` instead.")

        self.include_paths = include_paths

    def _read_stream(self, f: "pyarrow.NativeFile", path: str):
        from decord import VideoReader

        reader = VideoReader(f)

        for frame_index, frame in enumerate(reader):
            item = {"frame": frame.asnumpy(), "frame_index": frame_index}
            if self.include_paths:
                item["path"] = path

            builder = DelegatingBlockBuilder()
            builder.add(item)
            yield builder.build()

    def estimate_inmemory_data_size(self) -> Optional[int]:
        # TODO: The compression ratio varies widely depending on the video, so we can't
        # provide a good estimate without sampling.
        return None
