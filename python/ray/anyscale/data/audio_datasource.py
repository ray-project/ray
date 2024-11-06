# Copyright (2023 and onwards) Anyscale, Inc.
import io
import logging
from typing import TYPE_CHECKING, Iterator, List, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import Deprecated

if TYPE_CHECKING:
    import pyarrow

logger = logging.getLogger(__name__)


@Deprecated
class AudioDatasource(FileBasedDatasource):
    DEFAULT_SAMPLE_RATE = 44100

    def __init__(
        self,
        paths: Union[str, List[str]],
        include_paths: bool = False,
        **file_based_datasource_kwargs
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        logger.warning("`AudioDatasource` is deprecated. Call `read_audio` instead.")

        self.include_paths = include_paths

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import soundfile

        # `soundfile` doesn't support reading from a `pyarrow.NativeFile` directly, so
        # we need to read the file into memory first.
        stream = io.BytesIO(f.read())
        amplitude, sample_rate = soundfile.read(stream, always_2d=True, dtype="float32")

        # (amplitude, channels) -> (channels, amplitude)
        amplitude = amplitude.transpose((1, 0))

        item = {"amplitude": amplitude, "sample_rate": sample_rate}
        if self.include_paths:
            item["path"] = path

        builder = DelegatingBlockBuilder()
        builder.add(item)
        yield builder.build()
