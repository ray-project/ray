# Copyright (2023 and onwards) Anyscale, Inc.

import io
from typing import TYPE_CHECKING, Iterator

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI(stability="alpha")
class AudioDatasource(FileBasedDatasource):
    DEFAULT_SAMPLE_RATE = 44100

    def _read_stream(
        self,
        f: "pyarrow.NativeFile",
        path: str,
        **reader_args,
    ) -> Iterator[Block]:
        import soundfile

        # `soundfile` doesn't support reading from a `pyarrow.NativeFile` directly, so
        # we need to read the file into memory first.
        stream = io.BytesIO(f.read())
        amplitude, _ = soundfile.read(stream, always_2d=True, dtype="float32")

        # (amplitude, channels) -> (channels, amplitude)
        amplitude = amplitude.transpose((1, 0))

        item = {"amplitude": amplitude}
        if reader_args.get("include_paths", False):
            item["path"] = path

        builder = DelegatingBlockBuilder()
        builder.add(item)
        yield builder.build()
