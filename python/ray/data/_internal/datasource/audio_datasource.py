import io
from typing import TYPE_CHECKING, Iterator, List, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


class AudioDatasource(FileBasedDatasource):
    _FILE_EXTENSIONS = [
        "mp3",
        "wav",
        "aac",
        "flac",
        "ogg",
        "m4a",
        "wma",
        "alac",
        "aiff",
        "pcm",
        "amr",
        "opus",
        "ra",
        "rm",
        "au",
        "mid",
        "midi",
        "caf",
    ]

    def __init__(
        self,
        paths: Union[str, List[str]],
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="soundfile", package="soundfile")

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import soundfile

        # `soundfile` doesn't support reading from a `pyarrow.NativeFile` directly, so
        # we need to read the file into memory first.
        stream = io.BytesIO(f.read())
        amplitude, sample_rate = soundfile.read(stream, always_2d=True, dtype="float32")

        # (amplitude, channels) -> (channels, amplitude)
        amplitude = amplitude.transpose((1, 0))

        builder = DelegatingBlockBuilder()
        builder.add({"amplitude": amplitude, "sample_rate": sample_rate})
        yield builder.build()
