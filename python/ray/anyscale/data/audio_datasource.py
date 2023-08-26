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
        """Args parsed from `reader_args`:
        - `sample_rate`: Sample rate for reading audio
        - `mono_audio`: If true, use mono signal; if false, use original layout
        """
        from decord import AudioReader

        sample_rate = reader_args.get(
            "sample_rate", AudioDatasource.DEFAULT_SAMPLE_RATE
        )
        mono_audio = reader_args.get("mono_audio", False)
        reader = AudioReader(f, sample_rate=sample_rate, mono=mono_audio)

        item = {"amplitude": reader[:].asnumpy()}
        if reader_args.get("include_paths", False):
            item["path"] = path

        builder = DelegatingBlockBuilder()
        builder.add(item)
        yield builder.build()
