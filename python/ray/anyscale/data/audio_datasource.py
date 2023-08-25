from typing import TYPE_CHECKING, Iterator

from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block
from ray.data.context import DataContext
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

        ctx = DataContext.get_current()
        output_buffer = BlockOutputBuffer(
            block_udf=None, target_max_block_size=ctx.target_max_block_size
        )

        item = {"amplitude": reader[:].asnumpy()}
        if reader_args.get("include_paths", False):
            item["path"] = path
        output_buffer.add(item)

        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()
