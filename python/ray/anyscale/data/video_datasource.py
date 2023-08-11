from typing import TYPE_CHECKING

from ray.data import DataContext
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.util import _check_import
from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
    _FileBasedDatasourceReader,
)
from ray.util import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI(stability="alpha")
class VideoDatasource(FileBasedDatasource):
    def create_reader(self, **kwargs):
        return _VideoDatasourceReader(self, **kwargs)

    def _read_stream(self, f: "pyarrow.NativeFile", path: str, **reader_args):
        _check_import(self, module="decord", package="decord")
        from decord import VideoReader

        reader = VideoReader(f)
        ctx = DataContext.get_current()
        output_buffer = BlockOutputBuffer(
            block_udf=None, target_max_block_size=ctx.target_max_block_size
        )

        for frame_index, frame in enumerate(reader):
            item = {"frame": frame.asnumpy(), "frame_index": frame_index}
            if reader_args.get("include_paths", False):
                item["path"] = path

            output_buffer.add(item)

            if output_buffer.has_next():
                yield output_buffer.next()

        output_buffer.finalize()
        if output_buffer.has_next():
            yield output_buffer.next()


class _VideoDatasourceReader(_FileBasedDatasourceReader):
    # The compression ratio for a SewerAI video is 5.82GB (in-memory size) to 58.91 MB
    # (on-disk file size) = 98.81 ~= 100.
    COMPRESSION_RATIO = 100

    def estimate_inmemory_data_size(self) -> int:
        return sum(self._file_sizes) * self.COMPRESSION_RATIO
