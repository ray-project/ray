from typing import TYPE_CHECKING, Optional

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
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

        for frame_index, frame in enumerate(reader):
            item = {"frame": frame.asnumpy(), "frame_index": frame_index}
            if reader_args.get("include_paths", False):
                item["path"] = path

            builder = DelegatingBlockBuilder()
            builder.add(item)
            yield builder.build()


class _VideoDatasourceReader(_FileBasedDatasourceReader):
    # The compression ratio for a SewerAI video is 5.82GB (in-memory size) to 58.91 MB
    # (on-disk file size) = 98.81 ~= 100.
    COMPRESSION_RATIO = 100

    def estimate_inmemory_data_size(self) -> Optional[int]:
        total_size = 0
        for sz in self._file_sizes:
            if sz is not None:
                total_size += sz
        return total_size * self.COMPRESSION_RATIO
