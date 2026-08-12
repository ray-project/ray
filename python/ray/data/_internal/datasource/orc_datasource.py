from typing import TYPE_CHECKING, Iterator

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow
    import pyarrow.fs


class ORCDatasource(FileBasedDatasource):
    """A datasource that reads ORC files."""

    _FILE_EXTENSIONS = ["orc"]

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import pyarrow as pa
        from pyarrow import orc

        orc_file = orc.ORCFile(f)
        # Read one stripe at a time rather than the whole file to bound per-task
        # memory usage on large files. Output block shaping (coalescing small
        # stripes and splitting large ones to the target block size) is handled
        # by the read operator's BlockMapTransformFn, so no manual buffering is
        # needed here.
        for stripe_index in range(orc_file.nstripes):
            yield pa.Table.from_batches([orc_file.read_stripe(stripe_index)])

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # ORC stores its metadata footer at the end of the file, so reading
        # requires a seekable file (open_input_file) rather than a sequential
        # input stream.
        return filesystem.open_input_file(path)
