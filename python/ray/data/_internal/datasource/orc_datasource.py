from typing import TYPE_CHECKING, Iterator

from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


class ORCDatasource(FileBasedDatasource):
    """A datasource that reads ORC files."""

    _FILE_EXTENSIONS = ["orc"]

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import pyarrow as pa
        from pyarrow import orc

        orc_file = orc.ORCFile(f)

        # Read one stripe at a time rather than the whole file at once to bound
        # per-task memory usage on large files. Stripes are accumulated in an
        # output buffer so that the yielded blocks respect the target block size
        # (small stripes are coalesced, large ones are split).
        ctx = DataContext.get_current()
        output_buffer = BlockOutputBuffer(
            OutputBlockSizeOption.of(target_max_block_size=ctx.target_max_block_size)
        )
        for stripe_index in range(orc_file.nstripes):
            batch = orc_file.read_stripe(stripe_index)
            output_buffer.add_block(pa.Table.from_batches([batch]))
            yield from output_buffer.iter_ready_blocks()

        output_buffer.finalize()
        yield from output_buffer.iter_ready_blocks()

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # ORC stores its metadata footer at the end of the file, so reading
        # requires a seekable file (open_input_file) rather than a sequential
        # input stream. open_args (e.g. compression, buffer_size) only apply to
        # sequential stream reads and ORC handles compression internally, so
        # they are intentionally not forwarded here.
        return filesystem.open_input_file(path)
