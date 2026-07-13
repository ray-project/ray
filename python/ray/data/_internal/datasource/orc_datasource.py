from typing import TYPE_CHECKING, Iterator

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow


class ORCDatasource(FileBasedDatasource):
    """A datasource that reads ORC files."""

    _FILE_EXTENSIONS = ["orc"]

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        from pyarrow import orc

        yield orc.read_table(f)

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # ORC stores its metadata footer at the end of the file, so reading
        # requires a seekable file rather than a sequential input stream.
        return filesystem.open_input_file(path)
