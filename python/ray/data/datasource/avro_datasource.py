from typing import TYPE_CHECKING, Iterator, List, Union

from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI(stability="alpha")
class AvroDatasource(FileBasedDatasource):
    """A datasource that reads Avro files."""

    _FILE_EXTENSIONS = ["avro"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        _check_import(self, module="fastavro", package="fastavro")

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import fastavro

        # Read the Avro file. This assumes the Avro file includes its schema.
        reader = fastavro.reader(f)

        ctx = DataContext.get_current()
        output_buffer = BlockOutputBuffer(ctx.target_max_block_size)
        for record in reader:
            output_buffer.add(record)
            while output_buffer.has_next():
                yield output_buffer.next()

        output_buffer.finalize()
        while output_buffer.has_next():
            yield output_buffer.next()
