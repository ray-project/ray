from io import BytesIO
from typing import TYPE_CHECKING, Iterator, List, Union

from ray.data._internal.util import _check_import
from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

MAX_BLOCK_SIZE = 128

if TYPE_CHECKING:
    import pyarrow


@PublicAPI(stability="alpha")
class AvroDatasource(FileBasedDatasource):
    """A datasource that reads Avro files."""

    _FILE_EXTENSIONS = ["avro"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        target_max_block_size: int = MAX_BLOCK_SIZE,
        **file_based_datasource_kwargs,
    ):
        _check_import(self, module="google.cloud", package="bigquery")
        # Ensure target_max_block_size is valid (e.g., positive integer)
        if target_max_block_size <= 0:
            raise ValueError("target_max_block_size must be a positive integer")

        self._target_max_block_size = target_max_block_size
        super().__init__(paths, **file_based_datasource_kwargs)

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import fastavro

        from ray.data._internal.output_buffer import BlockOutputBuffer

        assert (
            self._target_max_block_size is not None
        ), "target_max_block_size must be set before running"

        file_content = f.readall()

        avro_file = BytesIO(file_content)

        # Read the Avro file. This assumes the Avro file includes its schema.
        reader = fastavro.reader(avro_file)

        output_buffer = BlockOutputBuffer(self._target_max_block_size)
        for record in reader:
            output_buffer.add(record)
            while output_buffer.has_next():
                yield output_buffer.next()

        output_buffer.finalize()
        while output_buffer.has_next():
            yield output_buffer.next()
