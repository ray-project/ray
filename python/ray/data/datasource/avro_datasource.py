from io import BytesIO
from typing import TYPE_CHECKING, Iterator, List, Union

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block
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

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import fastavro

        builder = DelegatingBlockBuilder()

        # Check if the file is empty
        # Assuming `f.readall()` reads the entire Avro file into memory
        file_content = f.readall()
        if not file_content:
            yield builder.build()  # Yield an empty block for an empty file
            return

        avro_file = BytesIO(file_content)

        # Read the Avro file. This assumes the Avro file includes its schema.
        reader = fastavro.reader(avro_file)

        # Collect records into a list (consider chunking for large files)
        records = [record for record in reader]

        # If there are records, convert them into a PyArrow Table
        if records:
            for record in records:
                builder.add(record)

            # Yield the table as a block of data
        block = builder.build()
        yield block
