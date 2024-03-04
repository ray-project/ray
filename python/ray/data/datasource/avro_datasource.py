from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Union, Any
from io import BytesIO

from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    import pyarrow


@PublicAPI(stability="alpha")
class AvroDatasource(FileBasedDatasource):
    """Avro datasource, for reading and writing Avro files."""

    _FILE_EXTENSIONS = ["avro"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        # avro_schema: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        # self.avro_schema = avro_schema

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import fastavro

        builder = DelegatingBlockBuilder()

        # Assuming `f.readall()` reads the entire Avro file into memory
        avro_file = BytesIO(f.readall())

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
