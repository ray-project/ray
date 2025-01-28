from collections import defaultdict
from typing import TYPE_CHECKING, Iterable

from .native_file_reader import NativeFileReader
from ray.data._internal.util import _check_import
from ray.data.block import DataBatch

if TYPE_CHECKING:
    import pyarrow


BATCH_SIZE = 128


class AvroReader(NativeFileReader):
    def __init__(self, **file_reader_kwargs):
        super().__init__(**file_reader_kwargs)

        _check_import(self, module="fastavro", package="fastavro")

    def read_stream(self, file: "pyarrow.NativeFile", path: str) -> Iterable[DataBatch]:
        import fastavro

        # Read the Avro file. This assumes the Avro file includes its schema.
        reader = fastavro.reader(file)

        batch = defaultdict(list)
        num_records = 0
        for record in reader:
            for key, value in record.items():
                batch[key].append(value)
            num_records += 1

            if num_records >= BATCH_SIZE:
                yield batch
                batch = defaultdict(list)
                num_records = 0

        if num_records > 0:
            yield batch
