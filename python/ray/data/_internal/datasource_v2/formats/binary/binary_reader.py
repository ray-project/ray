"""``BinaryReader``: reads each file whole into one ``bytes`` row.

The simplest :class:`FileStreamReader`: a file is one row, and its contents
are the row. Every file holds exactly one row, so ``count()`` is answered from
the listing through :class:`FixedRowsPerFileMetadata`, as V1's
``_rows_per_file`` did.
"""

from typing import Iterator

import pyarrow as pa
from typing_extensions import override

from ray._private.utils import INT32_MAX
from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource.binary_datasource import BYTES_COLUMN_NAME
from ray.data._internal.datasource_v2.common.file_stream_reader import (
    FileStreamReader,
    FixedRowsPerFileMetadata,
)
from ray.util.annotations import DeveloperAPI

BINARY_SCHEMA = pa.schema([pa.field(BYTES_COLUMN_NAME, pa.binary())])


@DeveloperAPI
class BinaryReader(FileStreamReader, FixedRowsPerFileMetadata):
    """Reads the files a manifest assigns to one task, one row per file."""

    @override
    def _read_stream(self, stream: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        if not self._wants(BYTES_COLUMN_NAME):
            # ``bytes`` is projected away: keep the row, skip decoding it.
            yield pa.table({_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(1)})
            return
        # ``read_buffer`` hands the bytes to Arrow without a Python ``bytes``
        # copy in between.
        data = stream.read_buffer()
        if len(data) > INT32_MAX:
            raise ValueError(
                f"File {path!r} is {len(data)} bytes, but a `binary` cell holds at "
                f"most {INT32_MAX} bytes. Read it with the V1 reader by setting "
                "`ray.data.DataContext.get_current().use_datasource_v2 = False`."
            )
        offsets = pa.array([0, len(data)], type=pa.int32()).buffers()[1]
        column = pa.Array.from_buffers(pa.binary(), 1, [None, offsets, data])
        yield pa.table({BYTES_COLUMN_NAME: column})
