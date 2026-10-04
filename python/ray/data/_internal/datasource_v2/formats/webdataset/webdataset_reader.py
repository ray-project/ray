"""Worker-side reader for WebDataset tar shards."""

from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Sequence, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.webdataset.webdataset_utils import (
    WEBDATASET_READ_CHUNK_SIZE,
    iter_webdataset_rows,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnit
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


def rows_to_arrow(rows: List[Dict[str, Any]]) -> pa.Table:
    """Convert a chunk of sample rows into an Arrow table.

    Goes through the same ``pd.DataFrame.from_records`` call the legacy reader
    emits, then Ray's pandas-to-Arrow conversion, so column types (including
    ndarray columns from decoded images) match what the legacy read path
    produces once its pandas blocks are converted.

    TODO: Verify against the legacy read path, and compare with building the
    table directly via ``pa.Table.from_pylist``.
    """
    import pandas as pd

    from ray.data.block import BlockAccessor

    return BlockAccessor.for_block(pd.DataFrame.from_records(rows)).to_arrow()


@DeveloperAPI
class WebDatasetReader(Reader[FileManifest]):
    """Reads the tar shards in a manifest and yields Arrow tables.

    Each shard is read sequentially from start to end (tar streams are not
    seekable here), and samples are emitted in tables of up to
    ``WEBDATASET_READ_CHUNK_SIZE`` rows so peak memory is bounded by a chunk,
    not by the shard.
    """

    def __init__(
        self,
        *,
        filesystem: "FileSystem",
        decoder: Optional[Any],
        fileselect: Optional[Any],
        filerename: Optional[Any],
        suffixes: Optional[Any],
        verbose_open: bool,
        expand_json: bool,
        allow_unsafe_deserialization: bool,
        open_stream_args: Optional[Dict[str, Any]] = None,
        synthesized_columns: Sequence[SynthesizedColumn] = (),
    ):
        self._filesystem = filesystem
        self._open_stream_args = open_stream_args or {}
        self._decoder = decoder
        self._fileselect = fileselect
        self._filerename = filerename
        self._suffixes = suffixes
        self._verbose_open = verbose_open
        self._expand_json = expand_json
        self._allow_unsafe_deserialization = allow_unsafe_deserialization
        self._synthesized_columns: Tuple[SynthesizedColumn, ...] = tuple(
            synthesized_columns
        )

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        retried_io_errors = DataContext.get_current().retried_io_errors
        for path in input_split.paths.tolist():
            unit = ReadUnit(id=path, source=path)
            rows_before = 0
            # TODO: V1 opens through ``_open_file_with_retry``; add the same
            # retry around opening the stream.
            with self._filesystem.open_input_stream(
                path, **self._open_stream_args
            ) as stream:
                rows = iter_webdataset_rows(
                    stream,
                    path,
                    decoder=self._decoder,
                    fileselect=self._fileselect,
                    filerename=self._filerename,
                    suffixes=self._suffixes,
                    verbose_open=self._verbose_open,
                    expand_json=self._expand_json,
                    allow_unsafe_deserialization=self._allow_unsafe_deserialization,
                    retried_io_errors=retried_io_errors,
                )
                chunk = []
                for row in rows:
                    chunk.append(row)
                    if len(chunk) >= WEBDATASET_READ_CHUNK_SIZE:
                        yield self._finalize(chunk, unit, rows_before)
                        rows_before += len(chunk)
                        chunk = []
                if chunk:
                    yield self._finalize(chunk, unit, rows_before)

    def _finalize(
        self, chunk: List[Dict[str, Any]], unit: ReadUnit, rows_before: int
    ) -> pa.Table:
        table = rows_to_arrow(chunk)
        position = ReadUnitPosition(unit=unit, rows_before=rows_before)
        for column in self._synthesized_columns:
            values = column.compute(position, table.num_rows)
            idx = table.schema.get_field_index(column.name)
            if idx == -1:
                table = table.append_column(column.name, values)
            else:
                table = table.set_column(idx, column.name, values)
        return table
