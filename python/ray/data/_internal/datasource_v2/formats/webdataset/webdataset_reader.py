"""Worker-side reader for WebDataset tar shards."""

import io
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
from ray.data._internal.util import infer_compression
from ray.data.context import DataContext
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


def rows_to_arrow(rows: List[Dict[str, Any]]) -> pa.Table:
    from ray.data.block import BlockAccessor, BlockType

    columns = list(dict.fromkeys(key for row in rows for key in row))
    return BlockAccessor.batch_to_block(
        # A sample that lacks a key gets a null in that column.
        {column: [row.get(column) for row in rows] for column in columns},
        block_type=BlockType.ARROW,
    )


@DeveloperAPI
class WebDatasetReader(Reader[FileManifest]):
    """Reads the tar shards in a manifest and yields Arrow tables.

    Each shard is read sequentially from start to end.
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
        """Initialize the reader.

        Args:
            filesystem: Filesystem the tar shards are opened with.
            decoder: Decoder spec applied to each sample, or ``None`` to skip.
            fileselect: Patterns or function selecting files while reading.
            filerename: Patterns or function renaming files while reading.
            suffixes: Patterns or function selecting suffixes while grouping.
            verbose_open: Print progress when starting and finishing each tar.
            expand_json: Expand the ``json`` member's fields into columns.
            allow_unsafe_deserialization: Allow pickle/torch deserialization in
                the default decoder.
            open_stream_args: Arguments for ``open_input_stream``.
                ``buffer_size`` defaults to
                ``DataContext.streaming_read_buffer_size``.
            synthesized_columns: Columns computed from the read position, such
                as the path column.
        """
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
            with self._open_stream(path) as stream:
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
                    # Grayscale and color images get the same ndim, so every
                    # chunk converts to the same Arrow tensor type.
                    expand_grayscale=True,
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

    def _open_stream(self, path: str) -> "pa.NativeFile":
        open_args = dict(self._open_stream_args)
        open_args.setdefault(
            "buffer_size", DataContext.get_current().streaming_read_buffer_size
        )
        compression = open_args.get("compression")
        if compression is None:
            compression = infer_compression(path)
        if compression != "snappy":
            return self._filesystem.open_input_stream(path, **open_args)
        # Arrow can't decompress Snappy as a stream, so decompress the whole
        # file with python-snappy.
        import snappy
        from pyarrow.fs import HadoopFileSystem

        open_args["compression"] = None
        filesystem = self._filesystem
        if hasattr(filesystem, "unwrap"):
            filesystem = filesystem.unwrap()
        decompressed = io.BytesIO()
        with self._filesystem.open_input_stream(path, **open_args) as file:
            if isinstance(filesystem, HadoopFileSystem):
                snappy.hadoop_snappy.stream_decompress(src=file, dst=decompressed)
            else:
                snappy.stream_decompress(src=file, dst=decompressed)
        decompressed.seek(0)
        return pa.PythonFile(decompressed, mode="r")

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
