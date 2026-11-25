"""``TextReader``: reads whole text files into one ``text`` column of lines.

``pyarrow.dataset`` has no plain-text format, so this implements ``Reader.read``
directly, like the MCAP reader. Line semantics match V1 ``TextDatasource``:
``str.splitlines()`` splits, and ``drop_empty_lines`` drops whitespace-only lines.
"""

import codecs
import io
from contextlib import closing, contextmanager
from functools import partial
from itertools import islice
from typing import (
    Any,
    Generator,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import pyarrow as pa
from pyarrow.fs import FileSystem, HadoopFileSystem, LocalFileSystem

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource.text_datasource import TEXT_COLUMN_NAME
from ray.data._internal.datasource_v2.common.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
)
from ray.data._internal.datasource_v2.common.partitioning_utils import (
    _evaluate_path_predicate,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnit
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import infer_compression, iterate_with_retry
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionParser,
    _broadcast_partition_value,
)
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI

# Keep decoding and Python row accumulation bounded independently of the
# downstream block size. A single line can still exceed these sizes.
_READ_CHUNK_SIZE = 64 * 1024
_TARGET_BATCH_SIZE = 1024 * 1024
_LINE_ENDINGS = "\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029"
# CodecInfo.incrementaldecoder being present doesn't guarantee equivalence
# with bytes.decode. In particular, the UTF-16/32 incremental decoders require
# a BOM, and UTF-8-SIG ignores an incomplete initial BOM at EOF.
_STREAMING_ENCODINGS = frozenset(
    {
        "ascii",
        "iso8859-1",
        "utf-8",
        "utf-16-le",
        "utf-16-be",
        "utf-32-le",
        "utf-32-be",
    }
)


def _iter_line_batches(
    stream: pa.NativeFile, encoding: str, read_size: int
) -> Generator[List[str], None, None]:
    """Read lines with the semantics of ``bytes.decode(encoding).splitlines()``.

    Args:
        stream: Decompressed input stream.
        encoding: Python text encoding.
        read_size: Decompressed bytes per chunk for streaming encodings.

    Yields:
        List[str]: Complete lines without their line endings.
    """
    codec = codecs.lookup(encoding)
    if codec.name not in _STREAMING_ENCODINGS or codec.incrementaldecoder is None:
        # Yield the temporary directly so a caller that keeps only a prefix
        # can release the full list while this generator is suspended.
        yield stream.readall().decode(encoding).splitlines()
        return

    decoder = codec.incrementaldecoder()
    fragments = []
    skip_lf = False
    final = False
    while not final:
        chunk = _read_chunk(stream, read_size)
        final = len(chunk) < read_size
        text = decoder.decode(chunk, final=final)
        if not text:
            continue
        if skip_lf:
            text = text.removeprefix("\n")
        skip_lf = text.endswith("\r")
        lines = text.splitlines()
        remainder = None
        if text and not final and text[-1] not in _LINE_ENDINGS:
            remainder = lines.pop()
        if fragments and lines:
            fragments.append(lines[0])
            lines[0] = "".join(fragments)
            fragments.clear()
        if lines:
            yield lines
        if remainder is not None:
            # Scan only newly decoded text. Joining an unfinished long line
            # on every chunk would make its processing quadratic.
            fragments.append(remainder)
    if fragments:
        yield ["".join(fragments)]


def _read_chunk(stream: pa.NativeFile, size: int) -> bytes:
    """Fill one chunk so short reads don't change retry batch boundaries.

    Args:
        stream: Decompressed input stream.
        size: Number of bytes per chunk, except the final chunk at EOF.

    Returns:
        A full chunk, or fewer bytes once the stream reaches EOF.
    """
    chunk = stream.read(size)
    if not chunk or len(chunk) == size:
        return chunk
    fragments = [chunk]
    remaining = size - len(chunk)
    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            break
        fragments.append(chunk)
        remaining -= len(chunk)
    return b"".join(fragments)


def _raise_on_text_partition_field(partition_names) -> None:
    """Reject a partition key that would replace the lines column."""
    if TEXT_COLUMN_NAME in partition_names:
        raise ValueError(
            f"Partition field {TEXT_COLUMN_NAME!r} clashes with the column "
            "`read_text` produces. Rename the partition directory."
        )


@DeveloperAPI
class TextReader(Reader[FileManifest]):
    """Read a manifest's text files in bounded batches without splitting files.

    ASCII, Latin-1, UTF-8, and explicit-endian UTF-16/32 use memory proportional
    to the stream buffer, the longest line, and a batch of decoded rows. Other
    encodings use whole-file decoding. Snappy uses whole-file decompression.
    A pushed-down limit can leave errors in an unread suffix undetected.
    """

    def __init__(
        self,
        *,
        encoding: str = "utf-8",
        drop_empty_lines: bool = True,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        partition_predicate: Optional[Expr] = None,
        synthesized_columns: Sequence[SynthesizedColumn] = (),
        schema: Optional[pa.Schema] = None,
    ):
        """Initialize the reader.

        Args:
            encoding: Codec the file bytes are decoded with.
            drop_empty_lines: Whether to drop lines that are empty or
                whitespace only.
            columns: Columns to produce, in order; ``None`` for all of them.
            limit: Stop after this many rows per manifest.
            filesystem: Filesystem the paths resolve against; local when ``None``.
            partitioning: Path partitioning whose values become columns.
            partition_predicate: Pushed-down predicate over partition columns,
                evaluated per file on its path values; a rejected file is not opened.
            synthesized_columns: Columns appended to every table rather than
                read, e.g. ``PathColumn`` for ``include_paths``.
            schema: Dataset schema, used to type partition columns.
        """
        self._encoding = encoding
        self._drop_empty_lines = drop_empty_lines
        self._columns = list(columns) if columns is not None else None
        self._limit = limit
        self._filesystem = filesystem
        self._partition_parser = (
            PathPartitionParser(partitioning) if partitioning is not None else None
        )
        self._partition_predicate = partition_predicate
        self._synthesized_columns: Tuple[SynthesizedColumn, ...] = tuple(
            synthesized_columns
        )
        self._schema = schema

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read the files named by ``input_split`` in manifest order."""
        from ray.data.context import DataContext

        if len(input_split) == 0:
            return
        # Whole files only: byte-range chunks are a follow-up on #67725.
        assert all(
            metadata is None for metadata in input_split.file_chunk_metadatas
        ), "TextReader reads whole files; the manifest carries chunk metadata"
        context = DataContext.get_current()
        retried_io_errors = context.retried_io_errors
        buffer_size = context.streaming_read_buffer_size
        read_chunk_size = min(
            context.target_max_block_size or _TARGET_BATCH_SIZE, _TARGET_BATCH_SIZE
        )
        if self._limit is not None:
            read_chunk_size = min(read_chunk_size, _READ_CHUNK_SIZE)
        remaining = self._limit
        for path in input_split.paths:
            if remaining is not None and remaining <= 0:
                return
            path = str(path)
            if not self._partition_matches(path):
                continue
            tables = iterate_with_retry(
                partial(self._read_file, path, remaining, buffer_size, read_chunk_size),
                f"read text file {path}",
                match=retried_io_errors,
            )
            with closing(tables):
                for table in tables:
                    if remaining is not None:
                        remaining -= table.num_rows
                    yield table

    def _read_file(
        self, path: str, limit: Optional[int], buffer_size: int, read_chunk_size: int
    ) -> Generator[pa.Table, None, None]:
        """Reopen a file and replay the same batches on every retry.

        Args:
            path: File to read from the beginning.
            limit: Maximum number of retained rows from this file.
            buffer_size: Filesystem stream buffer size.
            read_chunk_size: Number of decompressed bytes to decode per batch.

        Yields:
            pa.Table: Tables with deterministic boundaries across short reads.
        """
        rows_before = 0
        final_table = None
        read_size = min(read_chunk_size, buffer_size or read_chunk_size)
        with self._open_input_stream(path, buffer_size) as stream:
            for lines in _iter_line_batches(stream, self._encoding, read_size):
                if limit is not None:
                    remaining = min(limit - rows_before, len(lines))
                    if self._drop_empty_lines:
                        lines = list(
                            islice((line for line in lines if line.strip()), remaining)
                        )
                    else:
                        del lines[remaining:]
                elif self._drop_empty_lines:
                    lines = [line for line in lines if line.strip()]
                # Bound row counts as well as input bytes, including when
                # every line is empty and the text column is projected away.
                for start in range(0, len(lines), _ARROW_DEFAULT_BATCH_SIZE):
                    batch = lines[start : start + _ARROW_DEFAULT_BATCH_SIZE]
                    table = self._finish(self._make_table(batch), path, rows_before)
                    rows_before += len(batch)
                    if limit is not None and rows_before >= limit:
                        final_table = table
                        break
                    yield table
                if final_table is not None:
                    break
        # Close before exposing the batch that satisfies the limit: downstream
        # can cancel the task without ever resuming it after its final yield.
        if final_table is not None:
            yield final_table

    def _make_table(self, lines: List[str]) -> pa.Table:
        """Build the text column, or just preserve row counts for a projection."""
        if self._columns is not None and TEXT_COLUMN_NAME not in self._columns:
            # ``text`` is projected away (``count()``): keep only the row count.
            return pa.table(
                {_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(len(lines))}
            )
        return pa.table({TEXT_COLUMN_NAME: pa.array(lines, type=pa.string())})

    @contextmanager
    def _open_input_stream(
        self, path: str, buffer_size: int
    ) -> Iterator[pa.NativeFile]:
        """Open a decompressed stream, retaining the whole-file Snappy fallback."""
        filesystem = self._filesystem or LocalFileSystem()
        compression = infer_compression(path)
        # Read in blocks of this size, as the V1 reader does. An HTTP server
        # without range requests (such as ``python -m http.server``) then
        # returns a file up to this size in one request; fsspec's own 5 MiB
        # blocks would need a range request for anything larger.
        if compression == "snappy":
            # Arrow's own codec detection raises on ``.snappy``; open raw.
            with filesystem.open_input_stream(
                path, compression=None, buffer_size=buffer_size
            ) as f:
                with pa.BufferReader(_decompress_snappy(f, filesystem)) as stream:
                    yield stream
            return
        with filesystem.open_input_stream(
            path, compression=compression, buffer_size=buffer_size
        ) as f:
            yield f

    def _partition_matches(self, path: str) -> bool:
        """Return whether the path's partition values pass the pushed-down predicate."""
        if self._partition_predicate is None:
            return True
        referenced = dict.fromkeys(get_column_references(self._partition_predicate))
        if not referenced:
            return True
        parsed = self._partition_parser(path) if self._partition_parser else {}
        # Same evaluator as listing-time pruning; a key the path lacks is a null.
        values = {name: parsed.get(name) for name in referenced}
        return bool(
            _evaluate_path_predicate(
                self._partition_predicate, values, self._schema, path=path
            )
        )

    def _finish(self, table: pa.Table, path: str, rows_before: int) -> pa.Table:
        """Append partition and synthesized columns, then apply the projection."""
        wanted = set(self._columns) if self._columns is not None else None
        num_rows = table.num_rows
        unit = ReadUnit(id=path, source=path, count=1)
        table = self._append_partition_columns(table, path, wanted)
        table = self._append_synthesized_columns(table, unit, wanted, rows_before)
        table = self._fill_missing_planned_columns(table, wanted)
        if self._columns is not None:
            produced = set(table.column_names)
            kept = [c for c in self._columns if c in produced]
            if not kept and num_rows > 0:
                # ``count()`` projects no columns; a stub keeps the row count.
                if _BATCH_SIZE_PRESERVING_STUB_COL_NAME not in produced:
                    table = table.append_column(
                        _BATCH_SIZE_PRESERVING_STUB_COL_NAME, pa.nulls(num_rows)
                    )
                kept = [_BATCH_SIZE_PRESERVING_STUB_COL_NAME]
            table = table.select(kept)
        raise_on_pickle_object_columns(table)
        return table

    def _fill_missing_planned_columns(
        self, table: pa.Table, wanted: Optional[Set[str]]
    ) -> pa.Table:
        """Null-fill planned columns this file's path lacks, in schema order."""
        schema = self._schema
        if schema is None:
            return table
        num_rows = table.num_rows
        for field in schema:
            if field.name in table.column_names:
                continue
            if wanted is not None and field.name not in wanted:
                continue
            table = table.append_column(field.name, pa.nulls(num_rows, type=field.type))
        planned = [name for name in schema.names if name in table.column_names]
        extra = [name for name in table.column_names if name not in schema.names]
        return table.select(planned + extra)

    def _append_partition_columns(
        self, table: pa.Table, path: str, wanted: Optional[Set[str]]
    ) -> pa.Table:
        """Append the partition values parsed from ``path``, if wanted."""
        if self._partition_parser is None:
            return table
        num_rows = table.num_rows
        partitions = self._partition_parser(path)
        _raise_on_text_partition_field(partitions)
        for name, value in partitions.items():
            if wanted is not None and name not in wanted:
                continue
            if name in table.column_names:
                table = table.drop([name])
            table = table.append_column(
                name, self._partition_value_array(name, value, num_rows)
            )
        return table

    def _append_synthesized_columns(
        self,
        table: pa.Table,
        unit: ReadUnit,
        wanted: Optional[Set[str]],
        rows_before: int,
    ) -> pa.Table:
        """Append the wanted synthesized columns, replacing any of the same name."""
        num_rows = table.num_rows
        position = ReadUnitPosition(unit=unit, rows_before=rows_before)
        for column in self._synthesized_columns:
            if wanted is not None and column.name not in wanted:
                continue
            if column.name in table.column_names:
                table = table.drop([column.name])
            table = table.append_column(column.name, column.compute(position, num_rows))
        return table

    def _partition_value_array(self, name: str, value: Any, num_rows: int) -> pa.Array:
        """Broadcast one path-derived partition value, typed by the schema if known."""
        return _broadcast_partition_value(name, value, num_rows, self._schema)


def _decompress_snappy(file: pa.NativeFile, filesystem: FileSystem) -> bytes:
    """Decompress a snappy stream; HDFS writes the hadoop framing."""
    import snappy

    stream = io.BytesIO()
    unwrap = getattr(filesystem, "unwrap", None)
    base = unwrap() if callable(unwrap) else filesystem
    decompressor_cls = (
        snappy.HadoopStreamDecompressor
        if isinstance(base, HadoopFileSystem)
        else snappy.StreamDecompressor
    )
    snappy.stream_decompress(src=file, dst=stream, decompressor_cls=decompressor_cls)
    return stream.getvalue()
