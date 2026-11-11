"""``TextReader``: reads whole text files into one ``text`` column of lines.

PyArrow's dataset layer has no plain-text format, so this reader does not
extend ``FileReader``. It implements ``Reader.read`` directly, the way the
MCAP reader does: open each file the manifest names, decode it with the
user's encoding, split it into lines, and finish the table with the
path-derived partition columns and synthesized columns.

Line semantics match V1 ``TextDatasource``: ``str.splitlines()`` decides
where lines end (``\\n``, ``\\r\\n``, ``\\r`` and the other Unicode line
boundaries), a last line without a trailing newline still counts, and
``drop_empty_lines`` removes lines that are empty or whitespace only. A
compressed file is decoded by its extension, as V1 does through
``infer_compression``.
"""

import io
from functools import partial
from typing import Any, Iterator, Optional, Sequence, Set, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem, HadoopFileSystem, LocalFileSystem

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
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

# The one data column ``read_text`` produces. V1 ``TextDatasource`` reads it
# from here, so the name has one source.
TEXT_COLUMN_NAME = "text"


@DeveloperAPI
class TextReader(Reader[FileManifest]):
    """Reads the text files a manifest assigns to one task, one table per file.

    Created by ``TextScanner.create_reader`` with every pushdown applied:
    the projected columns, the per-manifest row limit and the partition
    predicate, which decides per file whether it is opened at all.
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
            limit: Stop after this many rows per manifest (a read task may
                receive several manifests; the ``Limit`` operator above enforces
                the total).
            filesystem: Filesystem the paths resolve against; local when ``None``.
            partitioning: Path partitioning whose values become columns.
            partition_predicate: Pushed-down predicate over partition columns.
                Evaluated once per file on its path values, with nulls for
                keys the path lacks; a file it rejects is not opened.
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
        """Read the files named by ``input_split``, one table per manifest row.

        A path listed twice is read twice, as ``FileReader`` does. With
        ``limit``, reading stops at that many rows: no later file is opened.
        A file whose partition values fail the partition predicate is not
        opened either. Empty files yield nothing.
        """
        from ray.data.context import DataContext

        if len(input_split) == 0:
            return
        # Whole files only: byte-range chunks are a follow-up on #67725.
        assert all(
            metadata is None for metadata in input_split.file_chunk_metadatas
        ), "TextReader reads whole files; the manifest carries chunk metadata"
        retried_io_errors = DataContext.get_current().retried_io_errors
        remaining = self._limit
        for path in input_split.paths:
            if remaining is not None and remaining <= 0:
                return
            path = str(path)
            if not self._partition_matches(path):
                continue
            tables = iterate_with_retry(
                partial(self._read_file, path, remaining),
                f"read text file {path}",
                match=retried_io_errors,
            )
            for table in tables:
                if remaining is not None:
                    remaining -= table.num_rows
                yield table

    def _read_file(self, path: str, limit: Optional[int]) -> Iterator[pa.Table]:
        """Yield the lines of one file as a table, at most ``limit`` of them.

        Reopens the file on every call, so a retry replays the same rows.
        """
        lines = self._read_bytes(path).decode(self._encoding).splitlines()
        if self._drop_empty_lines:
            lines = [line for line in lines if line.strip() != ""]
        if limit is not None:
            lines = lines[:limit]
        if not lines:
            return
        if self._columns is not None and TEXT_COLUMN_NAME not in self._columns:
            # Projected away (``count()``, ``select_columns(["year"])``): the
            # file is still decoded for its row count, but no string column
            # is built. ``_finish`` replaces the stub with the wanted columns.
            table = pa.table(
                {_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(len(lines))}
            )
        else:
            # Past 2 GiB of text, ``pa.array`` returns a chunked array rather
            # than overflowing the int32 offsets; the table takes either.
            table = pa.table({TEXT_COLUMN_NAME: pa.array(lines, type=pa.string())})
        yield self._finish(table, path)

    def _read_bytes(self, path: str) -> bytes:
        """Return the file's decompressed bytes, inferring the codec from its name.

        Mirrors V1's ``FileBasedDatasource._open_input_source``: Arrow decodes
        gzip, bz2, zstd, lz4 and brotli streams; snappy has no streaming
        decoder in Arrow, so python-snappy decompresses it.
        """
        filesystem = self._filesystem or LocalFileSystem()
        compression = infer_compression(path)
        if compression == "snappy":
            # Arrow's own codec detection raises on ``.snappy``; open raw.
            with filesystem.open_input_stream(path, compression=None) as f:
                return _decompress_snappy(f, filesystem)
        with filesystem.open_input_stream(path, compression=compression) as f:
            return f.readall()

    def _partition_matches(self, path: str) -> bool:
        """Evaluate the partition predicate on this file's path values.

        A key the path lacks is a null, the value the file's rows get, so
        ``col("year").is_null()`` keeps a root file and ``col("year") != 2024``
        drops it, as filtering its rows would.
        """
        if self._partition_predicate is None:
            return True
        referenced = dict.fromkeys(get_column_references(self._partition_predicate))
        if not referenced:
            # Nothing path-dependent to decide here; leave it to the rows.
            return True
        parsed = self._partition_parser(path) if self._partition_parser else {}
        # Same evaluator as listing-time pruning, so both agree on every path.
        # A key the path lacks is passed as ``None`` explicitly: the evaluator
        # types it by the schema like any other value, and a null result
        # (``null != 2024``) is False, as filtering the rows would be.
        values = {name: parsed.get(name) for name in referenced}
        return bool(
            _evaluate_path_predicate(
                self._partition_predicate, values, self._schema, path=path
            )
        )

    def _finish(self, table: pa.Table, path: str) -> pa.Table:
        """Append partition and synthesized columns, then apply the projection."""
        wanted = set(self._columns) if self._columns is not None else None
        num_rows = table.num_rows
        unit = ReadUnit(id=path, source=path, count=1)
        table = self._append_partition_columns(table, path, wanted)
        table = self._append_synthesized_columns(table, unit, wanted)
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
        # The table holds plain strings, so it can't carry a pickled-object
        # column; the gate is called anyway so every reader of external
        # input goes through it.
        raise_on_pickle_object_columns(table)
        return table

    def _fill_missing_planned_columns(
        self, table: pa.Table, wanted: Optional[Set[str]]
    ) -> pa.Table:
        """Null-fill planned columns this file's path lacks, in schema order.

        A file outside the partition directories (a root file next to
        ``year=2024/``) has no value for ``year``; every task's blocks must
        still share the planned schema.
        """
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
        """Append the partition values parsed from ``path``, if wanted.

        A path value replaces a same-named column, as ``FileReader`` does.
        """
        if self._partition_parser is None:
            return table
        num_rows = table.num_rows
        for name, value in self._partition_parser(path).items():
            if wanted is not None and name not in wanted:
                continue
            if name in table.column_names:
                table = table.drop([name])
            table = table.append_column(
                name, self._partition_value_array(name, value, num_rows)
            )
        return table

    def _append_synthesized_columns(
        self, table: pa.Table, unit: ReadUnit, wanted: Optional[Set[str]]
    ) -> pa.Table:
        """Append the wanted synthesized columns, replacing any of the same name."""
        num_rows = table.num_rows
        position = ReadUnitPosition(unit=unit, rows_before=0)
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
    if isinstance(base, HadoopFileSystem):
        snappy.hadoop_snappy.stream_decompress(src=file, dst=stream)
    else:
        snappy.stream_decompress(src=file, dst=stream)
    return stream.getvalue()
