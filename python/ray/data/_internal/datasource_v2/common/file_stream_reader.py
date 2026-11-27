"""``FileStreamReader``: the shared reader for formats that decode a file as one
byte stream.

Binary, text, image, NumPy, audio and video files have no row groups to seek
to and no columnar layout for ``pyarrow.dataset`` to scan: a reader opens the
file as a stream and decodes it from the first byte to the last. This module
holds everything about such a read that is not the decoding itself. A format
subclasses :class:`FileStreamReader` and implements :meth:`_read_stream`,
which turns one open stream into Arrow tables; the base class does the rest:

- opens each file through the filesystem with the user's
  ``arrow_open_stream_args``, inferring the compression codec from the path
  and decompressing Snappy with python-snappy, as V1 ``FileBasedDatasource``
  does;
- retries transient errors by reopening the file, so every attempt replays the
  same rows;
- evaluates a pushed-down partition predicate on the path before opening it;
- applies the per-task row limit;
- appends partition columns and synthesized columns such as ``path``, null-fills
  planned columns a file lacks, applies the projection, and refuses
  pickled-object columns.

:class:`FixedRowsPerFileMetadata` is the matching ``SupportsMetadata`` for a
format whose every file holds the same number of rows, such as one binary blob
or one image per file: ``count()`` then needs no file opened.
"""

import io
from abc import ABC, abstractmethod
from functools import partial
from typing import Any, Dict, Iterator, Optional, Sequence, Set, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem, HadoopFileSystem, LocalFileSystem
from typing_extensions import override

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.common.partitioning_utils import (
    _evaluate_path_predicate,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnit
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
    MetadataType,
    SupportsMetadata,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import (
    infer_compression,
    iterate_with_retry,
    make_async_gen,
)
from ray.data.block import BlockMetadata
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionParser,
    _broadcast_partition_value,
)
from ray.data.expressions import Expr
from ray.util.annotations import DeveloperAPI


def _raise_on_partition_field_clash(partition_names, column_names) -> None:
    """Reject a partition key that would replace a column the files produce."""
    for name in partition_names:
        if name in column_names:
            raise ValueError(
                f"Partition field {name!r} clashes with a column the files "
                "produce. Rename the partition directory."
            )


@DeveloperAPI
class FileStreamReader(Reader[FileManifest], ABC):
    """Reads each file of a manifest as one byte stream, one file at a time.

    Subclass this for a format that decodes a whole file from an open stream
    and implement :meth:`_read_stream`. Override :meth:`_open_stream` only
    when the format needs random access instead of a sequential stream.

    Attributes:
        num_threads_per_task: Files one read task opens at once; ``0`` reads
            them one after another. Raise it for a format whose files are
            small and many, as V1's ``ImageDatasource`` did with 8 threads.
            Tables still come out in manifest order, and ``preserve_order``
            forces ``0``.
    """

    num_threads_per_task: int = 0

    def __init__(
        self,
        *,
        filesystem: Optional[FileSystem] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        partitioning: Optional[Partitioning] = None,
        partition_predicate: Optional[Expr] = None,
        synthesized_columns: Sequence[SynthesizedColumn] = (),
        schema: Optional[pa.Schema] = None,
    ):
        """Initialize the reader.

        Args:
            filesystem: Filesystem the paths resolve against; local when ``None``.
            open_stream_args: Arguments for ``open_input_stream``.
                ``compression`` defaults to the codec the path's extension
                names and ``buffer_size`` to
                ``DataContext.streaming_read_buffer_size``.
            columns: Columns to produce, in order; ``None`` for all of them.
            limit: Stop after this many rows per manifest.
            partitioning: Path partitioning whose values become columns.
            partition_predicate: Pushed-down predicate over partition columns,
                evaluated per file on its path values; a rejected file is not
                opened.
            synthesized_columns: Columns appended to every table rather than
                read, e.g. ``PathColumn`` for ``include_paths``.
            schema: Dataset schema, used to type partition columns and to
                null-fill the columns a file's path lacks.
        """
        self._filesystem = filesystem
        self._open_stream_args = dict(open_stream_args or {})
        self._columns = list(columns) if columns is not None else None
        self._limit = limit
        self._partition_parser = (
            PathPartitionParser(partitioning) if partitioning is not None else None
        )
        self._partition_predicate = partition_predicate
        self._synthesized_columns: Tuple[SynthesizedColumn, ...] = tuple(
            synthesized_columns
        )
        self._schema = schema

    @abstractmethod
    def _read_stream(self, stream: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        """Decode one open file into Arrow tables.

        Yield the file's data columns only: partition columns, synthesized
        columns and the projection are applied afterwards. Check
        :meth:`_wants` before decoding a column the projection dropped; a
        table that is left with no wanted column keeps its row count through
        a stub column.

        Args:
            stream: The file, open for sequential reading and already
                decompressed.
            path: The file's path, for error messages.

        Returns:
            The file's data columns, as one or more tables.
        """
        ...

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read the files named by ``input_split``, in manifest order."""
        from ray.data.context import DataContext

        if len(input_split) == 0:
            return
        if any(metadata is not None for metadata in input_split.file_chunk_metadatas):
            raise ValueError(
                f"{type(self).__name__} reads whole files, but the manifest "
                "carries chunk metadata."
            )
        remaining = self._limit
        if remaining is not None and remaining <= 0:
            return
        ctx = DataContext.get_current()
        paths = [str(path) for path in input_split.paths]
        paths = [path for path in paths if self._partition_matches(path)]
        read_paths = partial(self._read_paths, ctx.retried_io_errors)
        num_threads = (
            0
            if ctx.execution_options.preserve_order
            else min(self.num_threads_per_task, len(paths))
        )
        if num_threads > 0:
            tables = make_async_gen(
                iter(paths),
                read_paths,
                preserve_ordering=True,
                num_workers=num_threads,
            )
        else:
            tables = read_paths(iter(paths))
        for table in tables:
            if remaining is not None:
                if table.num_rows > remaining:
                    table = table.slice(0, remaining)
                remaining -= table.num_rows
            yield table
            if remaining is not None and remaining <= 0:
                return

    def _read_paths(
        self, retried_io_errors: Sequence[str], paths: Iterator[str]
    ) -> Iterator[pa.Table]:
        """Yield the tables of each path in turn, retrying transient errors.

        Each attempt reopens the file and replays its tables from the start;
        ``iterate_with_retry`` skips the ones already yielded.
        """
        for path in paths:
            yield from iterate_with_retry(
                partial(self._read_file, path),
                f"read {path}",
                match=list(retried_io_errors),
            )

    def _read_file(self, path: str) -> Iterator[pa.Table]:
        """Yield the finished tables of one file; empty tables are dropped."""
        rows_before = 0
        with self._open_stream(path) as stream:
            for table in self._read_stream(stream, path):
                if table.num_rows == 0:
                    continue
                table = self._finish(table, path, rows_before)
                rows_before += table.num_rows
                yield table

    def _open_stream(self, path: str) -> pa.NativeFile:
        """Open ``path`` for sequential reading, decompressed.

        The codec comes from ``open_stream_args["compression"]`` or the path's
        extension. Arrow cannot stream Snappy, so a Snappy file is read whole
        and decompressed with python-snappy, as V1 does.
        """
        from ray.data.context import DataContext

        filesystem = self._filesystem or LocalFileSystem()
        open_args = dict(self._open_stream_args)
        compression = open_args.pop("compression", None)
        if compression is None:
            compression = infer_compression(path)
        # Read in blocks of this size, as the V1 reader does. An HTTP server
        # without range requests (such as ``python -m http.server``) then
        # returns a file up to this size in one request; fsspec's own 5 MiB
        # blocks would need a range request for anything larger.
        if open_args.get("buffer_size") is None:
            open_args[
                "buffer_size"
            ] = DataContext.get_current().streaming_read_buffer_size
        if compression == "snappy":
            # Arrow's own codec detection raises on ``.snappy``; open raw.
            with filesystem.open_input_stream(path, compression=None, **open_args) as f:
                return pa.BufferReader(_decompress_snappy(f, filesystem))
        return filesystem.open_input_stream(path, compression=compression, **open_args)

    def _wants(self, column: str) -> bool:
        """Whether the projection keeps ``column``; ``True`` without one."""
        return self._columns is None or column in self._columns

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
        table = self._append_synthesized_columns(table, unit, rows_before, wanted)
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
        _raise_on_partition_field_clash(partitions, table.column_names)
        for name, value in partitions.items():
            if wanted is not None and name not in wanted:
                continue
            table = table.append_column(
                name, _broadcast_partition_value(name, value, num_rows, self._schema)
            )
        return table

    def _append_synthesized_columns(
        self,
        table: pa.Table,
        unit: ReadUnit,
        rows_before: int,
        wanted: Optional[Set[str]],
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


@DeveloperAPI
class FixedRowsPerFileMetadata(SupportsMetadata):
    """``SupportsMetadata`` for a format whose every file holds the same number
    of rows, so ``count()`` is answered from the listing without opening a file.

    Mix it into a :class:`FileStreamReader` and have the scanner's
    ``metadata_row_count_is_exact`` return ``True`` whenever no pushdown on it
    reduces rows.
    """

    rows_per_file: int = 1

    @override
    def read_metadata(self, file_manifest: FileManifest) -> Iterator[BlockMetadata]:
        for _ in range(len(file_manifest)):
            yield BlockMetadata(
                num_rows=self.rows_per_file,
                size_bytes=None,
                exec_stats=None,
                input_files=None,
            )

    @override
    def available_metadata(self) -> Set[MetadataType]:
        return {MetadataType.NUM_ROWS}

    @override
    def get_target_metadata_batch_size(self) -> Optional[int]:
        return None


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
