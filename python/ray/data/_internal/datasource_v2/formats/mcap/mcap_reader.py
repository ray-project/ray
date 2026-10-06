"""``MCAPReader``, which reads one read task's chunks into message rows.

``MCAPReader.read`` receives a ``FileManifest`` whose rows name files and, per
file, the byte offsets of the chunks the task owns. For each file it seeks to
those chunks, decompresses them, keeps the selected messages and yields Arrow
tables of about ``target_block_size`` bytes. With ``log_time_order``, a file's
chunks are merged by log time as they are read, so memory holds only the
chunks that overlap in time.

A row has the columns of ``message_schema``. Its optional ``row_id`` is a
deterministic name for the message, built from the file path, the chunk's byte
offset and the message's position in the chunk.

This module groups the manifest by file and finishes every table with its
partition and synthesized columns. The other modules read and build the rows:

- ``mcap_chunks`` reads the selected messages of a file.
- ``mcap_message_rows`` builds message rows.
"""

import logging
from functools import partial
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem, LocalFileSystem

from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource_v2.formats.mcap.mcap_chunks import (
    SelectedMessageReader,
    _Selected,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    _MessageTableBuilder,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import MCAPSelection
from ray.data._internal.datasource_v2.formats.mcap.mcap_summary import chunk_unit_id
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.read_units import ReadUnit
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import iterate_with_retry
from ray.data.datasource.partitioning import Partitioning, PathPartitionParser
from ray.util.annotations import DeveloperAPI
from ray.util.debug import log_once

logger = logging.getLogger(__name__)


@DeveloperAPI
class MCAPReader(Reader[FileManifest]):
    """Reads the chunks of MCAP files a manifest assigns to one task.

    Created by ``MCAPScanner.create_reader`` with every pushdown applied:
    the message selection, the projected columns and the per-task row limit.
    """

    def __init__(
        self,
        *,
        selection: MCAPSelection,
        include_metadata: bool = True,
        include_row_id: bool = False,
        log_time_order: bool = True,
        columns: Optional[Sequence[str]] = None,
        limit: Optional[int] = None,
        filesystem: Optional[FileSystem] = None,
        partitioning: Optional[Partitioning] = None,
        synthesized_columns: Sequence[SynthesizedColumn] = (),
        target_block_size: Optional[int] = None,
        schema: Optional[pa.Schema] = None,
        decode_json: bool = False,
    ):
        """Initialize the reader.

        Args:
            selection: Which messages to keep.
            include_metadata: Whether to emit the channel and schema columns.
            include_row_id: Whether to emit ``row_id``.
            log_time_order: Whether each file's messages come out in ascending
                ``log_time`` order rather than in file order.
            columns: Columns to produce, in order; ``None`` for all of them.
            limit: Stop after this many rows per manifest.
            filesystem: Filesystem the paths resolve against; local when ``None``.
            partitioning: Path partitioning whose values become columns.
            synthesized_columns: Columns appended to every table rather than
                read, e.g. ``PathColumn`` for ``include_paths``.
            target_block_size: Estimated bytes per yielded table; ``None`` yields
                one table per file.
            schema: Dataset schema, used to type partition columns.
            decode_json: Whether ``data`` holds decoded JSON values rather than
                the payload bytes.
        """
        self._message_reader = SelectedMessageReader(selection, log_time_order)
        self._decode_json = decode_json
        self._include_metadata = include_metadata
        self._include_row_id = include_row_id
        self._columns = list(columns) if columns is not None else None
        self._limit = limit
        self._filesystem = filesystem
        self._partition_parser = (
            PathPartitionParser(partitioning) if partitioning is not None else None
        )
        self._synthesized_columns = tuple(synthesized_columns)
        self._target_block_size = target_block_size
        self._schema = schema

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        """Read the files and chunks named by ``input_split``.

        All rows that name one file are read together. Files come out in
        manifest order.
        """
        from ray.data.context import DataContext

        if len(input_split) == 0:
            return
        retried_io_errors = DataContext.get_current().retried_io_errors
        remaining = self._limit
        for path, offsets in _owned_chunks(input_split):
            tables = iterate_with_retry(
                partial(self._read_file, path, offsets),
                f"read MCAP file {path}",
                match=retried_io_errors,
            )
            for table in tables:
                if remaining is not None:
                    if remaining <= 0:
                        return
                    if table.num_rows > remaining:
                        table = table.slice(0, remaining)
                    remaining -= table.num_rows
                yield table

    # -- one file ----------------------------------------------------------

    def _read_file(self, path: str, offsets: Optional[Set[int]]) -> Iterator[pa.Table]:
        """Yield the tables of one file, limited to the chunks at ``offsets``.

        ``offsets`` is ``None`` for a whole-file row, which means every chunk. A
        file without a chunk index is scanned linearly.
        """
        from mcap.reader import SeekingReader

        filesystem = self._filesystem or LocalFileSystem()
        with filesystem.open_input_file(path) as f:
            reader = SeekingReader(f)
            summary = reader.get_summary()
            if summary is None or not summary.chunk_indexes:
                messages = self._message_reader.iter_unindexed(f, path)
            else:
                messages = self._message_reader.iter_chunks(f, path, summary, offsets)
            yield from self._tables(messages, path, offsets)

    # -- rows to tables ----------------------------------------------------

    def _tables(
        self, messages: Iterator[_Selected], path: str, offsets: Optional[Set[int]]
    ) -> Iterator[pa.Table]:
        """Build tables of about ``target_block_size`` bytes from the messages."""
        # Every table reports one read unit: the file for a whole-file read, else
        # the task's first chunk. Each chunk has one owner, so a checkpoint that
        # hands the unit back skips only this task's rows. The unit is finished
        # once every table of the file is written.
        if offsets is None:
            unit = ReadUnit(id=path, source=path, count=1)
        else:
            unit = ReadUnit(id=chunk_unit_id(path, min(offsets)), source=path)
        wanted = set(self._columns) if self._columns is not None else None
        builder = _MessageTableBuilder(
            columns=wanted,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            decode_json=self._decode_json,
        )
        rows_before = 0
        for selected in messages:
            builder.add(selected)
            if (
                self._target_block_size is not None
                and builder.estimated_bytes >= self._target_block_size
            ):
                yield self._finish(builder.build(), path, unit, rows_before)
                rows_before += builder.num_rows
                builder.reset()
        if builder.num_rows > 0:
            yield self._finish(builder.build(), path, unit, rows_before)

    def _finish(
        self, table: pa.Table, path: str, unit: ReadUnit, rows_before: int
    ) -> pa.Table:
        """Append partition and synthesized columns, then apply the projection."""
        wanted = set(self._columns) if self._columns is not None else None
        num_rows = table.num_rows
        table = self._append_partition_columns(table, path, wanted)
        table = self._append_synthesized_columns(table, unit, rows_before, wanted)
        if self._columns is not None:
            produced = set(table.column_names)
            table = table.select([c for c in self._columns if c in produced])
            if table.num_columns == 0 and num_rows > 0:
                table = table.append_column(
                    _BATCH_SIZE_PRESERVING_STUB_COL_NAME, pa.nulls(num_rows)
                )
        # A JSON payload Arrow cannot type falls back to Ray's pickled-object
        # extension. Unpickling runs arbitrary code, so such a column is refused
        # unless the user opted in, as in every other datasource.
        raise_on_pickle_object_columns(table)
        return table

    def _append_partition_columns(
        self, table: pa.Table, path: str, wanted: Optional[Set[str]]
    ) -> pa.Table:
        """Append the partition values parsed from ``path``, if wanted."""
        if self._partition_parser is None:
            return table
        num_rows = table.num_rows
        for name, value in self._partition_parser(path).items():
            if wanted is not None and name not in wanted:
                continue
            if name in table.column_names:
                # A partition key that names a message column, such as a
                # ``topic=camera/`` folder, does not replace the messages' own
                # values. Parquet also keeps the file's column.
                if log_once(f"mcap_partition_key_shadowed:{name}"):
                    logger.warning(
                        "The partition key %r in %r names a message column; "
                        "read_mcap keeps the messages' values and ignores the "
                        "one in the path.",
                        name,
                        path,
                    )
                continue
            table = table.append_column(
                name, self._partition_value_array(name, value, num_rows)
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

    def _partition_value_array(self, name: str, value: Any, num_rows: int) -> pa.Array:
        """Broadcast one path-derived partition value, typed by the schema if known."""
        as_str = None if value is None else str(value)
        array = pa.repeat(pa.scalar(as_str, type=pa.string()), num_rows)
        if self._schema is not None:
            idx = self._schema.get_field_index(name)
            if idx != -1 and self._schema.field(idx).type != pa.string():
                array = array.cast(self._schema.field(idx).type)
        return array


def _owned_chunks(manifest: FileManifest) -> List[Tuple[str, Optional[Set[int]]]]:
    """Group a manifest's rows by file: ``(path, chunk offsets or None)``.

    A row with chunk metadata contributes its ``unit_ids``, the chunk byte
    offsets. A row without it means the whole file, which wins over any offsets
    listed for the same path.
    """
    owned: Dict[str, Optional[Set[int]]] = {}
    for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas):
        path = str(path)
        if metadata is None or "unit_ids" not in metadata:
            owned[path] = None
            continue
        offsets = {int(i) for i in metadata["unit_ids"]}
        current = owned.get(path, offsets)
        if current is not None:
            current.update(offsets)
        owned[path] = current
    return list(owned.items())
