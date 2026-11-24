"""Lists and reads an Iceberg table's plain Parquet files with Ray's Parquet code.

``ArrowScan`` reads a whole file and holds its decoded batches until the file
is done, and none of Ray's Parquet work applies to it. A file that needs
nothing from PyIceberg goes through Ray's Parquet path instead:

    plan_files() -> routed FileScanTask -> FooterFileIndexer (footer read,
        row groups the filter rules out dropped, one row per row-group run)
        -> OnlineBinPacker -> ParquetFileReader (streams the kept row groups)

The route is decided from table metadata, never by opening a file. A read takes
it when:

- the table's ``FileIO`` is PyIceberg's ``PyArrowFileIO``, whose filesystem
  carries the catalog's credentials;
- ``read_iceberg`` got no ``row_filter`` (it has no Ray form), the read is
  case-sensitive and it reads at least one column;
- the table has no name mapping, which files added without field IDs need;
- every column the read or its filter uses is a top-level primitive column
  with the same field ID, name and type in every schema the table has had,
  so every data file stores it under that name and type.

A file then takes it when it is Parquet, lives on the filesystem of the table's
location and has no delete files but position deletes. Every other file, such
as one with equality deletes, is listed and read through ``ArrowScan`` as
before. Footer pruning and the Parquet reader's filter use only the predicate
parts PyArrow's statistics handle (``iceberg_filter_pushdown``); Ray applies
the rest after the read on both routes.

A routed file with position deletes keeps its scan task on its rows, and its
row counts are not exact. Its row groups are read unfiltered with a file
position column, so the rows its delete files name can be dropped by
position; the filter runs after that, as ``ArrowScan`` does with deletes.
Delete files are decoded by PyIceberg's ``_read_deletes``, so Parquet, ORC and
Puffin ones all work; a PyIceberg without it routes no file with deletes.
"""

from __future__ import annotations

from dataclasses import replace
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
)

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ray._common.utils import env_integer
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
    SCAN_TASK_COLUMN_NAME,
    pickle_scan_task,
    scan_tasks_by_row,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
)
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    ReadUnitPosition,
    SynthesizedColumn,
)

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem
    from pyiceberg.expressions import BooleanExpression
    from pyiceberg.io import FileIO
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import TableMetadata

    from ray.data.expressions import Expr


class IcebergParquetRoute:
    """Which of a read's files Ray's Parquet code lists and reads, and how."""

    def __init__(self, *, filesystem: "FileSystem", scheme: str, netloc: str):
        self._filesystem = filesystem
        self._scheme = scheme
        self._netloc = netloc
        self._reads_position_deletes = _read_deletes_function() is not None

    @classmethod
    def create(
        cls,
        *,
        table_metadata: "TableMetadata",
        io: "FileIO",
        row_filter: "BooleanExpression",
        case_sensitive: bool,
        columns: Sequence[str],
    ) -> Optional["IcebergParquetRoute"]:
        """The route for one read, or ``None`` when no file can take it.

        Args:
            table_metadata: The table's metadata.
            io: The table's ``FileIO``.
            row_filter: ``read_iceberg``'s ``row_filter``.
            case_sensitive: Whether column names resolve case-sensitively.
            columns: Every column the read or its filter uses.

        Returns:
            The route, or ``None``.
        """
        from pyiceberg.expressions import AlwaysTrue
        from pyiceberg.io.pyarrow import PyArrowFileIO

        if (
            not isinstance(io, PyArrowFileIO)
            or not isinstance(row_filter, AlwaysTrue)
            or not case_sensitive
            or not columns
            or table_metadata.name_mapping() is not None
        ):
            return None
        if not all(_same_in_every_schema(table_metadata, name) for name in columns):
            return None
        scheme, netloc, _ = PyArrowFileIO.parse_location(table_metadata.location)
        return cls(
            filesystem=io.fs_by_scheme(scheme, netloc), scheme=scheme, netloc=netloc
        )

    def file_path(self, task: "FileScanTask") -> Optional[str]:
        """``task``'s path on the route's filesystem, or ``None`` if it can't go."""
        from pyiceberg.io.pyarrow import PyArrowFileIO
        from pyiceberg.manifest import DataFileContent, FileFormat

        if task.file.file_format != FileFormat.PARQUET:
            return None
        if task.delete_files and (
            not self._reads_position_deletes
            or any(
                delete_file.content != DataFileContent.POSITION_DELETES
                for delete_file in task.delete_files
            )
        ):
            return None
        scheme, netloc, path = PyArrowFileIO.parse_location(task.file.file_path)
        if (scheme, netloc) != (self._scheme, self._netloc):
            return None
        return path

    def list_files(
        self,
        tasks: Sequence["FileScanTask"],
        *,
        predicate: Optional["Expr"],
        projected_columns: Optional[List[str]],
        limit: Optional[int],
        excluded_read_unit_ids: Optional[AbstractSet[str]],
        exact_counts: bool,
    ) -> Iterator[FileManifest]:
        """List ``tasks``' row-group runs with Parquet's footer indexer.

        Args:
            tasks: Scan tasks ``file_path`` accepted.
            predicate: The filter to prune row groups with, ``None`` for none.
            projected_columns: The columns read, which size each run.
            limit: Stop once runs every row of which passes hold this many rows.
            excluded_read_unit_ids: Row groups not to list.
            exact_counts: ``False`` when Ray filters further after the read, so
                no run's row count is exact.

        Yields:
            FileManifest: One manifest per file, one row per row-group run.
        """
        from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (  # noqa: E501
            FooterFileIndexer,
        )

        indexer = FooterFileIndexer(
            ignore_missing_paths=False,
            # Same default as ``ParquetDatasourceV2``.
            coalesce_bytes=env_integer("RAY_DATA_PARQUET_FOOTER_COALESCE_BYTES", 0),
        )
        file_infos = []
        tasks_with_deletes: Dict[str, "FileScanTask"] = {}
        for task in tasks:
            path = self.file_path(task)
            assert path is not None, task.file.file_path
            file_infos.append(FileInfo(path=path, size=task.file.file_size_in_bytes))
            if task.delete_files:
                tasks_with_deletes[path] = task
        if tasks_with_deletes:
            # The footer indexer would count rows the deletes remove.
            limit = None
        for manifest in indexer.list_files_from_file_infos(
            file_infos,
            filesystem=self._filesystem,
            predicate=predicate,
            limit=limit,
            projected_columns=projected_columns,
            excluded_read_unit_ids=excluded_read_unit_ids,
        ):
            yield _as_routed_rows(
                manifest,
                exact_counts=exact_counts,
                tasks_with_deletes=tasks_with_deletes,
            )


def is_routed_row(chunk_metadata: Optional[dict]) -> bool:
    """Whether a manifest row is a row-group run ``list_files`` listed."""
    # ``ArrowScan`` rows are whole files, with no unit ids.
    return chunk_metadata is not None and len(chunk_metadata["unit_ids"]) > 0


def read_routed_rows(
    manifest: FileManifest,
    *,
    io: "FileIO",
    delete_io: "FileIO",
    table_location: str,
    columns: List[str],
    predicate: Optional["Expr"],
    limit: Optional[int],
) -> Iterator[pa.RecordBatch]:
    """Read routed rows with Ray's Parquet reader.

    Args:
        manifest: Rows ``is_routed_row`` accepts.
        io: The table's ``FileIO``, a ``PyArrowFileIO``.
        delete_io: The ``FileIO`` to read delete files through.
        table_location: The table's location, whose filesystem holds the rows.
        columns: The columns to read.
        predicate: The filter ``list_files`` pruned with.
        limit: The most rows to read.

    Yields:
        pa.RecordBatch: The rows' batches.
    """
    from pyiceberg.io.pyarrow import PyArrowFileIO

    assert isinstance(io, PyArrowFileIO), type(io)
    scheme, netloc, _ = PyArrowFileIO.parse_location(table_location)
    filesystem = io.fs_by_scheme(scheme, netloc)
    rows_read = 0
    for rows, task in _rows_by_task(manifest):
        remaining = None if limit is None else limit - rows_read
        if remaining is not None and remaining <= 0:
            return
        if task is None:
            tables = _parquet_reader(
                filesystem, columns=columns, predicate=predicate, limit=remaining
            ).read(rows)
        else:
            tables = _read_with_position_deletes(
                rows,
                task,
                filesystem=filesystem,
                delete_io=delete_io,
                columns=columns,
                predicate=predicate,
                limit=remaining,
            )
        for table in tables:
            rows_read += table.num_rows
            yield from table.to_batches()


def _parquet_reader(filesystem: "FileSystem", **kwargs):
    from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (  # noqa: E501
        ParquetFileReader,
    )

    return ParquetFileReader(filesystem=filesystem, **kwargs)


def _rows_by_task(
    manifest: FileManifest,
) -> Iterator[Tuple[FileManifest, Optional["FileScanTask"]]]:
    """Runs of rows without a scan task, and each row with one, in order."""
    block = manifest.as_block()
    assert isinstance(block, pa.Table)
    start = 0
    for index, task in enumerate(scan_tasks_by_row(manifest)):
        if task is None:
            continue
        if start < index:
            yield FileManifest(block.slice(start, index - start)), None
        yield FileManifest(block.slice(index, 1)), task
        start = index + 1
    if start < block.num_rows:
        yield FileManifest(block.slice(start)), None


class _FilePosition(SynthesizedColumn):
    """Each row's position in its file, which position deletes name."""

    name = "__ib_file_position"
    type = pa.int64()
    requires_read_unit_boundaries = True

    def compute(self, position: ReadUnitPosition, num_rows: int) -> pa.Array:
        # Read unfiltered, so ``rows_before`` counts every row of the unit.
        start = position.unit_start_row + position.rows_before
        return pa.array(np.arange(start, start + num_rows, dtype=np.int64))


def _read_with_position_deletes(
    row: FileManifest,
    task: "FileScanTask",
    *,
    filesystem: "FileSystem",
    delete_io: "FileIO",
    columns: List[str],
    predicate: Optional["Expr"],
    limit: Optional[int],
) -> Iterator[pa.Table]:
    deleted = _deleted_positions(task, delete_io)
    reader = _parquet_reader(
        filesystem,
        columns=columns + [_FilePosition.name],
        predicate=None,
        synthesized_columns=[_FilePosition()],
    )
    rows_read = 0
    for table in reader.read(row):
        # pyrefly: ignore[missing-attribute]  # pc functions are runtime-generated.
        deleted_rows = pc.is_in(table[_FilePosition.name], value_set=deleted)
        # pyrefly: ignore[missing-attribute]  # pc functions are runtime-generated.
        table = table.filter(pc.invert(deleted_rows)).drop([_FilePosition.name])
        if predicate is not None:
            table = ArrowBlockAccessor(table).filter(predicate)
        if limit is not None:
            table = table.slice(0, limit - rows_read)
        rows_read += table.num_rows
        yield table
        if limit is not None and rows_read >= limit:
            return


def _deleted_positions(task: "FileScanTask", io: "FileIO") -> pa.Array:
    """The positions ``task``'s delete files remove from its data file."""
    read_deletes = _read_deletes_function()
    assert read_deletes is not None
    chunks = []
    for delete_file in task.delete_files:
        positions = read_deletes(io, delete_file).get(task.file.file_path)
        if positions is not None:
            chunks.extend(positions.cast(pa.int64()).chunks)
    if not chunks:
        return pa.array([], type=pa.int64())
    return pa.concat_arrays(chunks)


def _read_deletes_function() -> Optional[Callable]:
    """PyIceberg's position delete decoder, private to it, if it has one."""
    try:
        from pyiceberg.io.pyarrow import _read_deletes
    except ImportError:
        return None
    return _read_deletes


def _same_in_every_schema(table_metadata: "TableMetadata", name: str) -> bool:
    current = _top_level_field(table_metadata.schema(), name)
    if current is None or not current.field_type.is_primitive:
        return False
    for schema in table_metadata.schemas:
        field = _top_level_field(schema, name)
        if (
            field is None
            or field.field_id != current.field_id
            or field.field_type != current.field_type
        ):
            return False
    return True


def _top_level_field(schema, name: str):
    return next((field for field in schema.columns if field.name == name), None)


def _as_routed_rows(
    manifest: FileManifest,
    *,
    exact_counts: bool,
    tasks_with_deletes: Dict[str, "FileScanTask"],
) -> FileManifest:
    """``manifest`` with Iceberg's scan-task column, empty for a file without
    deletes."""
    paths = [str(path) for path in manifest.paths]
    tasks = [tasks_with_deletes.get(path) for path in paths]
    chunks = [
        FileChunk.from_metadata(chunk)
        if exact_counts and task is None
        else replace(FileChunk.from_metadata(chunk), fully_matched=False)
        for chunk, task in zip(manifest.file_chunk_metadatas, tasks)
    ]
    return FileManifest.construct_manifest(
        paths=paths,
        sizes=[int(size) for size in manifest.file_sizes],
        chunk_metadatas=[chunk.to_metadata() for chunk in chunks],
        extra_columns=pa.table(
            {
                SCAN_TASK_COLUMN_NAME: pa.array(
                    [
                        None if task is None else pickle_scan_task(task)
                        for task in tasks
                    ],
                    type=pa.large_binary(),
                )
            }
        ),
    )
