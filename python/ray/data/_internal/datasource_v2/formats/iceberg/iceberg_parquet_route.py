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

A file then takes it when it is Parquet, has no delete files and lives on the
filesystem of the table's location. Every other file is listed and read
through ``ArrowScan`` as before. Footer pruning and the Parquet reader's filter
use only the predicate parts PyArrow's statistics handle
(``iceberg_filter_pushdown``); Ray applies the rest after the read on both
routes.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, AbstractSet, Iterator, List, Optional, Sequence

import pyarrow as pa

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
    SCAN_TASK_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    FileChunk,
    FileManifest,
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
        from pyiceberg.manifest import FileFormat

        if task.file.file_format != FileFormat.PARQUET or task.delete_files:
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
        for task in tasks:
            path = self.file_path(task)
            assert path is not None, task.file.file_path
            file_infos.append(FileInfo(path=path, size=task.file.file_size_in_bytes))
        for manifest in indexer.list_files_from_file_infos(
            file_infos,
            filesystem=self._filesystem,
            predicate=predicate,
            limit=limit,
            projected_columns=projected_columns,
            excluded_read_unit_ids=excluded_read_unit_ids,
        ):
            yield _as_routed_rows(manifest, exact_counts=exact_counts)


def is_routed_row(chunk_metadata: Optional[dict]) -> bool:
    """Whether a manifest row is a row-group run ``list_files`` listed."""
    # ``ArrowScan`` rows are whole files, with no unit ids.
    return chunk_metadata is not None and len(chunk_metadata["unit_ids"]) > 0


def read_routed_rows(
    manifest: FileManifest,
    *,
    io: "FileIO",
    table_location: str,
    columns: List[str],
    predicate: Optional["Expr"],
    limit: Optional[int],
) -> Iterator[pa.RecordBatch]:
    """Read routed rows with Ray's Parquet reader.

    Args:
        manifest: Rows ``is_routed_row`` accepts.
        io: The table's ``FileIO``, a ``PyArrowFileIO``.
        table_location: The table's location, whose filesystem holds the rows.
        columns: The columns to read.
        predicate: The filter ``list_files`` pruned with.
        limit: The most rows to read.

    Yields:
        pa.RecordBatch: The rows' batches.
    """
    from pyiceberg.io.pyarrow import PyArrowFileIO

    from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (  # noqa: E501
        ParquetFileReader,
    )

    assert isinstance(io, PyArrowFileIO), type(io)
    scheme, netloc, _ = PyArrowFileIO.parse_location(table_location)
    reader = ParquetFileReader(
        columns=columns,
        predicate=predicate,
        limit=limit,
        filesystem=io.fs_by_scheme(scheme, netloc),
    )
    for table in reader.read(manifest):
        yield from table.to_batches()


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


def _as_routed_rows(manifest: FileManifest, *, exact_counts: bool) -> FileManifest:
    """``manifest`` with an empty scan-task column, as Iceberg's rows carry one."""
    chunks = [FileChunk.from_metadata(chunk) for chunk in manifest.file_chunk_metadatas]
    if not exact_counts:
        chunks = [replace(chunk, fully_matched=False) for chunk in chunks]
    return FileManifest.construct_manifest(
        paths=[str(path) for path in manifest.paths],
        sizes=[int(size) for size in manifest.file_sizes],
        chunk_metadatas=[chunk.to_metadata() for chunk in chunks],
        extra_columns=pa.table(
            {SCAN_TASK_COLUMN_NAME: pa.nulls(len(chunks), type=pa.large_binary())}
        ),
    )
