"""Sizes an Iceberg table's Parquet files from their footers, as Parquet V2 does.

Iceberg's manifests record each column's compressed bytes on disk
(``column_sizes``) and nothing about its decoded size or the memory decoding it
takes. Ray's Parquet decoded-size estimator reads both from a file's footer, so
a read's Parquet files are listed through Parquet's ``FooterFileIndexer`` first
and each file's row groups are summed into one whole-file ``FileChunk``:

    plan_files() -> Parquet FileScanTask -> FooterFileIndexer (one footer read
        per file: each row group's decoded bytes and decode peak)
        -> FileChunk(size_bytes=sum, memory_bytes=sum)
        -> OnlineBinPacker (bins of ~128 MiB decoded; each bin's read task
           requests its summed peak as memory)

A read is sized this way when the table's ``FileIO`` is ``PyArrowFileIO``, the
read is case-sensitive and reads at least one column, the table has no name
mapping, and no schema the table has had stored a column the read uses, or a
field nested in it, under another name. A data file then stores each column
under its current name or not at all, which is how the footer reader finds it.
A file is sized this way when it is Parquet and lives on the filesystem of the
table's location. Every other file keeps its size on disk
(``file_size_in_bytes``) and requests no memory.

The estimate is for what ``ArrowScan`` returns: the read schema is the
projected Iceberg schema as Arrow types, which every batch is cast to. Delete
files are not sized.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Sequence

from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileChunk

if TYPE_CHECKING:
    import pyarrow as pa
    from pyarrow.fs import FileSystem
    from pyiceberg.io import FileIO
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import TableMetadata


class DecodedSize(NamedTuple):
    """A file's estimated decoded bytes, and PyArrow's estimated peak decoding it."""

    size_bytes: int
    memory_bytes: Optional[int]


class IcebergFooterSizing:
    """Which of a read's files are sized from their Parquet footers, and how."""

    def __init__(
        self,
        *,
        filesystem: "FileSystem",
        scheme: str,
        netloc: str,
        read_schema: "pa.Schema",
    ):
        self._filesystem = filesystem
        self._scheme = scheme
        self._netloc = netloc
        self._read_schema = read_schema

    @classmethod
    def create(
        cls,
        *,
        table_metadata: "TableMetadata",
        io: "FileIO",
        case_sensitive: bool,
        read_schema: "pa.Schema",
    ) -> Optional["IcebergFooterSizing"]:
        """The sizing for one read, or ``None`` when no file can take it.

        Args:
            table_metadata: The table's metadata.
            io: The table's ``FileIO``.
            case_sensitive: Whether column names resolve case-sensitively.
            read_schema: The Arrow schema of the columns the read returns.

        Returns:
            The sizing, or ``None``.
        """
        from pyiceberg.io.pyarrow import PyArrowFileIO

        if (
            not isinstance(io, PyArrowFileIO)
            or not case_sensitive
            or len(read_schema) == 0
            or table_metadata.name_mapping() is not None
        ):
            return None
        if not all(_names_stable(table_metadata, name) for name in read_schema.names):
            return None
        scheme, netloc, _ = PyArrowFileIO.parse_location(table_metadata.location)
        return cls(
            filesystem=io.fs_by_scheme(scheme, netloc),
            scheme=scheme,
            netloc=netloc,
            read_schema=read_schema,
        )

    def file_path(self, task: "FileScanTask") -> Optional[str]:
        """``task``'s path on the sizing's filesystem, or ``None`` if it can't go."""
        from pyiceberg.io.pyarrow import PyArrowFileIO
        from pyiceberg.manifest import FileFormat

        if task.file.file_format != FileFormat.PARQUET:
            return None
        scheme, netloc, path = PyArrowFileIO.parse_location(task.file.file_path)
        if (scheme, netloc) != (self._scheme, self._netloc):
            return None
        return path

    def size_files(self, tasks: Sequence["FileScanTask"]) -> Dict[str, DecodedSize]:
        """Read ``tasks``' footers and size each file whole.

        Args:
            tasks: Scan tasks ``file_path`` accepted.

        Returns:
            Each file's size, by its Iceberg path. A file with no row groups is
            left out.
        """
        from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (  # noqa: E501
            FooterFileIndexer,
        )

        iceberg_paths: Dict[str, str] = {}
        file_infos: List[FileInfo] = []
        for task in tasks:
            path = self.file_path(task)
            assert path is not None, task.file.file_path
            iceberg_paths[path] = task.file.file_path
            file_infos.append(FileInfo(path=path, size=task.file.file_size_in_bytes))
        indexer = FooterFileIndexer(
            ignore_missing_paths=False, read_schema=self._read_schema
        )
        sizes: Dict[str, DecodedSize] = {}
        for manifest in indexer.list_files_from_file_infos(
            file_infos,
            filesystem=self._filesystem,
            projected_columns=list(self._read_schema.names),
        ):
            # The footer indexer lists one file per manifest.
            runs = [FileChunk.from_metadata(m) for m in manifest.file_chunk_metadatas]
            peaks = [run.memory_bytes for run in runs if run.memory_bytes is not None]
            sizes[iceberg_paths[manifest.paths[0]]] = DecodedSize(
                size_bytes=sum(run.size_bytes for run in runs),
                # Summed, as the footer indexer sums a run of row groups.
                memory_bytes=sum(peaks) if len(peaks) == len(runs) else None,
            )
        return sizes


def _names_stable(table_metadata: "TableMetadata", name: str) -> bool:
    """Whether every schema the table has had stores ``name`` and the fields
    nested in it under their current names, or not at all."""
    from pyiceberg.schema import index_by_id, index_by_name, index_name_by_id

    current = table_metadata.schema()
    field = current.find_field(name)
    current_names = index_name_by_id(current)
    field_ids = {field.field_id, *index_by_id(field.field_type)}
    for schema in table_metadata.schemas:
        names = index_name_by_id(schema)
        ids = index_by_name(schema)
        for field_id in field_ids:
            current_name = current_names[field_id]
            if names.get(field_id, current_name) != current_name:
                return False
            if ids.get(current_name, field_id) != field_id:
                return False
    return True
