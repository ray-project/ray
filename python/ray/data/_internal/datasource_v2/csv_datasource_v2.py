"""Concrete ``DataSourceV2`` for CSV files."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import pyarrow as pa
from pyarrow import csv
from typing_extensions import override

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    FileChunker,
    LineDelimitedFileChunker,
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.datasource_v2 import (
    DatasourceCategory,
    DataSourceV2,
)
from ray.data._internal.datasource_v2.listing.file_indexer import (
    FileIndexer,
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.csv_file_reader import CSVFileReader
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    IdentityInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.scanners.csv_scanner import CSVScanner
from ray.data._internal.util import _is_local_scheme, unify_schemas_with_validation
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionParser,
    _partition_field_types_to_pa_schema,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data.datasource.file_based_datasource import FileShuffleConfig


# Header-aware chunk planning performs a prefix read for each multi-chunk file.
# Emit manifests frequently so downstream reads can start without waiting for
# the generic 1000-row listing batch to accumulate.
_MAX_CHUNKS_PER_LIST_FILES_OUTPUT = 64


def _supports_line_delimited_chunking(
    read_options: csv.ReadOptions,
    parse_options: csv.ParseOptions,
    arrow_csv_args: Dict[str, Any],
    open_stream_args: Dict[str, Any],
) -> bool:
    """Return whether byte-range splitting preserves the configured semantics."""
    if parse_options.newlines_in_values:
        return False
    if read_options.skip_rows or read_options.skip_rows_after_names:
        # Skipping is defined relative to the beginning of each file. Keeping
        # the file whole avoids assigning the eventual header/data start to a
        # chunk that doesn't contain the skipped prefix.
        return False

    encoding = read_options.encoding.lower().replace("_", "-")
    if encoding not in {"utf8", "utf-8", "ascii"}:
        return False

    convert_options = arrow_csv_args.get("convert_options")
    if convert_options is not None and convert_options.include_columns:
        # Later chunks need all physical column names to replace the header.
        return False

    if open_stream_args.get("compression") is not None:
        return False
    if set(open_stream_args) - {"buffer_size", "compression"}:
        # Random-access chunk reads can't safely preserve other stream wrappers.
        return False

    return True


class _SchemaValidatingCSVFileChunker(FileChunker):
    """Validate multi-chunk files once without duplicating schema metadata."""

    def __init__(
        self,
        delegate: FileChunker,
        *,
        filesystem: Optional["FileSystem"],
        read_options: csv.ReadOptions,
        parse_options: csv.ParseOptions,
        arrow_csv_args: Dict[str, Any],
        open_stream_args: Dict[str, Any],
    ):
        self._delegate = delegate
        self._filesystem = filesystem
        self._read_options = read_options
        self._parse_options = parse_options
        self._arrow_csv_args = arrow_csv_args
        self._open_stream_args = open_stream_args
        self._physical_schema: Optional[pa.Schema] = None

    @property
    def requires_file_io(self) -> bool:
        return True

    def set_physical_schema(self, schema: pa.Schema) -> None:
        self._physical_schema = schema

    def generate_chunk_metadatas(self, path: str, file_size: int):
        chunks = iter(self._delegate.generate_chunk_metadatas(path, file_size))
        first_chunk = next(chunks, None)
        if first_chunk is None:
            return
        second_chunk = next(chunks, None)
        if second_chunk is None:
            # A single chunk starts at byte zero and can consume the real file
            # header, so listing doesn't need an extra schema read.
            yield first_chunk
            return

        if self._physical_schema is None:
            raise RuntimeError("CSV physical schema must be inferred before listing")

        inspector = CSVFileReader(
            schema=pa.schema([]),
            filesystem=self._filesystem,
            read_options=self._read_options,
            parse_options=self._parse_options,
            arrow_csv_args=self._arrow_csv_args,
            open_stream_args=self._open_stream_args,
        )
        file_schema = inspector.inspect_schema(path)
        if file_schema.names != self._physical_schema.names:
            # A byte-range chunk has no header. If the physical column order
            # differs from the sampled schema, keep the file whole so the
            # reader can parse the real header and either align missing fields
            # or fail explicitly on unknown fields.
            yield None, file_size
            return

        yield first_chunk
        yield second_chunk
        yield from chunks


@DeveloperAPI
class CSVDatasourceV2(DataSourceV2[FileManifest]):
    """V2 CSV datasource with safe line-delimited file chunking."""

    def __init__(
        self,
        paths: List[str],
        *,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        file_extensions: Optional[List[str]] = None,
        ignore_missing_paths: bool = False,
        include_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
        arrow_csv_args: Optional[Dict[str, Any]] = None,
        open_stream_args: Optional[Dict[str, Any]] = None,
        file_chunker: Optional[FileChunker] = None,
    ):
        super().__init__(name="CSVV2", category=DatasourceCategory.FILE_BASED)
        self._supports_distributed_reads = not _is_local_scheme(paths)
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            paths, filesystem
        )
        self._paths = resolved_paths
        self._filesystem = resolved_filesystem
        self._partitioning = partitioning
        # ``read_csv`` intentionally doesn't filter extensions by default.
        self._file_extensions = file_extensions
        self._ignore_missing_paths = ignore_missing_paths
        self._include_paths = include_paths
        self._shuffle = shuffle
        self._open_stream_args = dict(open_stream_args or {})

        csv_args = dict(arrow_csv_args or {})
        read_options = csv_args.pop("read_options", None)
        self._read_options = (
            csv.ReadOptions(use_threads=False) if read_options is None else read_options
        )
        parse_options = csv_args.pop("parse_options", None)
        self._parse_options = (
            csv.ParseOptions() if parse_options is None else parse_options
        )
        self._arrow_csv_args = csv_args
        self._physical_schema: Optional[pa.Schema] = None

        if file_chunker is not None:
            selected_file_chunker = file_chunker
        elif _supports_line_delimited_chunking(
            self._read_options,
            self._parse_options,
            self._arrow_csv_args,
            self._open_stream_args,
        ):
            selected_file_chunker = LineDelimitedFileChunker()
        else:
            selected_file_chunker = WholeFileChunker()

        if isinstance(selected_file_chunker, WholeFileChunker):
            self._file_chunker = selected_file_chunker
        else:
            self._file_chunker = _SchemaValidatingCSVFileChunker(
                selected_file_chunker,
                filesystem=self._filesystem,
                read_options=self._read_options,
                parse_options=self._parse_options,
                arrow_csv_args=self._arrow_csv_args,
                open_stream_args=self._open_stream_args,
            )

    @property
    def paths(self) -> List[str]:
        return self._paths

    @property
    def filesystem(self) -> Optional["FileSystem"]:
        return self._filesystem

    @property
    def partitioning(self) -> Optional[Partitioning]:
        return self._partitioning

    @property
    def file_extensions(self) -> Optional[List[str]]:
        return self._file_extensions

    @property
    def ignore_missing_paths(self) -> bool:
        return self._ignore_missing_paths

    @property
    def include_paths(self) -> bool:
        return self._include_paths

    @property
    def shuffle(self) -> Optional[Union[Literal["files"], "FileShuffleConfig"]]:
        return self._shuffle

    def _get_file_indexer(self) -> FileIndexer:
        return NonSamplingFileIndexer(
            ignore_missing_paths=self._ignore_missing_paths,
            max_paths_per_output=_MAX_CHUNKS_PER_LIST_FILES_OUTPUT,
            file_chunker=self._file_chunker,
        )

    def get_size_estimator(self) -> IdentityInMemorySizeEstimator:
        return IdentityInMemorySizeEstimator()

    @override
    def resolve_partitioning(self, sample: FileManifest) -> Optional[Partitioning]:
        import copy

        if self._partitioning is None or len(sample) == 0:
            return copy.deepcopy(self._partitioning)
        if self._partitioning.field_names:
            return copy.deepcopy(self._partitioning)

        partition_kv = PathPartitionParser(self._partitioning)(sample.paths[0])
        if not partition_kv:
            return copy.deepcopy(self._partitioning)
        return Partitioning(
            style=self._partitioning.style,
            base_dir=self._partitioning.base_dir,
            field_names=list(partition_kv.keys()),
            field_types=self._partitioning.field_types,
            filesystem=self._partitioning.filesystem,
        )

    def infer_schema(self, sample: FileManifest) -> pa.Schema:
        if len(sample) == 0:
            return pa.schema([])

        inspector = CSVFileReader(
            schema=pa.schema([]),
            filesystem=self._filesystem,
            read_options=self._read_options,
            parse_options=self._parse_options,
            arrow_csv_args=self._arrow_csv_args,
            open_stream_args=self._open_stream_args,
        )
        schemas = [inspector.inspect_schema(str(path)) for path in sample.paths]
        schema = unify_schemas_with_validation(schemas) or schemas[0]
        assert isinstance(schema, pa.Schema)
        self._physical_schema = schema
        if isinstance(self._file_chunker, _SchemaValidatingCSVFileChunker):
            self._file_chunker.set_physical_schema(schema)

        resolved_partitioning = self.resolve_partitioning(sample)
        if resolved_partitioning is not None:
            partition_kv = PathPartitionParser(resolved_partitioning)(sample.paths[0])
            partition_schema = _partition_field_types_to_pa_schema(
                field_names=list(partition_kv.keys()),
                field_types=resolved_partitioning.field_types or {},
            )
            for field_name in partition_kv:
                if schema.get_field_index(field_name) == -1:
                    schema = schema.append(partition_schema.field(field_name))

        if self._include_paths:
            path_field = pa.field("path", pa.string())
            path_index = schema.get_field_index("path")
            schema = (
                schema.append(path_field)
                if path_index == -1
                else schema.set(path_index, path_field)
            )
        return schema

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> CSVScanner:
        return CSVScanner(
            schema=schema,
            physical_schema=(
                self._physical_schema if self._physical_schema is not None else schema
            ),
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            include_paths=self._include_paths,
            shuffle=self._shuffle,
            read_options=self._read_options,
            parse_options=self._parse_options,
            arrow_csv_args=dict(self._arrow_csv_args),
            open_stream_args=dict(self._open_stream_args),
        )
