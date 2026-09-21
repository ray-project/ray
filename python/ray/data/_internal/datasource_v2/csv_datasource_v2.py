"""Concrete ``DataSourceV2`` for CSV files."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import pyarrow as pa
from pyarrow import csv
from typing_extensions import override

from ray.data._internal.datasource_v2.datasource_v2 import (
    DatasourceCategory,
    DataSourceV2,
)
from ray.data._internal.datasource_v2.listing.csv_file_indexer import (
    RecordAlignedCSVFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_indexer import FileIndexer
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


# Record alignment opens every multi-chunk file during listing. Emit manifests
# frequently so downstream reads can start without waiting for the generic
# 1000-row listing batch to accumulate.
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
        # Later chunks need all physical column names to replace the header,
        # but a projected header block only exposes the included ones.
        return False

    if open_stream_args:
        # Chunk reads use ``open_input_file`` for random access, so arguments
        # promised to ``open_input_stream`` (including ``buffer_size``) wouldn't
        # be honored. Keep the file whole whenever the caller customizes stream
        # opening semantics.
        return False

    return True


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
        chunk_byte_size: Optional[int] = None,
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

        # Nominal split size for large uncompressed files. Exposed so tests can
        # exercise multi-chunk reads on tiny inputs; production reads keep the
        # indexer's default.
        self._chunk_byte_size = chunk_byte_size
        self._split_files = _supports_line_delimited_chunking(
            self._read_options,
            self._parse_options,
            self._arrow_csv_args,
            self._open_stream_args,
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

    @property
    def schema_needs_file_sample(self) -> bool:
        # CSV has no metadata; the planning schema comes from file headers.
        return True

    def _get_file_indexer(self) -> FileIndexer:
        return RecordAlignedCSVFileIndexer(
            ignore_missing_paths=self._ignore_missing_paths,
            filesystem=self._filesystem,
            split_files=self._split_files,
            chunk_byte_size=self._chunk_byte_size,
            max_paths_per_output=_MAX_CHUNKS_PER_LIST_FILES_OUTPUT,
        )

    def get_size_estimator(self) -> IdentityInMemorySizeEstimator:
        return IdentityInMemorySizeEstimator()

    @override
    def resolve_partitioning(self, sample: FileManifest) -> Optional[Partitioning]:
        import copy

        # Unlike Parquet, CSV keeps a dynamic output schema so Hive partition
        # keys discovered outside the bounded sample can still be appended at
        # execution time. Pinning ``field_names`` from the first sampled path
        # would make a later, deeper Hive path fail before the reader can retain
        # its additional partition column.
        return copy.deepcopy(self._partitioning)

    def infer_schema(self, sample: FileManifest) -> pa.Schema:
        """Unify the header schemas of the sampled files.

        Pure: the result is only a hint for planning. Chunked reads take the
        column names and types from each file's own header block, so nothing
        here needs to be retained by the datasource, its chunker, or the reader.
        """
        if len(sample) == 0:
            return pa.schema([])

        inspector = self._create_inspector()
        schemas = [inspector.inspect_schema(str(path)) for path in sample.paths]
        schema = unify_schemas_with_validation(schemas) or schemas[0]
        assert isinstance(schema, pa.Schema)

        resolved_partitioning = self.resolve_partitioning(sample)
        if resolved_partitioning is not None:
            partition_parser = PathPartitionParser(resolved_partitioning)
            partition_field_names = []
            for path in sample.paths:
                for field_name in partition_parser(path):
                    if field_name not in partition_field_names:
                        partition_field_names.append(field_name)
            partition_schema = _partition_field_types_to_pa_schema(
                field_names=partition_field_names,
                field_types=resolved_partitioning.field_types or {},
            )
            for field_name in partition_field_names:
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
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            include_paths=self._include_paths,
            shuffle=self._shuffle,
            read_options=self._read_options,
            parse_options=self._parse_options,
            arrow_csv_args=dict(self._arrow_csv_args),
            open_stream_args=dict(self._open_stream_args),
        )

    def _create_inspector(self) -> CSVFileReader:
        return CSVFileReader(
            filesystem=self._filesystem,
            read_options=self._read_options,
            parse_options=self._parse_options,
            arrow_csv_args=self._arrow_csv_args,
            open_stream_args=self._open_stream_args,
        )
