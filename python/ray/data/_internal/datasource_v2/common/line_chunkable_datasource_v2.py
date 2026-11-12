"""Shared ``FileDataSourceV2`` for newline-delimited formats (CSV and JSONL).

A subclass names the format and supplies three hooks: whether its options
allow splitting files at newlines (``_can_split_files``), how to read one
file's schema (``_read_file_schema``), and ``create_scanner``. The rest is
shared:

- Listing uses ``LineChunkFileIndexer``, which cuts large uncompressed files
  into ``chunk_size`` byte ranges, or ``NonSamplingFileIndexer`` when the
  file can't be split.
- ``RoundRobinPartitioner`` groups rows into read tasks by their bytes on
  disk, as V1 sizes CSV and JSON reads, weighing a chunk by its own bytes so
  a chunk the size of a block fills a task by itself. With
  ``preserve_order`` it fills one task at a time, in listing order.
- The plan-time schema is the unified schema of up to 16 sampled files, plus
  partition and synthesized columns.
"""

from __future__ import annotations

import copy
import dataclasses
from typing import TYPE_CHECKING, List, Literal, Optional, Union

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.line_chunk_file_indexer import (
    LineChunkFileIndexer,
)
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.common.schema_inference import (
    infer_schema_from_files,
)
from ray.data._internal.datasource_v2.common.size_estimators import (
    OnDiskSizeEstimator,
)
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
    FilePartitioner,
    PartitionHints,
)
from ray.data._internal.util import MiB, _is_local_scheme, _normalize_paths_to_strings
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionParser,
    _partition_field_types_to_pa_schema,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


@DeveloperAPI
class LineChunkableDatasourceV2(FileDataSourceV2):
    """Base datasource for CSV and JSONL files read in byte-range chunks."""

    # Names the format in schema-merge errors.
    _FORMAT_NAME: str = ""

    def __init__(
        self,
        paths: List[str],
        *,
        name: str,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        file_extensions: Optional[List[str]] = None,
        ignore_missing_paths: bool = False,
        include_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], FileShuffleConfig]] = None,
    ):
        super().__init__(name=name, category=DatasourceCategory.FILE_BASED)
        self._supports_distributed_reads = not _is_local_scheme(paths)
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            paths, filesystem
        )
        self._paths = resolved_paths
        # The resolved paths above lose their URI scheme; lineage tracking
        # reads the caller's paths through ``source_paths``.
        self._source_paths = _normalize_paths_to_strings(paths)
        self._filesystem = resolved_filesystem
        self._partitioning = partitioning
        self._file_extensions = file_extensions
        self._ignore_missing_paths = ignore_missing_paths
        self._shuffle = shuffle
        self._synthesized_columns = (PathColumn(),) if include_paths else ()
        # Read once here and carried to the indexer and the readers, so both
        # map a chunk index to the same byte range.
        self._chunk_size = env_integer("RAY_DATA_LINE_CHUNK_BYTES", 128 * MiB)

    @property
    @override
    def paths(self) -> List[str]:
        return self._paths

    @property
    @override
    def source_paths(self) -> List[str]:
        """Override when :attr:`paths` is resolved and loses URI schemes."""
        return self._source_paths

    @property
    @override
    def filesystem(self) -> "FileSystem":
        return self._filesystem

    @property
    @override
    def file_extensions(self) -> Optional[List[str]]:
        return self._file_extensions

    @property
    @override
    def shuffle(self) -> Optional[Union[Literal["files"], FileShuffleConfig]]:
        return self._shuffle

    def _can_split_files(self) -> bool:
        """Whether every newline ends a row, so files can be cut at newlines."""
        return True

    def _read_file_schema(self, path: str) -> pa.Schema:
        """Return the schema pyarrow infers for one file."""
        raise NotImplementedError

    @override
    def _get_file_indexer(self) -> FileIndexer:
        if self._can_split_files():
            return LineChunkFileIndexer(
                chunk_size=self._chunk_size,
                ignore_missing_paths=self._ignore_missing_paths,
            )
        return NonSamplingFileIndexer(ignore_missing_paths=self._ignore_missing_paths)

    @override
    def get_file_partitioner(
        self, *, hints: Optional[PartitionHints] = None
    ) -> Optional[FilePartitioner]:
        if hints is None:
            return None
        if DataContext.get_current().execution_options.preserve_order:
            # A single bucket fills in listing order, so read tasks keep each
            # file's chunks in order instead of dealing them round-robin.
            hints = dataclasses.replace(hints, num_buckets=1)
        return RoundRobinPartitioner(OnDiskSizeEstimator(), hints=hints)

    @property
    @override
    def schema_needs_file_sample(self) -> bool:
        return True

    @override
    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        assert sample is not None
        if len(sample) == 0:
            return pa.schema([])
        sample_paths = sample.paths.tolist()
        schema = infer_schema_from_files(
            sample_paths, self._read_file_schema, format_name=self._FORMAT_NAME
        )

        resolved_partitioning = self.resolve_partitioning(sample)
        if resolved_partitioning is not None:
            parser = PathPartitionParser(resolved_partitioning)
            partition_names = list(
                dict.fromkeys(
                    name for path in sample_paths for name in parser(path).keys()
                )
            )
            if partition_names:
                partition_schema = _partition_field_types_to_pa_schema(
                    field_names=partition_names,
                    field_types=resolved_partitioning.field_types or {},
                )
                for name in partition_names:
                    if schema.get_field_index(name) == -1:
                        schema = schema.append(partition_schema.field(name))

        for column in self._synthesized_columns:
            index = schema.get_field_index(column.name)
            field = pa.field(column.name, column.type)
            if index == -1:
                schema = schema.append(field)
            elif schema.field(index).type != column.type:
                schema = schema.set(index, field)

        return schema

    @override
    def resolve_partitioning(
        self, sample: Optional[FileManifest]
    ) -> Optional[Partitioning]:
        if self._partitioning is None or sample is None or len(sample) == 0:
            return copy.deepcopy(self._partitioning)
        if self._partitioning.field_names:
            return copy.deepcopy(self._partitioning)

        parser = PathPartitionParser(self._partitioning)
        partition_kv = parser(sample.paths.tolist()[0])
        if not partition_kv:
            return copy.deepcopy(self._partitioning)
        return Partitioning(
            style=self._partitioning.style,
            base_dir=self._partitioning.base_dir,
            field_names=list(partition_kv.keys()),
            field_types=self._partitioning.field_types,
            filesystem=self._partitioning.filesystem,
        )
