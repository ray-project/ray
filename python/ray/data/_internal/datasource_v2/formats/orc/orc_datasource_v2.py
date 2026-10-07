"""Concrete ``DataSourceV2`` for ORC files."""

from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, List, Literal, Optional, Union

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileFormat
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.datasource_v2.common.size_estimators import (
    SamplingInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
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
from ray.data._internal.util import _is_local_scheme, unify_schemas_with_validation
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
class OrcDatasourceV2(FileDataSourceV2):
    """V2 datasource that lists ORC files and reads them with ``OrcScanner``."""

    def __init__(
        self,
        paths: List[str],
        *,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        file_extensions: Optional[List[str]] = None,
        ignore_missing_paths: bool = False,
        include_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], FileShuffleConfig]] = None,
    ):
        super().__init__(name="OrcV2", category=DatasourceCategory.FILE_BASED)
        self._supports_distributed_reads = not _is_local_scheme(paths)
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            paths, filesystem
        )
        self._paths = resolved_paths
        self._filesystem = resolved_filesystem
        self._partitioning = partitioning
        self._file_extensions = file_extensions
        self._ignore_missing_paths = ignore_missing_paths
        self._shuffle = shuffle
        self._synthesized_columns = (PathColumn(),) if include_paths else ()

    @property
    @override
    def paths(self) -> List[str]:
        return self._paths

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

    @override
    def _get_file_indexer(self) -> FileIndexer:
        return NonSamplingFileIndexer(ignore_missing_paths=self._ignore_missing_paths)

    @override
    def get_file_partitioner(
        self, *, hints: Optional[PartitionHints] = None
    ) -> Optional[FilePartitioner]:
        if hints is None:
            return None
        reader = OrcFileReader(format=FileFormat.ORC, filesystem=self._filesystem)
        return RoundRobinPartitioner(SamplingInMemorySizeEstimator(reader), hints=hints)

    @property
    @override
    def schema_needs_file_sample(self) -> bool:
        return True

    @override
    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        import pyarrow.orc as orc

        assert sample is not None
        sample_paths = sample.paths.tolist()
        filesystem = self._filesystem

        def _read_schema(path: str) -> pa.Schema:
            if filesystem is None:
                return orc.ORCFile(path).schema
            with filesystem.open_input_file(path) as source:
                return orc.ORCFile(source).schema

        with ThreadPoolExecutor(max_workers=min(len(sample_paths), 16)) as executor:
            schemas = list(executor.map(_read_schema, sample_paths))
        schema = unify_schemas_with_validation(schemas) or schemas[0]
        assert isinstance(schema, pa.Schema)

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

    @override
    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options,
    ) -> OrcScanner:
        return OrcScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
        )
