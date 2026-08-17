"""Concrete ``DataSourceV2`` for ORC files."""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Literal, Optional, Union

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource.parquet_datasource import (
    check_for_legacy_tensor_type,
)
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    FileChunker,
    OrcFileChunker,
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
from ray.data._internal.datasource_v2.readers.file_reader import (
    INCLUDE_PATHS_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    OrcInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.scanners.orc_scanner import OrcScanner
from ray.data._internal.util import _is_local_scheme
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


@DeveloperAPI
class OrcDatasourceV2(DataSourceV2[FileManifest]):
    """V2 ORC datasource with worker-side stripe chunking."""

    _FILE_EXTENSIONS = ["orc"]

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
        file_chunker: Optional[FileChunker] = None,
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
        self._include_paths = include_paths
        self._shuffle = shuffle
        self._file_chunker = (
            file_chunker if file_chunker is not None else OrcFileChunker()
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
            file_chunker=self._file_chunker,
        )

    def get_size_estimator(self) -> OrcInMemorySizeEstimator:
        return OrcInMemorySizeEstimator()

    @override
    def resolve_partitioning(self, sample: FileManifest) -> Optional[Partitioning]:
        import copy

        if self._partitioning is None or len(sample) == 0:
            return copy.deepcopy(self._partitioning)
        if self._partitioning.field_names:
            return copy.deepcopy(self._partitioning)

        first_path = sample.paths.tolist()[0]
        parser = PathPartitionParser(self._partitioning)
        partition_kv = parser(first_path)
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
        from concurrent.futures import ThreadPoolExecutor

        import pyarrow.orc as orc

        from ray.data._internal.util import unify_schemas_with_validation

        if len(sample) == 0:
            return pa.schema([])

        sample_paths: List[str] = sample.paths.tolist()
        filesystem = self._filesystem

        def _read_schema(path: str) -> pa.Schema:
            if filesystem is None:
                return orc.ORCFile(path).schema
            with filesystem.open_input_file(path) as handle:
                return orc.ORCFile(handle).schema

        with ThreadPoolExecutor(max_workers=min(len(sample_paths), 16)) as executor:
            per_file_schemas = list(executor.map(_read_schema, sample_paths))
        schema = unify_schemas_with_validation(per_file_schemas) or per_file_schemas[0]
        assert isinstance(schema, pa.Schema)

        resolved_partitioning = self.resolve_partitioning(sample)
        if resolved_partitioning is not None:
            first_path = sample_paths[0]
            parser = PathPartitionParser(resolved_partitioning)
            partition_kv = parser(first_path)
            partition_pa_schema = _partition_field_types_to_pa_schema(
                field_names=list(partition_kv.keys()),
                field_types=resolved_partitioning.field_types or {},
            )
            for field_name in partition_kv.keys():
                if schema.get_field_index(field_name) == -1:
                    schema = schema.append(
                        pa.field(field_name, partition_pa_schema.field(field_name).type)
                    )

        if (
            self._include_paths
            and schema.get_field_index(INCLUDE_PATHS_COLUMN_NAME) == -1
        ):
            schema = schema.append(pa.field(INCLUDE_PATHS_COLUMN_NAME, pa.string()))

        check_for_legacy_tensor_type(schema)
        return schema

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options,
    ) -> OrcScanner:
        partitioning = options.get("partitioning", self._partitioning)
        return OrcScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            partitioning=partitioning,
            include_paths=self._include_paths,
            shuffle=self._shuffle,
            ignore_prefixes=options.get("ignore_prefixes"),
        )
