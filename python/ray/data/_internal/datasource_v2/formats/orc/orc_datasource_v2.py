"""Concrete ``DataSourceV2`` for ORC files."""

from __future__ import annotations

import copy
import logging
import pickle
from threading import Lock
from typing import TYPE_CHECKING, Callable, Dict, List, Literal, Optional, Tuple, Union

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.file_reader import FileFormat
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
    SamplingInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.formats.orc.orc_metadata import (
    OrcLayoutSeed,
    pyorc_available,
    read_orc_schema_and_layout,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.orc.orc_stripe_indexer import (
    OrcStripeIndexer,
    OrcStripePartitioner,
)
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
from ray.data._internal.util import _is_local_scheme
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


_MAX_SEED_BYTES = 16 * 1024 * 1024
logger = logging.getLogger(__name__)


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
        cls = OrcStripeIndexer if pyorc_available() else NonSamplingFileIndexer
        return cls(ignore_missing_paths=self._ignore_missing_paths)

    @override
    def get_file_partitioner(
        self, *, hints: Optional[PartitionHints] = None
    ) -> Optional[FilePartitioner]:
        if hints is None:
            return None
        if pyorc_available():
            return OrcStripePartitioner(hints)
        reader = OrcFileReader(format=FileFormat.ORC, filesystem=self._filesystem)
        return RoundRobinPartitioner(SamplingInMemorySizeEstimator(reader), hints=hints)

    @property
    @override
    def schema_needs_file_sample(self) -> bool:
        return True

    @override
    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        return self._infer_schema(sample)

    @override
    def prepare_read(
        self,
        sample: Optional[FileManifest],
        indexer: FileIndexer,
        hints: PartitionHints,
    ) -> Tuple[pa.Schema, FileIndexer, Optional[FilePartitioner]]:
        if not isinstance(indexer, OrcStripeIndexer):
            return super().prepare_read(sample, indexer, hints)
        assert sample is not None
        # Full-schema mode may pass every file. Only the bounded bootstrap
        # subset prepares layouts on the driver; the rest remain worker IO.
        layout_paths = tuple(dict.fromkeys(sample.paths[:16].tolist()))
        layout_path_set = frozenset(layout_paths)
        layouts: Dict[str, OrcLayoutSeed] = {}
        seed_lock = Lock()
        retained = 0

        def read_schema(path: str) -> pa.Schema:
            nonlocal retained
            if path not in layout_path_set:
                return self._read_schema(path)
            schema, seed = read_orc_schema_and_layout(path, self._filesystem)
            size = len(pickle.dumps(seed, protocol=5))
            with seed_lock:
                if path not in layouts:
                    if retained + size <= _MAX_SEED_BYTES:
                        layouts[path] = seed
                        retained += size
                    else:
                        logger.debug(
                            "ORC bootstrap layout not retained for %s: seed budget exceeded",
                            path,
                        )
            return schema

        schema = self._infer_schema(sample, read_schema)
        seeds = [layouts[path] for path in layout_paths if path in layouts]
        bootstrap_bytes = None
        if len(sample) < 16 and len(seeds) == len(layout_paths):
            bootstrap_bytes = sum(
                seed.size
                if len(seed.layout.stripes) <= 1
                else sum(s.size_bytes for s in seed.layout.stripes)
                for seed in seeds
            )
        prepared_indexer = OrcStripeIndexer(
            ignore_missing_paths=self._ignore_missing_paths,
            seeds=seeds,
            local_only=not self._supports_distributed_reads,
        )
        return (
            schema,
            prepared_indexer,
            OrcStripePartitioner(hints, bootstrap_bytes=bootstrap_bytes),
        )

    def _infer_schema(
        self,
        sample: Optional[FileManifest],
        read_schema: Optional[Callable[[str], pa.Schema]] = None,
    ) -> pa.Schema:
        assert sample is not None
        if len(sample) == 0:
            return pa.schema([])
        sample_paths = sample.paths.tolist()

        schema = infer_schema_from_files(
            sample_paths, read_schema or self._read_schema, format_name="ORC"
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

    def _read_schema(self, path: str) -> pa.Schema:
        import pyarrow.orc as orc

        if self._filesystem is None:
            return orc.ORCFile(path).schema
        with self._filesystem.open_input_file(path) as source:
            return orc.ORCFile(source).schema

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
            target_block_size=DataContext.get_current().target_max_block_size,
        )
