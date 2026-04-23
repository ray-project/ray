"""Concrete ``DataSourceV2`` for Parquet files.

Wires the V2 listing (`NonSamplingFileIndexer`, driven by the upstream
`ListFiles` op), scanning (`ParquetScanner`), and reading
(`ParquetFileReader`) components against a user-supplied path set.
Constructed from `read_api.read_parquet` when
`DataContext.use_datasource_v2` is set.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource.parquet_datasource import (
    ParquetDatasource,
    check_for_legacy_tensor_type,
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
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    ParquetInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.scanners.parquet_scanner import ParquetScanner
from ray.data.context import DataContext
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
class ParquetDatasourceV2(DataSourceV2[FileManifest]):
    """V2 Parquet datasource.

    Listing is delegated to :class:`NonSamplingFileIndexer` driven by the
    ``ListFiles`` logical op; scanning and reading are delegated to
    :class:`ParquetScanner` and :class:`ParquetFileReader`. Schema
    inference reads the first file's footer only and augments with
    partition/path columns as configured.
    """

    def __init__(
        self,
        paths: List[str],
        *,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = Partitioning("hive"),
        file_extensions: Optional[List[str]] = None,
        ignore_missing_paths: bool = False,
        include_paths: bool = False,
        shuffle: Optional[str] = None,
        arrow_parquet_args: Optional[dict] = None,
    ):
        super().__init__(name="ParquetV2", category=DatasourceCategory.FILE_BASED)
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            paths, filesystem
        )
        self._paths: List[str] = resolved_paths
        self._filesystem = resolved_filesystem
        self._partitioning = partitioning
        self._file_extensions = file_extensions or ParquetDatasource._FILE_EXTENSIONS
        self._ignore_missing_paths = ignore_missing_paths
        self._include_paths = include_paths
        self._shuffle = shuffle
        self._arrow_parquet_args = arrow_parquet_args or {}

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
    def file_extensions(self) -> List[str]:
        return self._file_extensions

    @property
    def ignore_missing_paths(self) -> bool:
        return self._ignore_missing_paths

    @property
    def include_paths(self) -> bool:
        return self._include_paths

    @property
    def shuffle(self) -> Optional[str]:
        return self._shuffle

    def _get_file_indexer(self) -> FileIndexer:
        return NonSamplingFileIndexer(
            ignore_missing_paths=self._ignore_missing_paths,
        )

    def get_size_estimator(self) -> ParquetInMemorySizeEstimator:
        return ParquetInMemorySizeEstimator()

    @override
    def resolve_partitioning(self, sample: FileManifest) -> Optional[Partitioning]:
        """Return ``self._partitioning`` with path-discovered field names.

        Hive partitioning ships with ``field_names=None`` by default and
        discovers keys from the file path at plan time. Directory
        partitioning already carries ``field_names`` at construction and
        needs no discovery. Returns a fresh ``Partitioning`` rather than
        mutating ``self`` so schema inference stays side-effect-free.
        """
        if self._partitioning is None or len(sample) == 0:
            return self._partitioning
        if self._partitioning.field_names:
            return self._partitioning

        first_path = sample.paths.tolist()[0]
        parser = PathPartitionParser(self._partitioning)
        partition_kv = parser(first_path)
        if not partition_kv:
            return self._partitioning
        return Partitioning(
            style=self._partitioning.style,
            base_dir=self._partitioning.base_dir,
            field_names=list(partition_kv.keys()),
            field_types=self._partitioning.field_types,
            filesystem=self._partitioning.filesystem,
        )

    def infer_schema(self, sample: FileManifest) -> pa.Schema:
        """Read Parquet footers from the sample manifest; unify and augment.

        When the sample has multiple files, their schemas are unified via
        ``unify_schemas_with_validation`` so a first file with all-null
        columns doesn't lock in ``null`` types that can't be cast to the
        actual types in later files.

        Pure: does not mutate ``self``. Partitioning field-name discovery
        is delegated to :meth:`resolve_partitioning` so the discovered
        ``Partitioning`` can flow through ``_read_datasource_v2`` into
        :meth:`create_scanner` without side effects.
        """
        from concurrent.futures import ThreadPoolExecutor

        import pyarrow.parquet as pq

        from ray.data._internal.util import unify_schemas_with_validation

        if len(sample) == 0:
            raise ValueError(
                "ParquetDatasourceV2.infer_schema received an empty FileManifest"
            )

        sample_paths = sample.paths.tolist()
        # Parquet footer reads against high-latency object stores
        # (S3, GCS) are ~50-100 ms each. Reading the sample's footers in
        # parallel keeps driver-side schema inference bounded by the
        # slowest single read rather than the sum. Order is preserved
        # because ``unify_schemas_with_validation`` treats the first
        # schema as the type-promotion base.
        filesystem = self._filesystem
        with ThreadPoolExecutor(max_workers=min(len(sample_paths), 16)) as executor:
            per_file_schemas = list(
                executor.map(
                    lambda p: pq.read_schema(p, filesystem=filesystem),
                    sample_paths,
                )
            )
        schema = unify_schemas_with_validation(per_file_schemas) or per_file_schemas[0]

        resolved_partitioning = self.resolve_partitioning(sample)
        if resolved_partitioning is not None:
            first_path = sample_paths[0]
            parser = PathPartitionParser(resolved_partitioning)
            partition_kv = parser(first_path)
            # For hive partitioning the parser discovers key names from the
            # path itself; for directory partitioning it uses ``field_names``.
            # In both cases ``partition_kv`` is the authoritative list of
            # partition columns for the first sample file.
            partition_pa_schema = _partition_field_types_to_pa_schema(
                list(partition_kv.keys()), resolved_partitioning.field_types or {}
            )
            for field_name in partition_kv.keys():
                if schema.get_field_index(field_name) == -1:
                    pa_type = partition_pa_schema.field(field_name).type
                    schema = schema.append(pa.field(field_name, pa_type))

        if self._include_paths and schema.get_field_index("path") == -1:
            schema = schema.append(pa.field("path", pa.string()))

        check_for_legacy_tensor_type(schema)
        return schema

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> ParquetScanner:
        # ``filter=`` in V1 read_parquet() is the legacy pyarrow-compute
        # predicate. Stamp it on the scanner's ``predicate`` field so it's
        # honored at scan time (V2 does not yet dispatch Ray-level
        # predicate pushdown rules).
        predicate = self._arrow_parquet_args.get("filter")
        # Callers (``_read_datasource_v2``) supply the sample-resolved
        # ``Partitioning`` via ``options["partitioning"]`` so the
        # datasource itself stays immutable — fall back to the
        # constructor-provided one for direct users of this API.
        partitioning = options.get("partitioning", self._partitioning)
        return ParquetScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            partitioning=partitioning,
            include_paths=self._include_paths,
            shuffle=self._shuffle,
            ignore_prefixes=options.get("ignore_prefixes"),
            target_block_size=DataContext.get_current().target_max_block_size,
            predicate=predicate,
        )
