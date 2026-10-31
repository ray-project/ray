"""Concrete ``DataSourceV2`` for ORC files."""

from __future__ import annotations

import copy
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.orc.orc_file_indexer import OrcFileIndexer
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.util import MiB, _is_local_scheme, unify_schemas_with_validation
from ray.data.datasource.file_based_datasource import FileShuffleConfig
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionParser,
    _partition_field_types_to_pa_schema,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem


@DeveloperAPI
class OrcDatasourceV2(FileDataSourceV2):
    """V2 datasource that lists ORC files and reads them with ``OrcScanner``."""

    def __init__(
        self,
        paths: list[str],
        *,
        filesystem: FileSystem | None = None,
        partitioning: Partitioning | None = None,
        file_extensions: list[str] | None = None,
        ignore_missing_paths: bool = False,
        include_paths: bool = False,
        shuffle: Literal["files"] | FileShuffleConfig | None = None,
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
    def paths(self) -> list[str]:
        return self._paths

    @property
    @override
    def filesystem(self) -> FileSystem:
        return self._filesystem

    @property
    @override
    def file_extensions(self) -> list[str] | None:
        return self._file_extensions

    @property
    @override
    def shuffle(self) -> Literal["files"] | FileShuffleConfig | None:
        return self._shuffle

    @override
    def _get_file_indexer(self) -> FileIndexer:
        # ORC V2 listing reads each file's footer to discover how many
        # stripes it contains and emits one manifest row per stripe (or
        # one whole-file row for single-stripe files). This lets the
        # partitioner bin-pack stripes across files so large multi-stripe
        # ORC files can be read by multiple Ray tasks.
        return OrcFileIndexer(ignore_missing_paths=self._ignore_missing_paths)

    @override
    def get_file_partitioner(self, **kwargs):
        # Listing rows carry per-stripe FileChunk stats, so bin-pack them
        # into read units instead of size-estimating whole files. We ignore
        # ``hints`` and size from stripe metadata directly, matching
        # Parquet's ``FooterFileIndexer`` / ``OnlineBinPacker`` pairing.
        from ray.data._internal.datasource_v2.common.online_bin_packer import (
            OnlineBinPacker,
        )

        max_bin_bytes = env_integer("RAY_DATA_ORC_BIN_PACKING_BYTES", 128 * MiB)
        max_shared_open_bins = env_integer(
            "RAY_DATA_ORC_BIN_PACKING_MAX_SHARED_OPEN_BINS", 16
        )
        logger.debug(
            "ORC OnlineBinPacker(max_bin_bytes=%d, max_shared_open_bins=%d)",
            max_bin_bytes,
            max_shared_open_bins,
        )
        return OnlineBinPacker(
            max_bin_bytes=max_bin_bytes,
            max_shared_open_bins=max_shared_open_bins,
        )

    @property
    @override
    def schema_needs_file_sample(self) -> bool:
        return True

    @override
    def infer_schema(self, sample: FileManifest | None) -> pa.Schema:
        from pyarrow import orc

        assert sample is not None
        # After stripe splitting, the same path can appear once per stripe
        # in the manifest. Deduplicate so we only open each file once for
        # schema inference (and stay within the 16-worker thread pool).
        sample_paths = list(dict.fromkeys(sample.paths.tolist()))
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
    def resolve_partitioning(self, sample: FileManifest | None) -> Partitioning | None:
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
        filesystem: FileSystem | None = None,
        **options,
    ) -> OrcScanner:
        return OrcScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
        )
