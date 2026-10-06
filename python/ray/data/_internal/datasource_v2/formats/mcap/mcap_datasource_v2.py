"""Concrete ``DataSourceV2`` for MCAP files.

``read_api.read_mcap`` constructs it when ``DataContext.use_datasource_v2`` is
set. Listing reads each file's summary, drops files that cannot match the
selection, and emits one row per chunk (``MCAPSummaryIndexer``).
``OnlineBinPacker`` groups the chunks into read tasks of about
``RAY_DATA_MCAP_BIN_PACKING_BYTES`` uncompressed bytes (128 MiB by default).
``MCAPScanner`` holds the optimizer's pushdowns and builds the ``MCAPReader``,
which seeks to each task's chunks. ``infer_schema`` calls ``infer_data_type``
(``mcap_data_column``) to decide whether ``data`` holds JSON values or bytes.

Format specification: https://mcap.dev/spec
"""

from __future__ import annotations

import copy
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Union,
)

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.mcap.mcap_data_column import (
    infer_data_type,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_file_indexer import (
    MCAPSummaryIndexer,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_message_rows import (
    message_schema,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_options import (
    MCAPSelection,
    TimeRange,
)
from ray.data._internal.datasource_v2.formats.mcap.mcap_scanner import MCAPScanner
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileIndexer
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.synthesized_columns import (
    SynthesizedColumn,
)
from ray.data._internal.util import MiB, _check_import, _is_local_scheme
from ray.data.context import DataContext
from ray.data.datasource.partitioning import (
    Partitioning,
    PathPartitionFilter,
    PathPartitionParser,
    _partition_field_types_to_pa_schema,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data.datasource.file_based_datasource import FileShuffleConfig

# Chunks are packed into read tasks of about this many uncompressed bytes.
_BIN_PACKING_BYTES = env_integer("RAY_DATA_MCAP_BIN_PACKING_BYTES", 128 * MiB)
# How many partly filled read tasks the packer keeps open at once.
_BIN_PACKING_MAX_SHARED_OPEN_BINS = env_integer(
    "RAY_DATA_MCAP_BIN_PACKING_MAX_SHARED_OPEN_BINS", 16
)


@DeveloperAPI
class MCAPDatasourceV2(FileDataSourceV2):
    """V2 MCAP datasource: summary-driven listing, chunk-level read tasks."""

    def __init__(
        self,
        paths: List[str],
        *,
        topics: Optional[Iterable[str]] = None,
        time_range: Optional[TimeRange] = None,
        message_types: Optional[Iterable[str]] = None,
        include_metadata: bool = True,
        log_time_order: bool = True,
        include_row_id: bool = False,
        include_paths: bool = False,
        filesystem: Optional["FileSystem"] = None,
        partitioning: Optional[Partitioning] = None,
        partition_filter: Optional[PathPartitionFilter] = None,
        file_extensions: Optional[Union[List[str], tuple[str, ...]]] = ("mcap",),
        ignore_missing_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
    ):
        super().__init__(name="MCAP", category=DatasourceCategory.FILE_BASED)
        _check_import(self, module="mcap", package="mcap")

        # Captured against the original paths: resolution below strips the
        # ``local://`` scheme (see ``ParquetDatasourceV2``).
        self._supports_distributed_reads = not _is_local_scheme(paths)
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            paths, filesystem
        )
        self._paths: List[str] = resolved_paths
        self._filesystem = resolved_filesystem
        self._selection = MCAPSelection.create(topics, time_range, message_types)
        self._include_metadata = include_metadata
        self._log_time_order = log_time_order
        self._include_row_id = include_row_id
        self._partitioning = partitioning
        self._partition_filter = partition_filter
        self._file_extensions = (
            list(file_extensions) if file_extensions is not None else None
        )
        self._ignore_missing_paths = ignore_missing_paths
        self._shuffle = shuffle
        synthesized: List[SynthesizedColumn] = []
        if include_paths:
            synthesized.append(PathColumn())
        self._synthesized_columns = tuple(synthesized)

    @property
    def paths(self) -> List[str]:
        return self._paths

    @property
    def filesystem(self) -> "FileSystem":
        return self._filesystem

    @property
    def file_extensions(self) -> Optional[List[str]]:
        return self._file_extensions

    @property
    def shuffle(self) -> Optional[Union[Literal["files"], "FileShuffleConfig"]]:
        return self._shuffle

    @property
    def selection(self) -> MCAPSelection:
        return self._selection

    def _get_file_indexer(self) -> FileIndexer:
        return MCAPSummaryIndexer(
            selection=self._selection,
            ignore_missing_paths=self._ignore_missing_paths,
        )

    def get_file_partitioner(self, **kwargs):
        # Each listing row is one chunk with its exact uncompressed size, so
        # chunks are packed into read tasks by bytes.
        #
        # Packing is per listing shard, not global, so listing can run as many
        # parallel tasks. Each chunk still lands in exactly one read task; the
        # cost is at most one under-filled task per shard.
        from ray.data._internal.datasource_v2.common.online_bin_packer import (
            OnlineBinPacker,
        )

        return OnlineBinPacker(
            max_bin_bytes=_BIN_PACKING_BYTES,
            max_shared_open_bins=_BIN_PACKING_MAX_SHARED_OPEN_BINS,
            requires_global_input=False,
        )

    @property
    @override
    def schema_needs_file_sample(self) -> bool:
        return True

    @override
    def resolve_partitioning(
        self, sample: Optional[FileManifest]
    ) -> Optional[Partitioning]:
        """``self._partitioning`` with field names discovered from a sample path.

        A partitioning is returned as a copy, so callers cannot change the
        datasource's own object.
        """
        if self._partitioning is None:
            return None
        if sample is None or len(sample) == 0 or self._partitioning.field_names:
            return copy.deepcopy(self._partitioning)
        partition_kv = PathPartitionParser(self._partitioning)(sample.paths.tolist()[0])
        if not partition_kv:
            return copy.deepcopy(self._partitioning)
        return Partitioning(
            style=self._partitioning.style,
            base_dir=self._partitioning.base_dir,
            field_names=list(partition_kv.keys()),
            field_types=self._partitioning.field_types,
            filesystem=self._partitioning.filesystem,
        )

    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        """The schema of message rows, plus partition and synthesized columns.

        Every column but ``data`` has a fixed type. ``data`` holds decoded JSON
        values when every selected channel of the sampled files is JSON-encoded,
        and the raw payload bytes (``binary``) otherwise. The reader follows this
        one decision for every file, so a selection that mixes encodings keeps
        every payload as bytes.
        """
        assert sample is not None, "MCAP always receives a sample"
        data_type = (
            infer_data_type(
                self._selection,
                self._filesystem,
                sample.paths.tolist(),
                self._listed_paths,
            )
            if len(sample) > 0
            else None
        )
        schema = message_schema(
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            data_type=data_type,
        )
        partitioning = self.resolve_partitioning(sample)
        if partitioning is not None and len(sample) > 0:
            partition_kv = PathPartitionParser(partitioning)(sample.paths.tolist()[0])
            partition_schema = _partition_field_types_to_pa_schema(
                field_names=list(partition_kv.keys()),
                field_types=partitioning.field_types or {},
            )
            for name in partition_kv:
                if schema.get_field_index(name) == -1:
                    schema = schema.append(partition_schema.field(name))
        for column in self._synthesized_columns:
            idx = schema.get_field_index(column.name)
            if idx == -1:
                schema = schema.append(pa.field(column.name, column.type))
            elif schema.field(idx).type != column.type:
                schema = schema.set(idx, pa.field(column.name, column.type))
        return schema

    def _listed_paths(self) -> Iterator[str]:
        """The files the read lists, in listing order."""
        from ray.data._internal.datasource_v2.common.listing_utils import (
            _build_pruners,
        )

        for file_info in self._get_file_indexer().list_file_infos(
            pa.array(self._paths, pa.string()),
            filesystem=self._filesystem,
            # The filters the listing applies, so planning never opens a file
            # the read skips, such as a sidecar or a filtered-out partition.
            pruners=_build_pruners(self._file_extensions, self._partition_filter),
            preserve_order=True,
        ):
            yield file_info.path

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> MCAPScanner:
        return MCAPScanner(
            schema=schema,
            selection=self._selection,
            include_metadata=self._include_metadata,
            include_row_id=self._include_row_id,
            log_time_order=self._log_time_order,
            filesystem=filesystem or self._filesystem,
            partitioning=options.get("partitioning", self._partitioning),
            synthesized_columns=self._synthesized_columns,
            shuffle=self._shuffle,
            target_block_size=DataContext.get_current().target_max_block_size,
        )
