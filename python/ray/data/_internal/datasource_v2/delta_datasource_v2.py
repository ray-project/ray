"""DataSourceV2 implementation for Delta Lake tables."""

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import pyarrow as pa

from ray.data._internal.datasource_v2.chunkers.file_chunker import FileChunker
from ray.data._internal.datasource_v2.datasource_v2 import (
    DatasourceCategory,
    DataSourceV2,
)
from ray.data._internal.datasource_v2.listing.delta_file_indexer import DeltaFileIndexer
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.readers.file_reader import (
    INCLUDE_PATHS_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    InMemorySizeEstimator,
    ParquetInMemorySizeEstimator,
)
from ray.data._internal.datasource_v2.scanners.delta_scanner import DeltaScanner
from ray.data.context import DataContext
from ray.data.datasource.partitioning import (
    HIVE_DEFAULT_PARTITION,
    Partitioning,
    PartitionStyle,
)
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem

    from ray.data.datasource.file_based_datasource import FileShuffleConfig

logger = logging.getLogger(__name__)


class DeltaDatasourceV2(DataSourceV2[FileManifest]):
    """V2 datasource for Delta Lake tables.

    Listing is delegated to :class:`DeltaFileIndexer`, which reads the Delta
    transaction log instead of walking the filesystem. Scanning and reading
    reuse :class:`ParquetScanner` and ``ParquetFileReader`` unchanged -- the
    files a Delta table points at are ordinary Parquet, and Delta lays
    partitioned tables out in Hive style, so partition columns are
    reconstructed from the file path by the existing reader.

    That reuse is deliberate: filter pushdown, column pruning, limit
    pushdown, and partition pruning all come from ``ArrowFileScanner``,
    leaving this class responsible only for what is genuinely Delta-specific
    -- reading the log for schema, partition columns, and file lists.
    """

    def __init__(
        self,
        path: str,
        *,
        version: Optional[int] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        filesystem: Optional["FileSystem"] = None,
        columns: Optional[List[str]] = None,
        include_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
        file_chunker: Optional[FileChunker] = None,
    ):
        super().__init__(name="DeltaV2", category=DatasourceCategory.DATA_LAKE)
        # ``deltalake`` resolves its storage backend from the URI, so the log
        # is always opened with the path exactly as given. The paths handed to
        # the reader must be filesystem-native instead -- PyArrow rejects a
        # scheme-prefixed path outright -- so resolve those separately.
        self._table_uri = path
        resolved_paths, resolved_filesystem = _resolve_paths_and_filesystem(
            [path], filesystem
        )
        assert len(resolved_paths) == 1, resolved_paths
        self._path = resolved_paths[0]
        filesystem = resolved_filesystem
        self._version = version
        self._storage_options = storage_options
        self._filesystem = filesystem
        self._columns = columns
        self._include_paths = include_paths
        self._shuffle = shuffle
        self._file_chunker = file_chunker
        self._table = None

    @property
    def paths(self) -> List[str]:
        return [self._path]

    @property
    def filesystem(self) -> Optional["FileSystem"]:
        return self._filesystem

    @property
    def file_extensions(self) -> Optional[List[str]]:
        """No extension filtering: the log names every file exactly.

        Directory-listing datasources filter by extension to skip
        non-data files they stumble across. Nothing is stumbled across
        here, and a Delta table may legitimately hold files whose names
        don't end in ``.parquet``.
        """
        return None

    @property
    def shuffle(self) -> Optional[Union[Literal["files"], "FileShuffleConfig"]]:
        return self._shuffle

    def _open_table(self):
        """Open the pinned snapshot, reusing it across driver-side calls.

        Planning asks for the schema, the partitioning and the scanner
        separately; each would otherwise re-read the log. The snapshot is
        pinned to a concrete version by ``read_delta``, so caching it can't
        mask a concurrent commit. Not part of the pickled state -- workers
        open their own.
        """
        from deltalake import DeltaTable

        if self._table is None:
            self._table = DeltaTable(
                self._table_uri,
                version=self._version,
                storage_options=self._storage_options,
            )
        return self._table

    def __getstate__(self):
        # ``DeltaTable`` wraps a Rust handle; drop the cache when this is sent
        # to a worker rather than requiring it to be picklable.
        return {**self.__dict__, "_table": None}

    def _get_file_indexer(self) -> DeltaFileIndexer:
        return DeltaFileIndexer(
            table_uri=self._table_uri,
            version=self._version,
            storage_options=self._storage_options,
            file_chunker=self._file_chunker,
        )

    def get_size_estimator(self) -> InMemorySizeEstimator:
        # The files a Delta table points at are ordinary Parquet, so the
        # Parquet estimator applies unchanged.
        return ParquetInMemorySizeEstimator()

    def _partitioning(self) -> Optional[Partitioning]:
        """Hive partitioning named and typed by the Delta log.

        Parquet has to discover partition keys by parsing a sample path.
        The Delta log states them outright, so no sample is needed and an
        unpartitioned table is reported as such rather than guessed at.

        ``field_types`` matters for more than convenience: it is what lets
        the scanner evaluate a partition predicate itself. Partition values
        exist only as directory names, so without it ``col("year") ==
        lit(2024)`` compares the string ``"2024"`` to an ``int64`` literal,
        which raises inside PyArrow and is conservatively read as "keep the
        file" -- leaving log-level pruning as the only thing enforcing the
        predicate. ``read_delta`` keeps tables whose partition types this
        can't express on the V1 path.
        """
        from ray.data.read_api import _delta_partition_field_types

        table = self._open_table()
        partition_columns = table.metadata().partition_columns
        if not partition_columns:
            return None
        return Partitioning(
            style=PartitionStyle.HIVE,
            field_names=list(partition_columns),
            field_types=_delta_partition_field_types(table),
            null_fallback=HIVE_DEFAULT_PARTITION,
        )

    def resolve_partitioning(self, sample: FileManifest) -> Optional[Partitioning]:
        return self._partitioning()

    def infer_schema(self, sample: FileManifest) -> pa.Schema:
        """Return the table's schema as recorded in the Delta log.

        No Parquet footers are read. Besides being cheaper, the log's schema
        is the authoritative one: it spans the whole table, so a file written
        before a column was added does not narrow the result, and partition
        columns keep their declared position and type rather than being
        appended as the strings the directory names encode.
        """
        schema = pa.schema(self._open_table().schema().to_arrow())

        if (
            self._include_paths
            and schema.get_field_index(INCLUDE_PATHS_COLUMN_NAME) == -1
        ):
            schema = schema.append(pa.field(INCLUDE_PATHS_COLUMN_NAME, pa.string()))

        return schema

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> DeltaScanner:
        partitioning = options.get("partitioning")
        if partitioning is None:
            partitioning = self._partitioning()

        # Pin a projection even when the caller asked for every column. The
        # reader appends path-derived partition columns after the file's own,
        # and only reorders to an explicit projection -- without this, a
        # partitioned table would come back with its partition columns moved
        # to the end, unlike every previous release of ``read_delta``.
        #
        # ``path`` is synthesized post-read and is absent from the caller's
        # ``columns``, so add it back explicitly; a projection that omits it
        # would drop the column ``include_paths`` was asked to produce.
        columns = self._columns if self._columns is not None else list(schema.names)
        if self._include_paths and INCLUDE_PATHS_COLUMN_NAME not in columns:
            columns = [*columns, INCLUDE_PATHS_COLUMN_NAME]

        return DeltaScanner(
            schema=schema,
            filesystem=filesystem or self._filesystem,
            partitioning=partitioning,
            columns=tuple(columns),
            include_paths=self._include_paths,
            shuffle=self._shuffle,
            ignore_prefixes=options.get("ignore_prefixes"),
            target_block_size=DataContext.get_current().target_max_block_size,
        )
