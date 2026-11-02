"""``DataSourceV2`` for Delta Lake tables.

The driver opens the table's log without its file list, pins the version and
takes the schema from the log. Listing tasks read the file list from the log at
that version (``DeltaFileIndexer``); the data files are Parquet and read by the
Parquet reader (``DeltaScanner``).

Everything goes through a filesystem rooted at the table directory, with paths
relative to it, as ``DeltaTable.to_pyarrow_dataset`` -- the V1 reader -- does.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    DeltaFileIndexer,
    DeltaTableSnapshot,
    make_partitioning,
)
from ray.data._internal.datasource_v2.formats.delta.delta_scanner import (
    DeltaScanner,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    make_row_group_bin_packer,
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    DataSourceWithMetadata,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.util import RetryingPyFileSystem
from ray.data.context import DataContext
from ray.data.datasource.file_based_datasource import (
    FileShuffleConfig,
    _validate_shuffle_arg,
)
from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.common.online_bin_packer import (
        OnlineBinPacker,
    )
    from ray.data._internal.datasource_v2.interfaces.file_partitioner import (
        PartitionHints,
    )

logger = logging.getLogger(__name__)

# Reader features the V2 reader handles. Tables that need others (deletion
# vectors, column mapping, ...) are read with the V1 reader.
_SUPPORTED_READER_FEATURES = frozenset({"timestampNtz"})

_INT32_MAX = 2**31 - 1


def delta_datasource_v2_unsupported_reason(
    table: "DeltaTable",
    *,
    include_paths: bool,
    arrow_parquet_args: Dict[str, Any],
) -> Optional[str]:
    """Why ``table`` can't be read with :class:`DeltaDatasourceV2`, if it can't.

    Args:
        table: The table, opened at the version to read.
        include_paths: The read's ``include_paths`` argument.
        arrow_parquet_args: The read's extra PyArrow scan options.

    Returns:
        A short reason for the V1 fallback, or ``None`` if V2 can read it.
    """
    if arrow_parquet_args:
        return f"PyArrow scan options were passed: {sorted(arrow_parquet_args)}"
    try:
        protocol = table.protocol()
        if protocol.min_reader_version not in (1, 3):
            return f"the table needs reader version {protocol.min_reader_version}"
        unsupported_features = (
            set(protocol.reader_features or []) - _SUPPORTED_READER_FEATURES
        )
        if unsupported_features:
            return f"the table needs the reader features {sorted(unsupported_features)}"
        metadata = table.metadata()
        configuration = metadata.configuration or {}
        if configuration.get("delta.columnMapping.mode", "none") != "none":
            return "the table uses column mapping"
        if (
            metadata.partition_columns
            and configuration.get("delta.randomizeFilePrefixes", "false").lower()
            == "true"
        ):
            return (
                "the table writes data files under random prefixes, not "
                "Hive-style partition directories"
            )
        schema = pa.schema(table.schema().to_arrow())
        # Delta writes a timestamp partition value in UTC without an offset,
        # which doesn't cast to a timestamp type with a time zone.
        zoned_partitions = [
            name
            for name in metadata.partition_columns
            if pa.types.is_timestamp(schema.field(name).type)
            and schema.field(name).type.tz is not None
        ]
        if zoned_partitions:
            return (
                f"the timestamp partition columns {zoned_partitions} have a time zone"
            )
        if include_paths and "path" in schema.names:
            return "the table has its own `path` column and include_paths is set"
    except Exception as e:  # Fail closed: when in doubt, read with V1.
        return f"the table couldn't be inspected ({type(e).__name__}: {e})"
    return None


def _make_table_filesystem(
    table_uri: str, storage_options: Optional[Dict[str, Any]]
) -> "FileSystem":
    """Filesystem rooted at the table directory that ``deltalake`` reads through.

    The same one ``DeltaTable.to_pyarrow_dataset`` builds when no filesystem
    is given: it takes ``storage_options`` for credentials and every URI
    scheme ``deltalake`` does, including ``az://``, which PyArrow can't parse.
    It is picklable.
    """
    import pyarrow.fs as pa_fs
    from deltalake.fs import DeltaStorageHandler

    return pa_fs.PyFileSystem(DeltaStorageHandler(table_uri, options=storage_options))


@DeveloperAPI
class DeltaDatasourceV2(DataSourceWithMetadata[FileManifest]):
    """V2 datasource for one version of a Delta Lake table."""

    def __init__(
        self,
        table: "DeltaTable",
        *,
        storage_options: Optional[Dict[str, Any]] = None,
        filesystem: Optional["FileSystem"] = None,
        columns: Optional[List[str]] = None,
        include_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
        version_pinned_by_caller: bool = False,
    ):
        """Create the datasource.

        Args:
            table: The table, opened at the version to read. Its file list
                isn't needed: open it with ``without_files=True``.
            storage_options: The ``deltalake`` storage options the table was
                opened with.
            filesystem: A filesystem rooted at the table directory. ``None``
                uses the one ``deltalake`` builds from ``storage_options``.
            columns: Columns to read, in this order. ``None`` reads every
                column, in the table's order.
            include_paths: Whether to add a ``path`` column with each row's
                data file path, relative to the table directory.
            shuffle: ``"files"`` or a ``FileShuffleConfig`` to shuffle the
                table's data files before reading them.
            version_pinned_by_caller: Whether the caller chose the version
                ``table`` is opened at, rather than the latest one.
        """
        super().__init__(name="DeltaV2", category=DatasourceCategory.DATA_LAKE)
        # Pinned here so every listing task, retry and re-execution reads the
        # same version, even if the table changes in the meantime.
        self._snapshot = DeltaTableSnapshot(
            table_uri=table.table_uri,
            version=table.version(),
            storage_options=storage_options,
        )
        self._table_schema = pa.schema(table.schema().to_arrow())
        self._partition_columns: List[str] = list(table.metadata().partition_columns)
        self._partitioning = make_partitioning(self._partition_columns)
        self._version_pinned_by_caller = version_pinned_by_caller
        if filesystem is None:
            filesystem = _make_table_filesystem(table.table_uri, storage_options)
        self._filesystem = RetryingPyFileSystem.wrap(
            filesystem,
            retryable_errors=DataContext.get_current().retried_io_errors,
        )
        self._synthesized_columns = (PathColumn(),) if include_paths else ()
        if columns is not None:
            # ``columns`` may name the ``path`` column, as with the V1 reader.
            known = set(self._table_schema.names) | {
                column.name for column in self._synthesized_columns
            }
            unknown = [c for c in columns if c not in known]
            if unknown:
                raise ValueError(
                    f"Columns {unknown} don't exist in the Delta table "
                    f"{table.table_uri!r}, whose columns are "
                    f"{self._table_schema.names}."
                )
        # Pinned on the scanner so the output keeps the table's (or the
        # caller's) column order: unprojected, the reader would append the
        # partition columns, which it takes from the path, at the end. The
        # ``path`` column comes last even when ``columns`` names it, as with
        # the V1 reader.
        synthesized = [column.name for column in self._synthesized_columns]
        projected = list(columns) if columns is not None else self._table_schema.names
        self._projected_columns = tuple(
            [name for name in projected if name not in synthesized] + synthesized
        )
        _validate_shuffle_arg(shuffle)
        # ``_read_datasource_v2`` gives a metadata-backed source's listing no
        # file shuffle, so the indexer applies it. As for file sources, the seed
        # for ``"files"`` is drawn when the dataset is created.
        self._shuffle_config: Optional[FileShuffleConfig] = (
            FileShuffleConfig(seed=time.time_ns() % _INT32_MAX)
            if shuffle == "files"
            else shuffle
        )

    @property
    def paths(self) -> List[str]:
        # DSv2 needs ``paths``, but the indexer lists files from the log, so
        # this is only a label for the table.
        return [self._snapshot.table_uri]

    @property
    def version(self) -> int:
        """The table version this datasource reads."""
        return self._snapshot.version

    def _get_file_indexer(self) -> DeltaFileIndexer:
        return DeltaFileIndexer(
            snapshot=self._snapshot,
            filesystem=self._filesystem,
            partition_columns=self._partition_columns,
            shuffle_config=self._shuffle_config,
            coalesce_bytes=env_integer("RAY_DATA_PARQUET_FOOTER_COALESCE_BYTES", 0),
            small_shuffle_chunk_size=(
                DataContext.get_current().small_shuffle_chunk_size
            ),
        )

    def get_file_partitioner(
        self, *, hints: Optional["PartitionHints"] = None
    ) -> "OnlineBinPacker":
        # The footer-read listing emits one row per row-group run with exact
        # sizes, as for Parquet.
        return make_row_group_bin_packer(self._shuffle_config)

    @override
    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        """The table schema from the log, plus the ``path`` column if asked."""
        schema = self._table_schema
        for column in self._synthesized_columns:
            schema = schema.append(pa.field(column.name, column.type))
        return schema

    def create_scanner(
        self,
        schema: pa.Schema,
        filesystem: Optional["FileSystem"] = None,
        **options: Any,
    ) -> DeltaScanner:
        # ``_read_datasource_v2`` passes ``filesystem=None`` and
        # ``partitioning=None`` for a metadata-backed source; both come from
        # the table.
        return DeltaScanner(
            schema=schema,
            columns=self._projected_columns,
            filesystem=self._filesystem,
            partitioning=self._partitioning,
            synthesized_columns=self._synthesized_columns,
            shuffle=self._shuffle_config,
            target_block_size=DataContext.get_current().target_max_block_size,
            table_uri=self._snapshot.table_uri,
            version_pinned_by_caller=self._version_pinned_by_caller,
        )
