"""``DataSourceV2`` for Delta Lake tables.

``read_delta`` opens the table once, in a Ray task (``open_delta_table``),
which pins the version and loads the log. The driver gets the schema and
partition columns; the file list stays in the object store for the listing
(``DeltaFileIndexer``). The data files are Parquet and read by the Parquet
reader (``DeltaScanner``).

Everything goes through a filesystem rooted at the table directory, with paths
relative to it, as ``DeltaTable.to_pyarrow_dataset`` -- the V1 reader -- does.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import pyarrow as pa
from typing_extensions import override

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.common.synthesized_columns import PathColumn
from ray.data._internal.datasource_v2.formats.delta.delta_file_indexer import (
    ADD_ACTION_PATH_COLUMN,
    DeltaFileIndexer,
    DeltaTableSnapshot,
    is_supported_partition_type,
    load_add_actions,
    make_partitioning,
    resolve_data_file_path,
)
from ray.data._internal.datasource_v2.formats.delta.delta_filesystem import (
    make_table_filesystem,
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

    from ray import ObjectRef
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
    table: "DeltaTable", *, include_paths: bool
) -> Optional[str]:
    """Why ``table`` can't be read with :class:`DeltaDatasourceV2`, if it can't.

    ``read_delta`` reads the tables this rejects with the V1 reader, as it
    does reads that pass PyArrow scan options. V1 can read some of them:
    partition columns of types V2 doesn't parse, a table's own ``path`` column
    with ``include_paths``, and reader features the installed ``deltalake``
    reads through PyArrow (``variantType`` in 1.6). V2 needs to support these
    before V1 is removed. V1 rejects the rest, such as column mapping and
    deletion vectors, with the same error as before.

    Args:
        table: The table, opened at the version to read.
        include_paths: The read's ``include_paths`` argument.

    Returns:
        A short reason for the V1 fallback, or ``None`` if V2 can read it.
    """
    try:
        protocol = table.protocol()
        # Reader version 2 adds only column mapping, which this reader doesn't
        # support yet. Version 3 lists the features it needs, checked below.
        if protocol.min_reader_version == 2:
            return "the table needs reader version 2 (column mapping)"
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
        schema = pa.schema(table.schema().to_arrow())
        unsupported_partitions = {
            name: str(schema.field(name).type)
            for name in metadata.partition_columns
            if not is_supported_partition_type(schema.field(name).type)
        }
        if unsupported_partitions:
            return (
                "the partition columns have types the V2 reader doesn't "
                f"support: {unsupported_partitions}"
            )
        if include_paths and "path" in schema.names:
            return "the table has its own `path` column and include_paths is set"
    except Exception as e:  # Fail closed: when in doubt, read with V1.
        return f"the table couldn't be inspected ({type(e).__name__}: {e})"
    return None


@dataclass(frozen=True)
class DeltaTableInfo:
    """What the driver needs from one version of a Delta table.

    Unlike a ``deltalake.DeltaTable``, it can be pickled, so the task that
    opens the table returns it.

    Attributes:
        table_uri: The table URI, as ``DeltaTable.table_uri`` reports it.
        version: The version the table was opened at.
        unsupported_reason: Why the V2 reader can't read the table, or
            ``None`` if it can.
        schema: The table schema. Empty if ``unsupported_reason`` is set.
        partition_columns: The partition columns. Empty if
            ``unsupported_reason`` is set.
    """

    table_uri: str
    version: int
    unsupported_reason: Optional[str]
    schema: pa.Schema
    partition_columns: Tuple[str, ...]

    @classmethod
    def from_table(
        cls, table: "DeltaTable", *, include_paths: bool = False
    ) -> "DeltaTableInfo":
        """Describe ``table``, opened at the version to read.

        Args:
            table: The table.
            include_paths: The read's ``include_paths`` argument, which the V2
                check takes.

        Returns:
            The table's description.
        """
        reason = delta_datasource_v2_unsupported_reason(
            table, include_paths=include_paths
        )
        return cls(
            table_uri=table.table_uri,
            version=table.version(),
            unsupported_reason=reason,
            # The check reads both; they're needed only if it passed.
            schema=pa.schema(table.schema().to_arrow())
            if reason is None
            else pa.schema([]),
            partition_columns=(
                tuple(table.metadata().partition_columns) if reason is None else ()
            ),
        )


def _open_and_load_table(
    table_uri: str,
    version: Optional[int],
    storage_options: Optional[Dict[str, Any]],
    partition_filters: Optional[List[Any]],
    include_paths: bool,
) -> Tuple[DeltaTableInfo, Optional[pa.Table]]:
    """Open the table and, if V2 can read it, load its ``add`` actions."""
    from deltalake import DeltaTable

    from ray.data.read_api import _delta_partition_filter_kwargs

    table = DeltaTable(table_uri, version=version, storage_options=storage_options)
    info = DeltaTableInfo.from_table(table, include_paths=include_paths)
    if info.unsupported_reason is not None:
        return info, None
    if partition_filters is not None:
        # Rejects what the V1 reader rejects.
        _delta_partition_filter_kwargs(table, partition_filters)
    add_actions = load_add_actions(table, partition_filters)
    # Raises for a data file outside the table directory, such as a shallow
    # clone's, so that ``read_delta`` fails instead of the listing task.
    for path in add_actions[ADD_ACTION_PATH_COLUMN].to_pylist():
        resolve_data_file_path(path)
    return info, add_actions


def open_delta_table(
    table_uri: str,
    *,
    version: Optional[int],
    storage_options: Optional[Dict[str, Any]],
    partition_filters: Optional[List[Any]],
    include_paths: bool,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Tuple[DeltaTableInfo, "ObjectRef[Optional[pa.Table]]"]:
    """Open a Delta table once, in a Ray task, for :class:`DeltaDatasourceV2`.

    ``deltalake`` has no metadata-only load
    (https://github.com/delta-io/delta-rs/issues/5034), so opening a table
    loads its file list. The task returns the table's description to the
    driver, which then never holds the file list, and keeps the ``add``
    actions in the object store, where the listing reads them instead of
    loading the log again.

    Args:
        table_uri: The table URI.
        version: The version to read, or ``None`` for the latest.
        storage_options: ``deltalake`` storage options, for credentials.
        partition_filters: ``read_delta``'s ``partition_filters``. The task
            checks them and keeps only the files they select.
        include_paths: The read's ``include_paths`` argument.
        ray_remote_args: Options for the task, such as its ``runtime_env``.

    Returns:
        The table's description, and a reference to its ``add`` actions, which
        are ``None`` if the V2 reader can't read the table.

    Raises:
        Exception: What opening or checking the table raised, such as
            ``deltalake``'s ``TableNotFoundError`` or a ``ValueError`` for
            invalid ``partition_filters``.
    """
    import ray
    from ray.data._internal.remote_fn import cached_remote_fn
    from ray.exceptions import RayTaskError

    open_remote = cached_remote_fn(_open_and_load_table, num_returns=2).options(
        **(ray_remote_args or {})
    )
    info_ref, add_actions_ref = open_remote.remote(
        table_uri, version, storage_options, partition_filters, include_paths
    )
    try:
        info = ray.get(info_ref)
    except RayTaskError as e:
        # Raise the table's own error, as when the driver opened the table.
        if isinstance(e.cause, Exception):
            raise e.cause from e
        raise
    assert isinstance(info, DeltaTableInfo), type(info)
    return info, add_actions_ref


@DeveloperAPI
class DeltaDatasourceV2(DataSourceWithMetadata[FileManifest]):
    """V2 datasource for one version of a Delta Lake table.

    The version is the one ``table`` was opened at: ``read_delta``'s
    ``version=`` argument, or the latest version when ``read_delta`` is
    called. Every listing task, retry and re-execution reads that version, so
    the rows don't change if the table is written to while the dataset is in
    use, and they match the schema read when the dataset was created.
    """

    def __init__(
        self,
        table: Union["DeltaTable", DeltaTableInfo],
        *,
        loaded_add_actions: Optional["ObjectRef[pa.Table]"] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        filesystem: Optional["FileSystem"] = None,
        columns: Optional[List[str]] = None,
        include_paths: bool = False,
        shuffle: Optional[Union[Literal["files"], "FileShuffleConfig"]] = None,
        version_pinned_by_caller: bool = False,
        partition_filters: Optional[List[Any]] = None,
    ):
        """Create the datasource.

        Args:
            table: The table, opened at the version to read, or its
                description from ``open_delta_table``.
            loaded_add_actions: The ``add`` actions ``open_delta_table``
                loaded, which the listing reads instead of loading the log
                again. ``None`` loads them in the listing.
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
            partition_filters: ``read_delta``'s ``partition_filters``, already
                checked. The listing reads only the files they select, which
                ``deltalake`` picks as it does for the V1 reader.
        """
        super().__init__(name="DeltaV2", category=DatasourceCategory.DATA_LAKE)
        info = (
            table
            if isinstance(table, DeltaTableInfo)
            else DeltaTableInfo.from_table(table, include_paths=include_paths)
        )
        if info.unsupported_reason is not None:
            raise ValueError(
                f"The V2 reader can't read the Delta table {info.table_uri!r}: "
                f"{info.unsupported_reason}."
            )
        # Pinned here so every listing task, retry and re-execution reads the
        # same version, even if the table changes in the meantime.
        self._snapshot = DeltaTableSnapshot(
            table_uri=info.table_uri,
            version=info.version,
            storage_options=storage_options,
            partition_filters=partition_filters,
            loaded_add_actions=loaded_add_actions,
        )
        self._table_schema = info.schema
        self._partition_columns: List[str] = list(info.partition_columns)
        self._partition_schema = pa.schema(
            [self._table_schema.field(name) for name in self._partition_columns]
        )
        self._partitioning = make_partitioning(self._partition_columns)
        self._version_pinned_by_caller = version_pinned_by_caller
        if filesystem is None:
            filesystem = make_table_filesystem(info.table_uri, storage_options)
        self._filesystem = RetryingPyFileSystem.wrap(
            filesystem,
            retryable_errors=DataContext.get_current().retried_io_errors,
        )
        self._synthesized_columns = (PathColumn(),) if include_paths else ()
        if columns is not None:
            # With ``include_paths=True``, ``columns`` may include ``path``, as
            # with the V1 reader. It isn't a table column, so accept it here.
            known = set(self._table_schema.names) | {
                column.name for column in self._synthesized_columns
            }
            unknown = [c for c in columns if c not in known]
            if unknown:
                raise ValueError(
                    f"Columns {unknown} don't exist in the Delta table "
                    f"{info.table_uri!r}, whose columns are "
                    f"{self._table_schema.names}."
                )
        # Pinned on the scanner so the output keeps the table's (or the
        # caller's) column order: unprojected, the reader would append the
        # partition columns, which it takes from the listing, at the end. The
        # ``path`` column comes last even when ``columns`` names it, as with
        # the V1 reader.
        synthesized = [column.name for column in self._synthesized_columns]
        projected = list(columns) if columns is not None else self._table_schema.names
        self._projected_columns = tuple(
            [name for name in projected if name not in synthesized] + synthesized
        )
        _validate_shuffle_arg(shuffle)
        # ``shuffle="files"`` gets its seed here, once per dataset. Each
        # execution still shuffles differently, because the seed is mixed with
        # the execution number (``reseed_after_execution``). The Delta listing
        # applies the shuffle itself: ``_read_datasource_v2`` doesn't shuffle
        # the listing of a source with metadata.
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
            partition_schema=self._partition_schema,
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
            version=self._snapshot.version,
            version_pinned_by_caller=self._version_pinned_by_caller,
        )
