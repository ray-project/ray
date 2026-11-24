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

# Reader features this reader handles. ``columnMapping`` only with the mapping
# mode ``none``, which ``check_delta_table_is_readable`` checks.
#
# `v2Checkpoint` only changes how the log is stored: checkpoints can list the
# data files in sidecar files. deltalake replays them when it lists the add
# actions, so the V2 reader sees every file. The V1 reader's
# `to_pyarrow_dataset` refuses these tables, and Databricks turns v2
# checkpoints on for new tables by default.
_SUPPORTED_READER_FEATURES = frozenset(
    {"timestampNtz", "columnMapping", "v2Checkpoint"}
)

# PyArrow scan options that only tune how the V1 reader read; the V2 reader
# sets its own batch size, readahead and threads, so it ignores them.
_IGNORED_SCAN_OPTIONS = frozenset(
    {
        "batch_size",
        "batch_readahead",
        "fragment_readahead",
        "use_threads",
        "cache_metadata",
        "memory_pool",
    }
)

_INT32_MAX = 2**31 - 1

# The oldest ``deltalake`` release ``read_delta`` is tested with.
MIN_DELTALAKE_VERSION = (1, 5, 0)


def check_deltalake_version(installed: str) -> None:
    """Raise if the installed ``deltalake`` is older than ``read_delta`` supports.

    Only the release numbers are compared, so a pre-release or development
    build of a supported release (``1.6.0rc1``, ``1.5.0.dev0``) passes. A
    version string that doesn't parse, such as a vendored build's, passes too.

    Args:
        installed: ``deltalake.__version__``.

    Raises:
        ImportError: If the release is older than ``MIN_DELTALAKE_VERSION``.
    """
    from packaging.version import InvalidVersion, Version

    try:
        release = Version(installed).release
    except InvalidVersion:
        return
    if release < MIN_DELTALAKE_VERSION:
        minimum = ".".join(map(str, MIN_DELTALAKE_VERSION))
        raise ImportError(
            f"`ray.data.read_delta` needs deltalake {minimum} or later, but "
            f"{installed} is installed. Upgrade it with `pip install -U "
            f"'deltalake>={minimum}'`."
        )


def column_mapping_error(mode: str) -> NotImplementedError:
    """The error for a table that uses column mapping.

    Args:
        mode: The table's ``delta.columnMapping.mode``.

    Returns:
        The error to raise.
    """
    return NotImplementedError(
        "This Delta table uses column mapping (`delta.columnMapping.mode` is "
        f"{mode!r}), which `ray.data.read_delta` doesn't support yet. Its data "
        "files store columns under physical names that only the Delta log maps "
        "to the table's column names, so reading them would return nulls or "
        "wrong columns. Follow https://github.com/ray-project/ray/issues/64854 "
        "for support."
    )


def deletion_vectors_error(cause: str) -> RuntimeError:
    """The error for a table that declares deletion vectors.

    Args:
        cause: What found the feature, kept for anyone debugging deeper.

    Returns:
        The error to raise.
    """
    # The restriction is in reading the data files with pyarrow, which can't
    # apply the deletion-vector bitmaps, so honouring them is impossible on
    # that path, and reading would return deleted rows. Tables are rejected on
    # the *declared* reader feature, so a table with deletion vectors enabled
    # but none written is rejected too. Don't suggest upgrading: it won't help.
    return RuntimeError(
        "This Delta table declares the `deletionVectors` reader feature, which "
        "`ray.data.read_delta` cannot honour: it reads the data files with "
        "pyarrow, which can't apply deletion vectors. Upgrading deltalake will "
        "not change this, and note the table is rejected for *declaring* the "
        "feature, whether or not any deletion vectors have actually been "
        "written.\n"
        "\n"
        "Reading it with Ray Data needs the `delta.enableDeletionVectors"
        "` table property turned off and the feature dropped from the "
        "table's protocol. That rewrites data files and changes what "
        "other readers and writers see, so it is worth reading your "
        "Delta engine's documentation on removing the feature before "
        "doing it -- some engines, Databricks included, enable deletion "
        "vectors by default. If the property is already off, the "
        "declaration can linger until existing deletion vectors are "
        "purged and the feature is dropped.\n"
        "\n"
        "If reading deletion-vector tables directly matters to you, "
        "please open or comment on a Ray issue -- it would need a read "
        "path that can apply them, and knowing there is demand helps us "
        "prioritise it.\n"
        "\n"
        f"Underlying error: {cause}"
    )


def check_delta_table_is_readable(table: "DeltaTable", *, include_paths: bool) -> None:
    """Raise if :class:`DeltaDatasourceV2` can't read ``table``.

    Column mapping and deletion vectors get the errors the V1 reader raised.

    Args:
        table: The table, opened at the version to read.
        include_paths: The read's ``include_paths`` argument.

    Raises:
        NotImplementedError: If the table needs a feature this reader doesn't
            support yet: column mapping, a reader version or feature it
            doesn't know, or a partition column type it doesn't parse.
        RuntimeError: If the table declares deletion vectors.
        ValueError: If ``include_paths`` would replace the table's own
            ``path`` column.
    """
    protocol = table.protocol()
    metadata = table.metadata()
    mode = (metadata.configuration or {}).get("delta.columnMapping.mode") or "none"
    # Reader version 2 and the ``columnMapping`` feature only allow column
    # mapping. With the mode ``none``, the data files use the column names.
    if mode.lower() != "none":
        raise column_mapping_error(mode)
    features = set(protocol.reader_features or [])
    if "deletionVectors" in features:
        raise deletion_vectors_error(
            "the table's protocol lists the `deletionVectors` reader feature."
        )
    if protocol.min_reader_version not in (1, 2, 3):
        raise NotImplementedError(
            f"This Delta table needs reader version {protocol.min_reader_version}, "
            "which `ray.data.read_delta` doesn't support."
        )
    unsupported_features = features - _SUPPORTED_READER_FEATURES
    if unsupported_features:
        raise NotImplementedError(
            f"This Delta table needs the reader features "
            f"{sorted(unsupported_features)}, which `ray.data.read_delta` "
            "doesn't support yet."
        )
    schema = pa.schema(table.schema().to_arrow())
    unsupported_partitions = {
        name: str(schema.field(name).type)
        for name in metadata.partition_columns
        if not is_supported_partition_type(schema.field(name).type)
    }
    if unsupported_partitions:
        raise NotImplementedError(
            "`ray.data.read_delta` doesn't support the types of the partition "
            f"columns {unsupported_partitions} yet."
        )
    if include_paths and "path" in schema.names:
        raise ValueError(
            "The Delta table has its own `path` column, which "
            "`include_paths=True` would replace with the data file paths. Read "
            "it without `include_paths`."
        )


def check_delta_scan_options(arrow_parquet_args: Dict[str, Any]) -> None:
    """Check ``read_delta``'s PyArrow scan options.

    The V2 reader sets its own batch size, readahead and threads, so the
    options that tuned those are ignored, with a warning. Any other option
    would change what is read, so it is rejected rather than dropped.

    Args:
        arrow_parquet_args: ``read_delta``'s ``**arrow_parquet_args``.

    Raises:
        ValueError: If an option other than the ignored ones is passed.
    """
    if "filter" in arrow_parquet_args:
        raise ValueError(
            "`filter=` on `read_delta` is not supported. Use "
            "`ray.data.read_delta(path).filter(expr=expr)` instead."
        )
    unsupported = sorted(set(arrow_parquet_args) - _IGNORED_SCAN_OPTIONS)
    if unsupported:
        raise ValueError(
            f"`read_delta` doesn't take the PyArrow scan options {unsupported}. "
            f"It accepts only {sorted(_IGNORED_SCAN_OPTIONS)}, and ignores them."
        )
    if arrow_parquet_args:
        logger.warning(
            "`read_delta` ignores the PyArrow scan options %s: the reader sets "
            "its own batch size, readahead and threads.",
            sorted(arrow_parquet_args),
        )


@dataclass(frozen=True)
class DeltaTableInfo:
    """What the driver needs from one version of a Delta table.

    Unlike a ``deltalake.DeltaTable``, it can be pickled, so the task that
    opens the table returns it.

    Attributes:
        table_uri: The table URI, as ``DeltaTable.table_uri`` reports it.
        version: The version the table was opened at.
        schema: The table schema.
        partition_columns: The partition columns.
    """

    table_uri: str
    version: int
    schema: pa.Schema
    partition_columns: Tuple[str, ...]

    @classmethod
    def from_table(
        cls, table: "DeltaTable", *, include_paths: bool = False
    ) -> "DeltaTableInfo":
        """Describe ``table``, opened at the version to read.

        Args:
            table: The table.
            include_paths: The read's ``include_paths`` argument.

        Returns:
            The table's description.

        Raises:
            Exception: What ``check_delta_table_is_readable`` raises.
        """
        check_delta_table_is_readable(table, include_paths=include_paths)
        return cls(
            table_uri=table.table_uri,
            version=table.version(),
            schema=pa.schema(table.schema().to_arrow()),
            partition_columns=tuple(table.metadata().partition_columns),
        )


def _open_and_load_table(
    table_uri: str,
    version: Optional[int],
    storage_options: Optional[Dict[str, Any]],
    partition_filters: Optional[List[Any]],
    include_paths: bool,
) -> Tuple[DeltaTableInfo, pa.Table]:
    """Open and check the table, and load its ``add`` actions."""
    from deltalake import DeltaTable

    from ray.data.read_api import _delta_partition_filter_kwargs

    table = DeltaTable(table_uri, version=version, storage_options=storage_options)
    info = DeltaTableInfo.from_table(table, include_paths=include_paths)
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
) -> Tuple[DeltaTableInfo, "ObjectRef[pa.Table]"]:
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
        The table's description, and a reference to its ``add`` actions.

    Raises:
        Exception: What opening or checking the table raised, such as
            ``deltalake``'s ``TableNotFoundError``, what
            ``check_delta_table_is_readable`` raises, or a ``ValueError`` for
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
