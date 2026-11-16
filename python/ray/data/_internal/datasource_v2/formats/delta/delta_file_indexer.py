"""Listing for Delta Lake tables.

A Delta table's data files are the ones its transaction log names at a given
version, not whatever a directory walk finds: files of removed versions,
uncommitted writes and the ``_delta_log`` directory share the table root. So the
indexers here read the file list from the log, at the version the driver pinned,
and hand it to the regular listing machinery.

Each file's partition values come from the log too, not from its path: Delta
doesn't require partition directories, and it stores a null value and the
string ``__HIVE_DEFAULT_PARTITION__`` in the same directory. The listing
carries them to the reader in a manifest column (``PARTITION_VALUES_COLUMN``).
"""

from __future__ import annotations

import logging
import posixpath
import urllib.parse
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
)

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.common.partitioning_utils import (
    _evaluate_path_predicate,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
from ray.data.datasource.partitioning import Partitioning, PartitionStyle

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from pyarrow.fs import FileSystem

    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# Columns of ``DeltaTable.get_add_actions(flatten=True)``.
ADD_ACTION_PATH_COLUMN = "path"
ADD_ACTION_SIZE_COLUMN = "size_bytes"
ADD_ACTION_PARTITION_PREFIX = "partition."

# The manifest column that carries each file's partition values from the log
# to the reader: a struct with one string field per partition column.
PARTITION_VALUES_COLUMN = "__delta_partition_values"

# One file's partition values, by column, as ``partition_values_as_strings``
# writes them. ``None`` is a null value.
PartitionValues = Dict[str, Optional[str]]


@dataclass(frozen=True)
class DeltaTableSnapshot:
    """One version of a Delta table, by reference.

    A ``deltalake.DeltaTable`` wraps a Rust handle that can't be pickled, so
    listing tasks receive this instead and open the table themselves, at the
    version the driver pinned.

    Attributes:
        table_uri: The table URI, as ``DeltaTable.table_uri`` reports it.
        version: The table version to read.
        storage_options: ``deltalake`` storage options, for credentials.
    """

    table_uri: str
    version: int
    storage_options: Optional[Dict[str, Any]] = None

    def open(self) -> "DeltaTable":
        """Open the table at the pinned version."""
        from deltalake import DeltaTable

        return DeltaTable(
            self.table_uri,
            version=self.version,
            storage_options=self.storage_options,
        )

    def add_actions(self) -> pa.Table:
        """The ``add`` actions of the pinned version, one row per data file."""
        # ``deltalake`` returns an arro3-core table, not a PyArrow one;
        # ``pa.table`` converts it through the Arrow C stream interface.
        return pa.table(self.open().get_add_actions(flatten=True))


def resolve_data_file_path(logged_path: str) -> str:
    """Path of a data file relative to the table root, as stored on disk.

    The log stores paths URL-encoded: a file under the partition directory
    ``grp=e%3Df`` is logged as ``grp=e%253Df/...``. Decoding exactly once gives
    the on-disk name, which is what the table-rooted filesystem expects.

    Args:
        logged_path: The ``path`` of an ``add`` action.

    Returns:
        The decoded path, relative to the table root.

    Raises:
        ValueError: If the path is absolute. The Delta protocol allows that
            (for example for shallow clones), but the DataSourceV2 reader reads
            through a filesystem rooted at the table.
    """
    if posixpath.isabs(logged_path) or _has_scheme(logged_path):
        raise ValueError(
            f"The Delta log names a data file by absolute path ({logged_path!r}). "
            "The DataSourceV2 reader for `read_delta` only reads files under the "
            "table directory. Set `DataContext.use_delta_datasource_v2 = False` "
            "to read this table with the V1 reader."
        )
    return urllib.parse.unquote(logged_path)


def _has_scheme(path: str) -> bool:
    # A one-letter "scheme" is a Windows drive letter, and a colon inside a
    # partition value (``ts=10:30``) never parses as a scheme because the text
    # before it holds ``=``.
    return len(urllib.parse.urlparse(path).scheme) > 1


def make_partitioning(partition_columns: Sequence[str]) -> Optional[Partitioning]:
    """Partitioning that names a Delta table's partition columns.

    The reader uses it to tell partition columns from data columns. Their
    values come from the log (``PARTITION_VALUES_COLUMN``), not from the path.

    Args:
        partition_columns: The table's partition columns, in log order.

    Returns:
        The ``Partitioning``, or ``None`` for an unpartitioned table.
    """
    if not partition_columns:
        return None
    return Partitioning(PartitionStyle.HIVE, field_names=list(partition_columns))


def is_supported_partition_type(data_type: pa.DataType) -> bool:
    """Whether ``partition_values_as_strings`` round-trips ``data_type``.

    The reader casts each value back from the string, so only types whose
    Arrow string form casts back exactly are supported.
    """
    return (
        pa.types.is_integer(data_type)
        or pa.types.is_floating(data_type)
        or pa.types.is_decimal(data_type)
        or pa.types.is_string(data_type)
        or pa.types.is_large_string(data_type)
        or pa.types.is_boolean(data_type)
        or pa.types.is_date(data_type)
        or pa.types.is_timestamp(data_type)
    )


def partition_values_as_strings(
    add_actions: pa.Table, partition_schema: pa.Schema
) -> List[PartitionValues]:
    """Each file's partition values from the log, in the reader's string form.

    The reader casts a partition value from a string to the column's type, as
    it does for a value parsed from a path. ``deltalake`` parses the log's
    values by type, so cast them to the table's types and back to strings with
    Arrow, whose cast from that string form returns the value exactly
    (``'true'``, ``'2024-01-02 03:04:05.000006Z'``, ...).

    Args:
        add_actions: The ``add`` actions, one row per data file.
        partition_schema: The table's partition columns and their types.

    Returns:
        One mapping per ``add`` action, in the same order.
    """
    columns = {}
    for field in partition_schema:
        logged = add_actions[ADD_ACTION_PARTITION_PREFIX + field.name]
        columns[field.name] = logged.cast(field.type).cast(pa.string()).to_pylist()
    return [
        {name: values[i] for name, values in columns.items()}
        for i in range(add_actions.num_rows)
    ]


def partition_values_type(partition_schema: pa.Schema) -> pa.StructType:
    """Type of ``PARTITION_VALUES_COLUMN``: one string per partition column.

    Every manifest gets the same type, so the bin packer can merge rows from
    different manifests.
    """
    return pa.struct([pa.field(field.name, pa.string()) for field in partition_schema])


class DeltaPartitionPruner(FilePruner):
    """Leaves out files whose partition values in the log fail a predicate.

    Its ``should_include`` keeps every file: a path doesn't carry the values.
    The Delta indexers call ``should_include_values`` instead.
    """

    def __init__(self, predicate: "Expr", schema: Optional[pa.Schema]):
        """Create the pruner.

        Args:
            predicate: The pushed-down predicate on partition columns.
            schema: The table schema, which gives the partition columns' types.
        """
        self.predicate = predicate
        self.schema = schema

    def should_include(self, path: str) -> bool:
        return True

    def should_include_values(self, values: Mapping[str, Any], path: str) -> bool:
        """Whether a file with these partition values may hold matching rows.

        Args:
            values: The file's partition values, as ``partition_values_as_strings``
                gives them.
            path: The file's path, for error messages.

        Returns:
            ``False`` only if the predicate is false for these values.
        """
        return (
            _evaluate_path_predicate(
                self.predicate, dict(values), self.schema, path=path
            )
            is not False
        )


class _DeltaLogListing:
    """Lists the data files the Delta log names, instead of walking directories.

    Mixed into an indexer ahead of the base class whose ``list_files`` then
    processes the listed files (footer reads, shuffle, excluded read units).
    """

    _snapshot: DeltaTableSnapshot
    _filesystem: "FileSystem"
    _partition_schema: pa.Schema
    _shuffle_config: Optional["FileShuffleConfig"]
    _partition_values_by_path: Dict[str, PartitionValues]

    def _init_delta_listing(
        self,
        *,
        snapshot: DeltaTableSnapshot,
        filesystem: "FileSystem",
        partition_schema: pa.Schema,
        shuffle_config: Optional["FileShuffleConfig"] = None,
    ) -> None:
        self._snapshot = snapshot
        # The framework passes ``filesystem=None`` and no file shuffle to a
        # metadata-backed source's indexer, so the table's filesystem and the
        # read's shuffle travel with the indexer.
        self._filesystem = filesystem
        self._partition_schema = partition_schema
        self._shuffle_config = shuffle_config
        # Filled by ``list_file_infos``, in the listing task.
        self._partition_values_by_path = {}

    def list_file_infos(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"] = None,
        pruners: Optional[List["FilePruner"]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        """Yield the table's data files at the pinned version.

        ``paths`` holds the table label from ``DeltaDatasourceV2.paths`` and is
        not read. Paths are relative to the table root and sorted, so a seeded
        shuffle and ``preserve_order`` don't depend on the log's order.
        Zero-size files are skipped and ``pruners`` applied, as the directory
        listing does; a ``DeltaPartitionPruner`` gets each file's partition
        values from the log.
        """
        add_actions = self._snapshot.add_actions()
        file_paths = [
            resolve_data_file_path(path)
            for path in add_actions[ADD_ACTION_PATH_COLUMN].to_pylist()
        ]
        self._partition_values_by_path = dict(
            zip(
                file_paths,
                partition_values_as_strings(add_actions, self._partition_schema),
            )
        )
        file_infos = sorted(
            (
                FileInfo(path=path, size=size)
                for path, size in zip(
                    file_paths, add_actions[ADD_ACTION_SIZE_COLUMN].to_pylist()
                )
            ),
            key=lambda info: info.path,
        )
        logger.debug(
            "Delta table %s version %d has %d data files",
            self._snapshot.table_uri,
            self._snapshot.version,
            len(file_infos),
        )
        yield from self._filter_file_infos(file_infos, pruners or [])

    def _filter_file_infos(
        self, file_infos: Iterable[FileInfo], pruners: List["FilePruner"]
    ) -> Iterator[FileInfo]:
        for info in file_infos:
            if not info.size:
                logger.warning(f"Skipping zero-size file: {info.path!r}")
                continue
            if not all(self._pruner_includes(pruner, info.path) for pruner in pruners):
                continue
            yield info

    def _pruner_includes(self, pruner: FilePruner, path: str) -> bool:
        if isinstance(pruner, DeltaPartitionPruner):
            return pruner.should_include_values(
                self._partition_values_by_path[path], path
            )
        return pruner.should_include(path)

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"] = None,
        **kwargs: Any,
    ) -> Iterator[FileManifest]:
        """List manifests through the base class, from ``list_file_infos``.

        For a partitioned table, each manifest row also carries its file's
        partition values from the log, in ``PARTITION_VALUES_COLUMN``.
        """
        self._default_shuffle(kwargs)
        # pyrefly: ignore[missing-attribute]
        manifests = super().list_files(paths, filesystem=self._filesystem, **kwargs)
        yield from self._with_partition_values(manifests)

    def _with_partition_values(
        self, manifests: Iterable[FileManifest]
    ) -> Iterator[FileManifest]:
        # Added after the base listing, whose row-group shuffle rebuilds its
        # rows from the required columns only.
        if len(self._partition_schema) == 0:
            yield from manifests
            return
        values_type = partition_values_type(self._partition_schema)
        for manifest in manifests:
            block = manifest.as_block()
            # Listing manifests are always built as Arrow tables.
            assert isinstance(block, pa.Table)
            values = pa.array(
                [
                    self._partition_values_by_path[path]
                    for path in manifest.paths.tolist()
                ],
                type=values_type,
            )
            yield FileManifest(
                block.append_column(
                    pa.field(PARTITION_VALUES_COLUMN, values_type), values
                )
            )

    def _default_shuffle(self, list_files_kwargs: Dict[str, Any]) -> None:
        if list_files_kwargs.get("shuffle_config") is None:
            list_files_kwargs["shuffle_config"] = self._shuffle_config


class DeltaFileIndexer(_DeltaLogListing, FooterFileIndexer):
    """Lists a Delta table's data files from its log, then reads their footers.

    Each file's Parquet footer is read on ``FooterFileIndexer``'s actor pool,
    so the listing emits one row per row-group run with exact stats: row groups
    are pruned by the pushed-down predicate, split into read units and skipped
    when a resumed job already finished them. For a partitioned table, each row
    also carries its file's partition values from the log, in
    ``PARTITION_VALUES_COLUMN``.
    """

    def __init__(
        self,
        *,
        snapshot: DeltaTableSnapshot,
        filesystem: "FileSystem",
        partition_schema: pa.Schema,
        shuffle_config: Optional["FileShuffleConfig"] = None,
        coalesce_bytes: int = 0,
        small_shuffle_chunk_size: int = 0,
    ):
        FooterFileIndexer.__init__(
            self,
            ignore_missing_paths=False,
            coalesce_bytes=coalesce_bytes,
            small_shuffle_chunk_size=small_shuffle_chunk_size,
        )
        self._init_delta_listing(
            snapshot=snapshot,
            filesystem=filesystem,
            partition_schema=partition_schema,
            shuffle_config=shuffle_config,
        )

    @override
    def as_whole_file_indexer(self) -> "DeltaWholeFileIndexer":
        """A log-based listing that emits each data file once.

        ``FooterFileIndexer.as_whole_file_indexer`` would walk the table
        directory, which also holds files the pinned version doesn't include.
        A metadata-only read needn't shuffle.
        """
        return DeltaWholeFileIndexer(
            snapshot=self._snapshot,
            filesystem=self._filesystem,
            partition_schema=self._partition_schema,
            max_paths_per_output=self._max_paths_per_output,
        )


class DeltaWholeFileIndexer(_DeltaLogListing, NonSamplingFileIndexer):
    """Lists a Delta table's data files from its log, one row per file.

    Used for metadata-only reads such as a pushed-down ``count()``.
    """

    def __init__(
        self,
        *,
        snapshot: DeltaTableSnapshot,
        filesystem: "FileSystem",
        partition_schema: pa.Schema,
        max_paths_per_output: Optional[int] = None,
    ):
        NonSamplingFileIndexer.__init__(
            self,
            ignore_missing_paths=False,
            max_paths_per_output=max_paths_per_output,
        )
        self._init_delta_listing(
            snapshot=snapshot,
            filesystem=filesystem,
            partition_schema=partition_schema,
        )

    @override
    def as_whole_file_indexer(self) -> "DeltaWholeFileIndexer":
        return self
