"""Listing for Delta Lake tables.

A Delta table's data files are the ones its transaction log names at a given
version, not whatever a directory walk finds: files of removed versions,
uncommitted writes and the ``_delta_log`` directory share the table root. So the
indexers here read the file list from the log, at the version the driver pinned,
and hand it to the regular listing machinery.
"""

from __future__ import annotations

import logging
import posixpath
import urllib.parse
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
)

import pyarrow as pa
from typing_extensions import override

from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.delta.delta_file_pruning import (
    statistics_keep_mask,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data.datasource.partitioning import (
    HIVE_DEFAULT_PARTITION,
    Partitioning,
    PartitionStyle,
    PathPartitionParser,
)

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from pyarrow.fs import FileSystem

    from ray.data._internal.datasource_v2.interfaces.file_manifest import (
        FileManifest,
    )
    from ray.data._internal.datasource_v2.interfaces.file_pruner import FilePruner
    from ray.data.block import BlockColumn
    from ray.data.datasource.file_based_datasource import FileShuffleConfig
    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)

# Columns of ``DeltaTable.get_add_actions(flatten=True)``.
ADD_ACTION_PATH_COLUMN = "path"
ADD_ACTION_SIZE_COLUMN = "size_bytes"
ADD_ACTION_PARTITION_PREFIX = "partition."


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
    """Partitioning that parses a Delta table's partition directories.

    Delta writes Hive-style directories in ``partitionColumns`` order, with
    ``__HIVE_DEFAULT_PARTITION__`` for a null value.

    Args:
        partition_columns: The table's partition columns, in log order.

    Returns:
        The ``Partitioning``, or ``None`` for an unpartitioned table.
    """
    if not partition_columns:
        return None
    return Partitioning(
        PartitionStyle.HIVE,
        field_names=list(partition_columns),
        null_fallback=HIVE_DEFAULT_PARTITION,
    )


def _check_partition_directories(
    parser: PathPartitionParser, path: str, partition_columns: AbstractSet[str]
) -> None:
    """Fail unless ``path`` sits under Hive directories for every partition column.

    The reader takes partition values from the path. The Delta protocol doesn't
    require partition directories, so a file written without them would
    otherwise read back with its partition columns missing.
    """
    try:
        found = set(parser(path))
    except ValueError:
        found = set()
    if found != partition_columns:
        raise ValueError(
            f"Delta data file {path!r} isn't under Hive-style directories for the "
            f"table's partition columns {sorted(partition_columns)}. The DataSourceV2 "
            "reader for `read_delta` reads partition values from those "
            "directories. Set `DataContext.use_delta_datasource_v2 = False` to "
            "read this table with the V1 reader."
        )


def _check_no_literal_null_partition_values(
    add_actions: pa.Table, partition_columns: Sequence[str]
) -> None:
    """Fail if a partition value is the string Delta writes for a null value.

    Delta stores a null partition value in a directory named
    ``__HIVE_DEFAULT_PARTITION__``. A file whose value is that exact string goes
    into the same directory, so only the log tells the two apart. The reader
    takes partition values from the directories, so it would read that string
    back as null.
    """
    for column in partition_columns:
        name = ADD_ACTION_PARTITION_PREFIX + column
        if name not in add_actions.column_names:
            continue
        values = add_actions[name]
        if not (
            pa.types.is_string(values.type) or pa.types.is_large_string(values.type)
        ):
            continue
        if values.index(HIVE_DEFAULT_PARTITION).as_py() != -1:
            raise ValueError(
                f"The Delta table's partition column {column!r} holds the string "
                f"{HIVE_DEFAULT_PARTITION!r}. Delta stores null partition values "
                "in a directory with that name, so the DataSourceV2 reader for "
                "`read_delta`, which reads partition values from the directory "
                "names, can't tell the string from null. Set "
                "`DataContext.use_delta_datasource_v2 = False` to read this table "
                "with the V1 reader."
            )


class _DeltaLogListing:
    """Lists the data files the Delta log names, instead of walking directories.

    Mixed into an indexer ahead of the base class whose ``list_files`` then
    processes the listed files (footer reads, shuffle, excluded read units).
    """

    _snapshot: DeltaTableSnapshot
    _filesystem: "FileSystem"
    _partition_columns: tuple
    _shuffle_config: Optional["FileShuffleConfig"]

    def _init_delta_listing(
        self,
        *,
        snapshot: DeltaTableSnapshot,
        filesystem: "FileSystem",
        partition_columns: Sequence[str],
        shuffle_config: Optional["FileShuffleConfig"] = None,
    ) -> None:
        self._snapshot = snapshot
        # The framework passes ``filesystem=None`` and no file shuffle to a
        # metadata-backed source's indexer, so the table's filesystem and the
        # read's shuffle travel with the indexer.
        self._filesystem = filesystem
        self._partition_columns = tuple(partition_columns)
        self._shuffle_config = shuffle_config

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
        not read.
        """
        yield from self._log_file_infos(pruners or [], predicate=None)

    def _log_file_infos(
        self, pruners: List["FilePruner"], predicate: Optional["Expr"]
    ) -> Iterator[FileInfo]:
        """The data files of the pinned version that may hold matching rows.

        Files whose log statistics rule out ``predicate`` are left out. Paths
        are relative to the table root and sorted, so a seeded shuffle and
        ``preserve_order`` don't depend on the log's order. Zero-size files are
        skipped and ``pruners`` applied, as the directory listing does.
        """
        add_actions = self._snapshot.add_actions()
        _check_no_literal_null_partition_values(add_actions, self._partition_columns)
        num_files = add_actions.num_rows
        if predicate is not None:
            keep = statistics_keep_mask(add_actions, predicate)
            if keep is not None:
                add_actions = add_actions.filter(keep)
        logger.debug(
            "Delta table %s version %d: %d of %d data files may match %s",
            self._snapshot.table_uri,
            self._snapshot.version,
            add_actions.num_rows,
            num_files,
            predicate,
        )
        file_infos = sorted(
            (
                FileInfo(path=resolve_data_file_path(path), size=size)
                for path, size in zip(
                    add_actions[ADD_ACTION_PATH_COLUMN].to_pylist(),
                    add_actions[ADD_ACTION_SIZE_COLUMN].to_pylist(),
                )
            ),
            key=lambda info: info.path,
        )
        yield from self._filter_file_infos(file_infos, pruners)

    def _filter_file_infos(
        self, file_infos: Iterable[FileInfo], pruners: List["FilePruner"]
    ) -> Iterator[FileInfo]:
        partitioning = make_partitioning(self._partition_columns)
        parser = PathPartitionParser(partitioning) if partitioning else None
        expected_partitions = frozenset(self._partition_columns)
        for info in file_infos:
            if parser is not None:
                _check_partition_directories(parser, info.path, expected_partitions)
            if not info.size:
                logger.warning(f"Skipping zero-size file: {info.path!r}")
                continue
            if not all(pruner.should_include(info.path) for pruner in pruners):
                continue
            yield info

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: Optional["FileSystem"] = None,
        pruners: Optional[List["FilePruner"]] = None,
        predicate: Optional["Expr"] = None,
        **kwargs: Any,
    ) -> Iterable["FileManifest"]:
        # The base class's ``list_files`` would list through
        # ``list_file_infos``, which doesn't see the predicate.
        return self.list_files_from_file_infos(
            self._log_file_infos(pruners or [], predicate),
            predicate=predicate,
            **kwargs,
        )

    def list_files_from_file_infos(
        self,
        file_infos: Iterable[FileInfo],
        *,
        filesystem: Optional["FileSystem"] = None,
        **kwargs: Any,
    ) -> Iterable["FileManifest"]:
        self._default_shuffle(kwargs)
        # pyrefly: ignore[missing-attribute]
        return super().list_files_from_file_infos(
            file_infos, filesystem=self._filesystem, **kwargs
        )

    def _default_shuffle(self, list_files_kwargs: Dict[str, Any]) -> None:
        if list_files_kwargs.get("shuffle_config") is None:
            list_files_kwargs["shuffle_config"] = self._shuffle_config


class DeltaFileIndexer(_DeltaLogListing, FooterFileIndexer):
    """Lists a Delta table's data files from its log, then reads their footers.

    Each file's Parquet footer is read on ``FooterFileIndexer``'s actor pool,
    so the listing emits one row per row-group run with exact stats: row groups
    are pruned by the pushed-down predicate, split into read units and skipped
    when a resumed job already finished them.
    """

    def __init__(
        self,
        *,
        snapshot: DeltaTableSnapshot,
        filesystem: "FileSystem",
        partition_columns: Sequence[str],
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
            partition_columns=partition_columns,
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
            partition_columns=self._partition_columns,
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
        partition_columns: Sequence[str],
        shuffle_config: Optional["FileShuffleConfig"] = None,
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
            partition_columns=partition_columns,
            shuffle_config=shuffle_config,
        )

    @override
    def as_whole_file_indexer(self) -> "DeltaWholeFileIndexer":
        return self
