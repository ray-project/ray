"""File listing driven by the Delta Lake transaction log."""

import logging
import posixpath
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional
from urllib.parse import unquote

from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    FileChunker,
    ParquetFileChunker,
)
from ray.data._internal.datasource_v2.listing.delta_file_pruning import (
    prune_add_actions,
)
from ray.data._internal.datasource_v2.listing.file_indexer import (
    FileIndexer,
    FileInfo,
    build_manifests,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
from ray.data.block import BlockColumn
from ray.data.expressions import Expr

if TYPE_CHECKING:
    import pyarrow as pa
    from pyarrow.fs import FileSystem

logger = logging.getLogger(__name__)

# Matches ``NonSamplingFileIndexer``'s batching so downstream partitioning
# behaves the same for Delta reads as for plain Parquet reads.
_DEFAULT_MAX_PATHS_PER_OUTPUT = 1000


class DeltaFileIndexer(FileIndexer):
    """Lists a Delta table's data files from its transaction log.

    Unlike :class:`NonSamplingFileIndexer`, this never walks the filesystem:
    the log already records every file's path, size and row count, plus the
    partition values and column statistics needed to decide which files a
    query can skip. Pruning therefore happens *before* a file is listed, so
    skipped files are never sized, chunked, or scheduled.

    Predicates are optional: an indexer without them lists the whole
    snapshot. That is what keeps pruning an optimization rather than a
    correctness requirement -- the scanner independently enforces the same
    predicates while reading (see
    :class:`~ray.data._internal.logical.rules.delta_file_pruning_pushdown.PushdownDeltaFilePruning`),
    which for partition columns depends on ``read_delta`` giving the
    partitioning a ``field_types`` mapping so path-parsed values are typed.
    """

    def __init__(
        self,
        *,
        table_uri: Optional[str] = None,
        version: Optional[int] = None,
        storage_options: Optional[Dict[str, Any]] = None,
        partition_predicate: Optional[Expr] = None,
        data_predicate: Optional[Expr] = None,
        table_schema: Optional["pa.Schema"] = None,
        file_chunker: Optional[FileChunker] = None,
        max_paths_per_output: int = _DEFAULT_MAX_PATHS_PER_OUTPUT,
    ):
        """Build an indexer for one Delta table snapshot.

        Args:
            table_uri: Table location as the user gave it, scheme included.
                ``deltalake`` resolves its own storage backend from the URI,
                while the paths this indexer emits must be filesystem-native
                (PyArrow rejects a scheme-prefixed path). When ``None``, the
                listed path is used for both, which is only equivalent for
                local paths.
            version: Table version to read. ``None`` reads the latest.
            storage_options: Backend credentials/config passed to ``deltalake``.
            partition_predicate: Predicate over partition columns only.
            data_predicate: Predicate over data columns, applied against the
                log's min/max statistics.
            table_schema: Arrow schema of the table, used to cast partition
                values (strings in the log) before comparing them.
            file_chunker: Strategy for splitting a file across read tasks.
                Defaults to :class:`ParquetFileChunker`, matching
                ``read_parquet``; a Delta table of a few large files would
                otherwise get one read task per file.
            max_paths_per_output: Maximum files per emitted manifest block.
        """
        self._table_uri = table_uri
        self._version = version
        self._storage_options = storage_options
        self._partition_predicate = partition_predicate
        self._data_predicate = data_predicate
        self._table_schema = table_schema
        self._file_chunker = (
            file_chunker if file_chunker is not None else ParquetFileChunker()
        )
        self._max_paths_per_output = max_paths_per_output

    @property
    def file_chunker(self) -> FileChunker:
        return self._file_chunker

    def with_predicates(
        self,
        *,
        partition_predicate: Optional[Expr],
        data_predicate: Optional[Expr],
        table_schema: Optional["pa.Schema"],
    ) -> "DeltaFileIndexer":
        """Return a copy carrying the given predicates.

        Returns a new instance rather than mutating: a logical operator can
        be optimized more than once, and an indexer that accumulated
        predicates in place would prune against a stale plan.
        """
        return DeltaFileIndexer(
            table_uri=self._table_uri,
            version=self._version,
            storage_options=self._storage_options,
            partition_predicate=partition_predicate,
            data_predicate=data_predicate,
            table_schema=table_schema,
            file_chunker=self._file_chunker,
            max_paths_per_output=self._max_paths_per_output,
        )

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
        predicate: Optional["Expr"] = None,
        limit: Optional[int] = None,
        projected_columns: Optional[List[str]] = None,
    ) -> Iterable[FileManifest]:
        """Yield manifests for the files this query may need.

        ``preserve_order`` needs no special handling: the add actions of a
        given snapshot are read in a fixed order, so listing is already
        deterministic.

        ``predicate`` / ``limit`` / ``projected_columns`` are the listing-time
        pushdown state derived by
        :class:`~ray.data._internal.logical.rules.derive_list_files_pushdown.DeriveListFilesPushdown`.
        They are ignored here: this indexer prunes from the predicates it was
        constructed with (see :meth:`with_predicates`), which are already split
        into partition-only and data-column halves. Consuming the derived state
        instead would let the custom rule be dropped -- see the note on the PR.
        """
        yield from build_manifests(
            self.list_file_infos(
                paths,
                filesystem=filesystem,
                pruners=pruners,
                preserve_order=preserve_order,
            ),
            file_chunker=self._file_chunker,
            max_paths_per_output=self._max_paths_per_output,
        )

    def list_file_infos(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        """Yield pruned, non-empty ``FileInfo``\\ s from the transaction log.

        Mirrors :meth:`NonSamplingFileIndexer.list_file_infos`: zero-size files
        are dropped and ``pruners`` applied here, so this and :meth:`list_files`
        share one filtering point. The log-level pruning that makes this indexer
        worthwhile happens upstream of both, in :meth:`_iter_file_infos`.
        """
        pruners = pruners or []
        for file_info in self._iter_file_infos(paths):
            if file_info.size is None or file_info.size == 0:
                logger.warning(f"Skipping zero-size file: {file_info.path!r}")
                continue
            if not all(pruner.should_include(file_info.path) for pruner in pruners):
                continue
            yield file_info

    def _iter_file_infos(self, paths: "BlockColumn") -> Iterable[FileInfo]:
        import pyarrow as pa
        from deltalake import DeltaTable

        for resolved_path in paths.to_pylist():
            table = DeltaTable(
                self._table_uri if self._table_uri is not None else resolved_path,
                version=self._version,
                storage_options=self._storage_options,
            )
            add_actions = pa.table(table.get_add_actions(flatten=True))

            total = add_actions.num_rows
            add_actions = prune_add_actions(
                add_actions,
                partition_predicate=self._partition_predicate,
                data_predicate=self._data_predicate,
                table_schema=self._table_schema,
            )
            if add_actions.num_rows < total:
                logger.debug(
                    "Delta log pruning kept %d of %d files in %r",
                    add_actions.num_rows,
                    total,
                    resolved_path,
                )

            relative_paths = add_actions.column("path").to_pylist()
            sizes = add_actions.column("size_bytes").to_pylist()
            for relative_path, size in zip(relative_paths, sizes):
                yield FileInfo(
                    path=_resolve_file_uri(resolved_path, relative_path), size=size
                )


def _resolve_file_uri(table_uri: str, relative_path: str) -> str:
    """Join a log-recorded relative path onto the table URI.

    Add actions store the path URL-encoded, while the object it names is not:
    a partition value of ``e=f`` is written to a directory called
    ``grp=e%3Df`` but recorded as ``grp=e%253Df``. Joining the raw value would
    produce a path that doesn't exist, so decode exactly once first. This
    reproduces ``DeltaTable.file_uris()`` without asking for a second listing.
    """
    return posixpath.join(table_uri.rstrip("/"), unquote(relative_path))
