"""File listing driven by the Delta Lake transaction log."""

import logging
import posixpath
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional
from urllib.parse import unquote, urlparse

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

    Predicates arrive per call, as the listing-time pushdown state that
    :class:`~ray.data._internal.logical.rules.derive_list_files_pushdown.DeriveListFilesPushdown`
    derives from the consuming ``ReadFiles`` scanner. They are optional: a
    call without them lists the whole snapshot. That is what keeps pruning an
    optimization rather than a correctness requirement -- the scanner
    independently enforces the same predicates while reading, which for
    partition columns depends on ``read_delta`` giving the partitioning a
    ``field_types`` mapping so path-parsed values are typed.
    """

    def __init__(
        self,
        *,
        table_uri: Optional[str] = None,
        version: Optional[int] = None,
        storage_options: Optional[Dict[str, Any]] = None,
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
            file_chunker: Strategy for splitting a file across read tasks.
                Defaults to :class:`ParquetFileChunker`, matching
                ``read_parquet``; a Delta table of a few large files would
                otherwise get one read task per file.
            max_paths_per_output: Maximum files per emitted manifest block.
        """
        self._table_uri = table_uri
        self._version = version
        self._storage_options = storage_options
        self._file_chunker = (
            file_chunker if file_chunker is not None else ParquetFileChunker()
        )
        self._max_paths_per_output = max_paths_per_output

    @property
    def file_chunker(self) -> FileChunker:
        return self._file_chunker

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
        partition_predicate: Optional["Expr"] = None,
    ) -> Iterable[FileManifest]:
        """Yield manifests for the files this query may need.

        ``preserve_order`` needs no special handling: the add actions of a
        given snapshot are read in a fixed order, so listing is already
        deterministic.

        ``predicate`` and ``partition_predicate`` are the listing-time pushdown
        state derived by
        :class:`~ray.data._internal.logical.rules.derive_list_files_pushdown.DeriveListFilesPushdown`,
        already split by column: the former binds data columns, the latter
        partition columns. Both are answerable from the log, so both prune.

        ``limit`` and ``projected_columns`` are ignored. Early-stop listing
        would have to know each file's row count survives the predicate, and
        the log's ``num_records`` is exact only when no data predicate applies;
        rather than special-case that, leave the limit to the reader.
        """
        yield from build_manifests(
            self.list_file_infos(
                paths,
                filesystem=filesystem,
                pruners=pruners,
                preserve_order=preserve_order,
                predicate=predicate,
                partition_predicate=partition_predicate,
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
        predicate: Optional["Expr"] = None,
        partition_predicate: Optional["Expr"] = None,
    ) -> Iterable[FileInfo]:
        """Yield pruned, non-empty ``FileInfo``\\ s from the transaction log.

        Mirrors :meth:`NonSamplingFileIndexer.list_file_infos`: zero-size files
        are dropped and ``pruners`` applied here, so this and :meth:`list_files`
        share one filtering point. The log-level pruning that makes this indexer
        worthwhile happens upstream of both, in :meth:`_iter_file_infos`.

        The two predicate arguments are an extension of the base signature:
        callers that reach an indexer directly (rather than through
        ``list_files``) can still prune. They default to ``None``, so the base
        contract is unchanged.
        """
        pruners = pruners or []
        for file_info in self._iter_file_infos(
            paths, predicate=predicate, partition_predicate=partition_predicate
        ):
            if file_info.size is None or file_info.size == 0:
                logger.warning(f"Skipping zero-size file: {file_info.path!r}")
                continue
            if not all(pruner.should_include(file_info.path) for pruner in pruners):
                continue
            yield file_info

    def _iter_file_infos(
        self,
        paths: "BlockColumn",
        *,
        predicate: Optional["Expr"] = None,
        partition_predicate: Optional["Expr"] = None,
    ) -> Iterable[FileInfo]:
        import pyarrow as pa
        from deltalake import DeltaTable

        for resolved_path in paths.to_pylist():
            table = DeltaTable(
                self._table_uri if self._table_uri is not None else resolved_path,
                version=self._version,
                storage_options=self._storage_options,
            )
            add_actions = pa.table(table.get_add_actions(flatten=True))

            # The log's partition values are strings, so comparing them to a
            # predicate's typed literals needs the table schema. Read it off the
            # snapshot we already opened rather than plumbing it in: it is the
            # unprojected schema by construction, which is what the cast needs.
            # ``deltalake`` returns arro3 objects, so this is a real conversion
            # over the Arrow PyCapsule interface, not a redundant wrap.
            table_schema = pa.schema(table.schema().to_arrow())

            total = add_actions.num_rows
            add_actions = prune_add_actions(
                add_actions,
                partition_predicate=partition_predicate,
                data_predicate=predicate,
                table_schema=table_schema,
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


def _resolve_file_uri(table_uri: str, logged_path: str) -> str:
    """Resolve a log-recorded add-action path against the table URI.

    Add actions store the path URL-encoded, while the object it names is not:
    a partition value of ``e=f`` is written to a directory called
    ``grp=e%3Df`` but recorded as ``grp=e%253Df``. Joining the raw value would
    produce a path that doesn't exist, so decode exactly once first. This
    reproduces ``DeltaTable.file_uris()`` without asking for a second listing.

    The decoded path is usually relative to the table root, but the Delta
    protocol also permits an absolute URI or absolute path -- shallow clones
    and externally managed files use that form. Joining one of those onto the
    root would either prefix it (``/table`` + ``s3://bucket/key``) or silently
    drop the root, so absolute paths are returned as-is.
    """
    decoded = unquote(logged_path)
    if _is_absolute_path(decoded):
        return decoded
    return posixpath.join(table_uri.rstrip("/"), decoded)


def _is_absolute_path(path: str) -> bool:
    """Whether a decoded add-action path stands on its own.

    True for a POSIX absolute path and for anything carrying a URI scheme.
    The scheme must be longer than one character so a Windows drive letter
    (``C:/data``) is not mistaken for one; a relative Delta path can contain a
    colon inside a partition value (``key=a:b/part-0.parquet``), but the text
    before it is not a valid scheme, so ``urlparse`` reports none.
    """
    if path.startswith("/"):
        return True
    return len(urlparse(path).scheme) > 1
