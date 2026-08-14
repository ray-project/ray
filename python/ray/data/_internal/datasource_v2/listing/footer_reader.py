from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Iterable, Iterator, NamedTuple

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from pyarrow.parquet import FileMetaData, RowGroupMetaData

import ray
from ray.data._internal.datasource.parquet_datasource import (
    _row_group_uncompressed_size,
)
from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import (
    FileChunks,
    RowGroupInfo,
)
from ray.data._internal.datasource_v2.chunkers.parquet_row_group_coalescing import (
    coalesce_row_groups,
)
from ray.data._internal.planner.plan_expression.expression_visitors import (
    get_column_references,
)
from ray.data._internal.util import call_with_retry

if TYPE_CHECKING:
    import pyarrow.compute as pc

    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


def _prefix_match(path: str, names: set[str]) -> set[str]:
    """Return the names that ``path`` equals or is nested under.

    A leaf matches a name outright (a flat column, whose *name* may itself
    contain dots -- ``sepal.length`` is a real one), or as a descendant of it
    (``outer.a`` under ``outer``). Splitting the leaf on ``.`` and comparing the
    first segment gets the nested case right and the dotted-flat-name case
    silently wrong, so match the whole path first.
    """
    return {n for n in names if path == n or path.startswith(f"{n}.")}


def _leaf_matches(leaf_path: str, names: set[str]) -> bool:
    """Whether a Parquet leaf belongs to any of ``names``."""
    return bool(_prefix_match(leaf_path, names))


class _FilterLeaves(NamedTuple):
    """Where the predicate's columns sit in a file's schema.

    A Parquet file has one schema, so this holds for all of its row groups and
    is resolved once per file. ``all_columns_found`` is false when some
    predicate column has no leaf in the schema at all.
    """

    indices: list[int]
    all_columns_found: bool


class FooterReader:
    """Reads Parquet footers and chunks files into row-group runs.

    Run as a pool of Ray actors (see :data:`FooterReaderActor`) to spread footer
    IO across the cluster instead of bottlenecking on the driver. ``read_footers``
    is a streaming generator that yields ``FileChunks`` in small batches as their
    footers land, so the driver does far fewer object-store fetches than one per
    file.

    Kept as a plain class (the actor is created via the functional
    ``ray.remote(FooterReader)`` form below) so callers can type actor handles as
    ``ActorProxy[FooterReader]`` -- see Ray's type-hint docs
    (https://docs.ray.io/en/latest/ray-core/type-hint.html).
    """

    def __init__(
        self,
        filesystem: pafs.FileSystem,
        io_concurrency: int = 128,
        filter_expr: "Expr" | None = None,
        projected_cols: list[str] | None = None,
        coalesce_bytes: int = 0,
        retried_io_errors: list[str] | None = None,
    ):
        from ray.data.context import DEFAULT_RETRIED_IO_ERRORS

        self.retried_io_errors = (
            retried_io_errors
            if retried_io_errors is not None
            else list(DEFAULT_RETRIED_IO_ERRORS)
        )
        # Coalescing target: merge contiguous row groups into chunks of
        # ~coalesce_bytes uncompressed before returning them, so the driver packs
        # fewer items. 0 disables coalescing (one chunk per physical row group).
        self.coalesce_bytes = coalesce_bytes
        self.filesystem = filesystem
        self.pool = ThreadPoolExecutor(max_workers=io_concurrency)
        # Match Arrow's process-wide pools to the actor's IO concurrency so
        # nested S3/footer work isn't bottlenecked on the default 8 threads.
        pa.set_io_thread_count(io_concurrency)
        # Lower the Ray Data predicate to a native PyArrow compute expression once
        # (per actor) so it can be pushed into split_by_row_group for row-group
        # pruning. ``None`` means "no predicate" -> keep every row group.
        self.filter: "pc.Expression" | None = (
            filter_expr.to_pyarrow() if filter_expr is not None else None
        )
        # Top-level columns the predicate reads. A row group with a null in any
        # of them cannot be an exact-survivor count -- see ``_has_filter_nulls``.
        self.filter_columns: set[str] = (
            set(get_column_references(filter_expr))
            if filter_expr is not None
            else set()
        )
        # Projection pushdown: when set, row-group byte sizes are accounted over
        # only these top-level columns, since the reader will only fetch those.
        self.projected_cols: set[str] | None = (
            set(projected_cols) if projected_cols is not None else None
        )
        # Reused across files; make_fragment is what lets us apply
        # split_by_row_group.
        self.file_format = pds.ParquetFileFormat()

    def _projected_leaf_indices(self, row_group: RowGroupMetaData) -> list[int] | None:
        # Map the requested top-level column names to Parquet leaf-column indices
        # (a nested field expands to several leaves, e.g. "a.b.list.element").
        # Returns None to signal "all columns" so callers can take the cheap path.
        if self.projected_cols is None:
            return None
        indices = [
            j
            for j in range(row_group.num_columns)
            if _leaf_matches(row_group.column(j).path_in_schema, self.projected_cols)
        ]

        return indices or None

    def _row_group_info(
        self,
        row_group: RowGroupMetaData,
        rg_idx: int,
        leaf_indices: list[int] | None,
        fully_matched: bool = False,
    ) -> RowGroupInfo:
        # Sum per-column sizes on both paths -- with a projection, only the
        # projected leaves, so bin packing reflects the bytes the reader will
        # actually pull. Deliberately not ``row_group.total_byte_size``, which
        # is one cheap accessor but can report the *compressed* size for some
        # files (apache/arrow#48138); undersizing bins here overfills read
        # tasks, which is the failure this whole path exists to avoid. Shares
        # the V1 helper so the three call sites cannot drift.
        uncompressed = _row_group_uncompressed_size(row_group, leaf_indices)
        return RowGroupInfo(
            rg_idx=rg_idx,
            uncompressed_size=uncompressed,
            num_rows=row_group.num_rows,
            fully_matched=fully_matched,
        )

    def _locate_filter_columns(self, row_group: RowGroupMetaData) -> _FilterLeaves:
        # Resolve the predicate's column names to Parquet leaf-column indices,
        # the same mapping ``_projected_leaf_indices`` does for the projection.
        # Called once per file so the per-row-group null check below is an
        # index lookup instead of another scan over every leaf.
        if not self.filter_columns:
            return _FilterLeaves([], True)
        indices: list[int] = []
        matched: set[str] = set()
        for j in range(row_group.num_columns):
            hits = _prefix_match(
                row_group.column(j).path_in_schema, self.filter_columns
            )
            if hits:
                matched.update(hits)
                indices.append(j)
        return _FilterLeaves(indices, matched == self.filter_columns)

    def _has_filter_nulls(
        self, metadata: FileMetaData, rg_idx: int, filter_leaves: _FilterLeaves
    ) -> bool:
        """Whether any predicate column may have a null in this row group.

        Parquet min/max statistics are computed over non-null values only, so
        bounds alone can never rule out nulls. A null in a predicate column
        means that row satisfies neither the filter nor its negation, so the
        group's ``num_rows`` is not an exact survivor count.

        Missing or null-count-less statistics are treated as "may contain
        nulls", i.e. not fully matched -- the safe direction.
        """
        # Fail closed on any predicate column we could not locate in the
        # footer. "No leaf matched" means we did not verify it, not that it is
        # null-free -- and treating it as null-free is what lets a partial
        # survivor count as exact.
        if not filter_leaves.all_columns_found:
            return True
        row_group = metadata.row_group(rg_idx)
        for j in filter_leaves.indices:
            stats = row_group.column(j).statistics
            if stats is None or not stats.has_null_count or stats.null_count > 0:
                return True
        return False

    def _read_and_chunk(self, path: str, size: int) -> FileChunks:
        fragment = self.file_format.make_fragment(path, filesystem=self.filesystem)
        # ``make_fragment`` is lazy, so this property is the footer read itself
        # (and caches it on the fragment). It raises on a truncated, non-Parquet,
        # or since-deleted file -- terminal conditions that must fail the read
        # rather than silently drop the file's rows, since the reader would fail
        # on the same file anyway. Transient remote-storage errors are the
        # exception and get retried, matching what the read side does per
        # fragment; a failed attempt caches nothing, so retrying re-reads.
        metadata = call_with_retry(
            lambda: fragment.metadata,
            description=f"read Parquet footer for {path}",
            match=self.retried_io_errors,
        )
        # Both column look-ups are schema-derived, and a Parquet file has a
        # single schema, so resolve them once against row group 0 rather than
        # rescanning every leaf for each group.
        if metadata.num_row_groups:
            first_row_group = metadata.row_group(0)
            leaf_indices = self._projected_leaf_indices(first_row_group)
            filter_leaves = self._locate_filter_columns(first_row_group)
        else:
            leaf_indices = None
            filter_leaves = _FilterLeaves([], False)

        if self.filter is not None:
            # Predicate pushdown: drop row groups whose Parquet statistics
            # contradict the filter, so we never emit chunks the reader would
            # skip anyway. Each returned fragment views a single surviving group.
            surviving = fragment.split_by_row_group(self.filter)
            rg_indices: Iterable[int] = [sub.row_groups[0].id for sub in surviving]
            # Classify surviving groups as fully- vs partially-matching via
            # predicate negation: a group is fully matched iff
            # ~filter cannot match any of its rows, i.e. the negation prunes it
            # out. For those, num_rows is an exact survivor count and can drive
            # the limit push-down. Any failure defaults to "not fully matched",
            # which is always safe (falls back to the post-filter stop).
            try:
                not_fully = {
                    sub.row_groups[0].id
                    for sub in fragment.split_by_row_group(~self.filter)
                }
            except Exception as e:
                logger.debug(
                    "Error splitting by row group: %s for file %s",
                    e,
                    path,
                    exc_info=True,
                )
                not_fully = set(rg_indices)
            # The negation test alone is not sufficient under three-valued
            # logic. A row whose filter column is null satisfies neither
            # ``filter`` nor ``~filter``, so a group of [35, 40, 45, NULL] under
            # ``id >= 30`` can have ``~filter`` prune it while one row did not
            # actually survive -- ``num_rows`` would then over-count and limit
            # push-down would stop early and under-deliver.
            #
            # PyArrow happens to decline that prune today (it does not use
            # ``null_count`` to sharpen the inverted predicate), so the negation
            # test is currently safe by accident. Do not rely on that: check
            # ``null_count`` directly, which holds regardless of how clever the
            # engine's statistics pruning gets.
            fully_by_idx: dict[int, bool] = {
                i: i not in not_fully
                and not self._has_filter_nulls(
                    metadata=metadata, rg_idx=i, filter_leaves=filter_leaves
                )
                for i in rg_indices
            }
        else:
            rg_indices = range(metadata.num_row_groups)
            # No predicate -> nothing to disqualify a group, so the lookup below
            # falls through to its fully-matched default for every group.
            fully_by_idx = {}

        per_rg = [
            self._row_group_info(
                row_group=metadata.row_group(rg_idx),
                rg_idx=rg_idx,
                leaf_indices=leaf_indices,
                fully_matched=fully_by_idx.get(rg_idx, True),
            )
            for rg_idx in rg_indices
        ]
        # Coalesce contiguous row groups into ~coalesce_bytes chunks (no-op when
        # coalesce_bytes == 0) so only a handful of descriptors per file reach the
        # driver's bin-packer instead of one per physical row group.
        row_groups = coalesce_row_groups(per_rg, self.coalesce_bytes)
        return FileChunks(path=path, size=size, row_groups=row_groups)

    @ray.method(num_returns="streaming")
    def read_footers(
        self,
        files: list[tuple[str, int]],
        *,
        result_batch_size: int = 1,
        preserve_order: bool = False,
    ) -> Iterator[list[FileChunks]]:
        """Read the footers of ``files`` concurrently, yielding ``FileChunks``.

        Yields lists of ``FileChunks`` (not single results): each list the driver
        receives costs one object-store fetch, so batching cuts driver-side
        deserialization overhead ~``result_batch_size``-fold. At the default of
        ``1`` a directory of N files costs N fetches on the single listing task;
        callers set it via ``RAY_DATA_PARQUET_FOOTER_RESULT_BATCH_SIZE``.
        ``num_returns`` is fixed to ``"streaming"`` via ``@ray.method`` so
        results stream out as footers land.

        ``preserve_order`` yields in ``files`` order rather than completion order.
        The reads are all submitted up front either way, so this delays only the
        yield of a footer that landed behind a slower one, never the read itself.
        """
        futures = [
            self.pool.submit(self._read_and_chunk, path, size) for path, size in files
        ]
        # Completion order depends on IO timing, which decides how the driver's
        # bin packer groups row groups into read tasks and -- under a pushed-down
        # limit -- which files it reads before stopping. Walking ``futures``
        # directly makes both a function of the listing order instead.
        ordered: Iterable[Future[FileChunks]] = (
            futures if preserve_order else as_completed(futures)
        )
        buffer: list[FileChunks] = []
        total_row_groups = 0
        for finished in ordered:
            chunk = finished.result()
            if not chunk.row_groups:
                # Only occurs if the file is empty or all row groups are filtered out.
                continue
            total_row_groups += len(chunk.row_groups)
            buffer.append(chunk)
            if len(buffer) >= result_batch_size:
                yield buffer
                buffer = []
        if buffer:
            yield buffer
        logger.debug(
            "FooterReader batch: %d files, %d row groups",
            len(files),
            total_row_groups,
        )


# The Ray actor class. Built via the functional ``ray.remote(...)`` form (rather
# than the ``@ray.remote`` decorator) so ``FooterReader`` stays a plain class and
# actor handles can be typed ``ActorProxy[FooterReader]``.
FooterReaderActor = ray.remote(FooterReader)
