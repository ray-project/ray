from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Iterable, Iterator, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.fs as pafs
from pyarrow.parquet import RowGroupMetaData

import ray
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

if TYPE_CHECKING:
    import pyarrow.compute as pc

    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


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
        filter_expr: Optional["Expr"] = None,
        projected_cols: Optional[List[str]] = None,
        coalesce_bytes: int = 0,
    ):
        # Coalescing target: merge contiguous row groups into chunks of
        # ~coalesce_bytes uncompressed before returning them, so the driver packs
        # fewer items. 0 disables coalescing (one chunk per physical row group).
        self.coalesce_bytes = coalesce_bytes
        self.filesystem = filesystem
        self.pool = ThreadPoolExecutor(max_workers=io_concurrency)
        # Match Arrow's process-wide pools to the actor's IO concurrency so
        # nested S3/footer work isn't bottlenecked on the default 8 threads.
        pa.set_io_thread_count(io_concurrency)
        pa.set_cpu_count(io_concurrency)
        # Lower the Ray Data predicate to a native PyArrow compute expression once
        # (per actor) so it can be pushed into split_by_row_group for row-group
        # pruning. ``None`` means "no predicate" -> keep every row group.
        self.filter: Optional["pc.Expression"] = (
            filter_expr.to_pyarrow() if filter_expr is not None else None
        )
        # Top-level columns the predicate reads. A row group with a null in any
        # of them cannot be an exact-survivor count -- see ``_has_filter_nulls``.
        self.filter_columns: Set[str] = (
            set(get_column_references(filter_expr))
            if filter_expr is not None
            else set()
        )
        # Projection pushdown: when set, row-group byte sizes are accounted over
        # only these top-level columns, since the reader will only fetch those.
        self.projected_cols: Optional[Set[str]] = (
            set(projected_cols) if projected_cols is not None else None
        )
        # Reused across files; make_fragment is what lets us apply
        # split_by_row_group.
        self.file_format = pds.ParquetFileFormat()

    def _projected_leaf_indices(self, row_group) -> Optional[List[int]]:
        # Map the requested top-level column names to Parquet leaf-column indices
        # (a nested field expands to several leaves, e.g. "a.b.list.element").
        # Returns None to signal "all columns" so callers can take the cheap path.
        if self.projected_cols is None:
            return None
        return [
            j
            for j in range(row_group.num_columns)
            if row_group.column(j).path_in_schema.split(".", 1)[0]
            in self.projected_cols
        ]

    def _row_group_info(
        self,
        row_group: RowGroupMetaData,
        rg_idx: int,
        leaf_indices: Optional[List[int]],
        fully_matched: bool = True,
    ) -> RowGroupInfo:
        if leaf_indices is None:
            # total_byte_size is a single cheap accessor for the whole row group,
            # so we avoid walking columns entirely on the no-projection path.
            uncompressed = row_group.total_byte_size
        else:
            # Sum only the projected leaves so bin packing reflects the bytes the
            # downstream reader will actually pull for this row group.
            uncompressed = sum(
                row_group.column(j).total_uncompressed_size for j in leaf_indices
            )
        return RowGroupInfo(
            rg_idx=rg_idx,
            uncompressed_size=uncompressed,
            num_rows=row_group.num_rows,
            fully_matched=fully_matched,
        )

    def _has_filter_nulls(self, metadata, rg_idx: int) -> bool:
        """Whether any predicate column has a null in this row group.

        Parquet min/max statistics are computed over non-null values only, so
        bounds alone can never rule out nulls. A null in a predicate column
        means that row satisfies neither the filter nor its negation, so the
        group's ``num_rows`` is not an exact survivor count.

        Missing or null-count-less statistics are treated as "may contain
        nulls", i.e. not fully matched -- the safe direction.
        """
        row_group = metadata.row_group(rg_idx)
        for j in range(row_group.num_columns):
            column = row_group.column(j)
            if column.path_in_schema.split(".", 1)[0] not in self.filter_columns:
                continue
            stats = column.statistics
            if stats is None or not stats.has_null_count or stats.null_count > 0:
                return True
        return False

    def _read_and_chunk(self, path: str, size: int) -> FileChunks:
        fragment = self.file_format.make_fragment(path, filesystem=self.filesystem)
        metadata = fragment.metadata  # reads + caches the footer on the fragment
        if self.filter is not None:
            # Predicate pushdown: drop row groups whose Parquet statistics
            # contradict the filter, so we never emit chunks the reader would
            # skip anyway. Each returned fragment views a single surviving group.
            surviving = fragment.split_by_row_group(self.filter)
            rg_indices: Iterable[int] = [sub.row_groups[0].id for sub in surviving]
            # Classify surviving groups as fully- vs partially-matching via
            # predicate negation (a la DataFusion): a group is fully matched iff
            # ~filter cannot match any of its rows, i.e. the negation prunes it
            # out. For those, num_rows is an exact survivor count and can drive
            # the limit push-down. Any failure defaults to "not fully matched",
            # which is always safe (falls back to the post-filter stop).
            try:
                not_fully = {
                    sub.row_groups[0].id
                    for sub in fragment.split_by_row_group(~self.filter)
                }
            except Exception:
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
            fully_by_idx: Optional[dict] = {
                i: i not in not_fully and not self._has_filter_nulls(metadata, i)
                for i in rg_indices
            }
        else:
            rg_indices = range(metadata.num_row_groups)
            fully_by_idx = None  # no predicate -> every group is fully matched

        leaf_indices = (
            self._projected_leaf_indices(metadata.row_group(0))
            if metadata.num_row_groups
            else None
        )
        per_rg = [
            self._row_group_info(
                metadata.row_group(i),
                i,
                leaf_indices,
                fully_matched=(True if fully_by_idx is None else fully_by_idx[i]),
            )
            for i in rg_indices
        ]
        # Coalesce contiguous row groups into ~coalesce_bytes chunks (no-op when
        # coalesce_bytes == 0) so only a handful of descriptors per file reach the
        # driver's bin-packer instead of one per physical row group.
        row_groups = coalesce_row_groups(per_rg, self.coalesce_bytes)
        return FileChunks(path=path, size=size, row_groups=row_groups)

    @ray.method(num_returns="streaming")
    def read_footers(
        self, files: List[Tuple[str, int]], *, result_batch_size: int = 1
    ) -> Iterator[List[FileChunks]]:
        """Read the footers of ``files`` concurrently, yielding ``FileChunks``.

        Yields lists of ``FileChunks`` (not single results): each list the driver
        receives costs one object-store fetch, so batching cuts driver-side
        deserialization overhead ~``result_batch_size``-fold. At the default of
        ``1`` a directory of N files costs N fetches on the single listing task;
        callers set it via ``RAY_DATA_PARQUET_FOOTER_RESULT_BATCH_SIZE``.
        ``num_returns`` is fixed to ``"streaming"`` via ``@ray.method`` so
        results stream out as footers land.
        """
        futures = [
            self.pool.submit(self._read_and_chunk, path, size) for path, size in files
        ]
        buffer: List[FileChunks] = []
        total_row_groups = 0
        for finished in as_completed(futures):
            chunk = finished.result()
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
