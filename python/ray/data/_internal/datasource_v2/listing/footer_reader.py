from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
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

if TYPE_CHECKING:
    import pyarrow.compute as pc

    from ray.data.expressions import Expr

logger = logging.getLogger(__name__)


def coalesce_row_groups(
    per_rg: List[RowGroupInfo], target: int
) -> Tuple[RowGroupInfo, ...]:
    """Merge runs of consecutive row groups into ~``target``-byte chunks.

    A run breaks on: a change in ``fully_matched`` (never merge across the
    match-class boundary, or limit push-down would miscount), a gap in the
    ``rg_idx`` sequence (e.g. filter-pruned groups), or once the accumulator
    reaches ``target``. A single row group larger than ``target`` forms its own
    chunk. ``target == 0`` disables coalescing entirely (one chunk per physical
    row group). Coalesced chunks (``rg_count > 1``) carry per-row-group
    ``rg_sizes`` / ``rg_rows`` so the packer can split them back at exact
    boundaries. Pure function -- unit-tested directly.
    """
    if not target:
        return tuple(per_rg)
    out: List[RowGroupInfo] = []
    cur: Optional[RowGroupInfo] = None
    cur_sizes: List[int] = []
    cur_rows: List[int] = []

    def _flush() -> None:
        if cur is None:
            return
        if len(cur_sizes) > 1:
            out.append(replace(cur, rg_sizes=tuple(cur_sizes), rg_rows=tuple(cur_rows)))
        else:
            out.append(cur)

    for rg in per_rg:  # ascending rg_idx; each is a single physical row group
        if (
            cur is not None
            and cur.fully_matched == rg.fully_matched
            and cur.rg_idx + cur.rg_count == rg.rg_idx  # contiguous
            and cur.uncompressed_size < target  # not full yet
        ):
            cur = replace(
                cur,
                rg_count=cur.rg_count + rg.rg_count,
                uncompressed_size=cur.uncompressed_size + rg.uncompressed_size,
                num_rows=cur.num_rows + rg.num_rows,
            )
            cur_sizes.append(rg.uncompressed_size)
            cur_rows.append(rg.num_rows)
        else:
            _flush()
            cur = rg
            cur_sizes = [rg.uncompressed_size]
            cur_rows = [rg.num_rows]
    _flush()
    return tuple(out)


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
            fully_by_idx: Optional[dict] = {i: i not in not_fully for i in rg_indices}
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
