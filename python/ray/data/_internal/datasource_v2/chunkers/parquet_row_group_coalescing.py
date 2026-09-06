"""Pure row-group coalescing for the footer-based Parquet chunking path."""
from dataclasses import replace
from typing import List, Optional, Tuple

from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import RowGroupInfo

__all__ = [
    "coalesce_row_groups",
]


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
    boundaries.

    Args:
        per_rg: Single-row-group infos in ascending ``rg_idx`` order.
        target: Target chunk size in bytes; ``0`` disables coalescing.

    Returns:
        The coalesced row-group chunks, in the same ascending order.
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
