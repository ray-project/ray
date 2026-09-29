"""Pure row-group coalescing for the footer-based Parquet chunking path."""
from dataclasses import replace
from typing import List, Optional, Tuple

from ray.data._internal.datasource_v2.interfaces.file_manifest import UnitRun

__all__ = [
    "coalesce_row_groups",
]


def coalesce_row_groups(per_rg: List[UnitRun], target: int) -> Tuple[UnitRun, ...]:
    """Merge runs of consecutive row groups into ~``target``-byte runs.

    A run breaks on: a change in ``fully_matched`` (never merge across the
    match-class boundary, or limit push-down would miscount), a gap in the
    row-group index sequence (e.g. filter-pruned groups), or once the
    accumulator reaches ``target``. A single row group larger than ``target``
    forms its own run. ``target == 0`` disables coalescing entirely (one run
    per physical row group). Runs of more than one group carry per-group
    ``unit_sizes`` / ``unit_rows`` so the packer can split them back at exact
    boundaries.

    Args:
        per_rg: Single-row-group runs in ascending index order.
        target: Target run size in bytes; ``0`` disables coalescing.

    Returns:
        The coalesced runs, in the same ascending order.
    """
    if not target:
        return tuple(per_rg)
    out: List[UnitRun] = []
    cur: Optional[UnitRun] = None
    cur_sizes: List[int] = []
    cur_rows: List[int] = []

    def _flush() -> None:
        if cur is None:
            return
        if len(cur_sizes) > 1:
            out.append(
                replace(cur, unit_sizes=tuple(cur_sizes), unit_rows=tuple(cur_rows))
            )
        else:
            out.append(cur)

    for rg in per_rg:  # ascending index; each is a single physical row group
        if (
            cur is not None
            and cur.fully_matched == rg.fully_matched
            and cur.unit_ids[-1] + 1 == rg.unit_ids[0]  # contiguous
            and cur.size_bytes < target  # not full yet
        ):
            cur = replace(
                cur,
                unit_ids=cur.unit_ids + rg.unit_ids,
                num_rows=cur.num_rows + rg.num_rows,
                size_bytes=cur.size_bytes + rg.size_bytes,
            )
            cur_sizes.append(rg.size_bytes)
            cur_rows.append(rg.num_rows)
        else:
            _flush()
            cur = rg
            cur_sizes = [rg.size_bytes]
            cur_rows = [rg.num_rows]
    _flush()
    return tuple(out)
