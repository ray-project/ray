"""Pure row-group coalescing for the footer-based Parquet chunking path."""
from dataclasses import replace
from typing import List, Tuple

from ray.data._internal.datasource_v2.chunkers.parquet_decoded_size import (
    decoded_size_or_fallback,
    sum_exact,
)
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
    ``rg_sizes`` / ``rg_decoded_sizes`` / ``rg_rows`` so the packer can split them
    back at exact boundaries.

    ``target`` is measured in decoded bytes, since that is what the downstream bin
    budget is measured in. A run whose row groups do not all have an exact decoded
    size falls back to the scaled uncompressed estimate for the accumulator, and
    reports ``decoded_size=None`` so consumers keep falling back consistently.

    Args:
        per_rg: Single-row-group infos in ascending ``rg_idx`` order.
        target: Target chunk size in decoded bytes; ``0`` disables coalescing.

    Returns:
        The coalesced row-group chunks, in the same ascending order.
    """
    if not target:
        return tuple(per_rg)
    out: List[RowGroupInfo] = []
    # The open run, plus its running totals in the units the fullness check
    # needs. Everything else about a run is derived from the list at flush time.
    run: List[RowGroupInfo] = []
    run_uncompressed = 0
    # ``None`` once any member lacks exact decoded sizing, so the fullness check
    # below falls back for the whole run -- the same all-or-nothing rule the
    # flushed chunk reports via ``sum_exact``.
    run_decoded: "int | None" = 0

    def _flush() -> None:
        if not run:
            return
        if len(run) == 1:
            out.append(run[0])
            return
        decoded = sum_exact(rg.decoded_size for rg in run)
        out.append(
            replace(
                run[0],
                rg_count=sum(rg.rg_count for rg in run),
                uncompressed_size=sum(rg.uncompressed_size for rg in run),
                num_rows=sum(rg.num_rows for rg in run),
                decoded_size=decoded,
                rg_sizes=tuple(rg.uncompressed_size for rg in run),
                rg_rows=tuple(rg.num_rows for rg in run),
                rg_decoded_sizes=(
                    tuple(rg.decoded_size for rg in run) if decoded is not None else ()
                ),
            )
        )

    for rg in per_rg:  # ascending rg_idx; each is a single physical row group
        if (
            run
            and run[0].fully_matched == rg.fully_matched
            and run[-1].rg_idx + run[-1].rg_count == rg.rg_idx  # contiguous
            # Not full yet, measured the same way the bin budget is.
            and decoded_size_or_fallback(run_decoded, run_uncompressed) < target
        ):
            run.append(rg)
        else:
            _flush()
            run = [rg]
            run_uncompressed = 0
            run_decoded = 0
        run_uncompressed += rg.uncompressed_size
        if run_decoded is not None:
            run_decoded = (
                None if rg.decoded_size is None else run_decoded + rg.decoded_size
            )
    _flush()
    return tuple(out)
