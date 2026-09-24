"""Parquet chunk helpers for DataSourceV2.

Maps ``ParquetRowGroupChunkMetadata`` (the explicit surviving row groups a bin
assigns to a file) to PyArrow ``ParquetFileFragment`` subsets for reading, and
names the read unit each subset stands for.
"""
from typing import Callable, Iterable, List, TypeVar

import pyarrow.dataset as pds

from ray._common.retry import call_with_retry
from ray.data._internal.datasource_v2.read_units import ReadUnit, ReadUnitFragment

R = TypeVar("R")


def _row_group_unit_id(path: str, row_group_id: int) -> str:
    """Stable :attr:`ReadUnit.id` of one physical row group of a Parquet file.

    The reader reports it for each row group it scans on its own, and the
    footer reader accepts the same id in ``excluded_read_unit_ids`` to leave
    that row group out of a listing. Nothing parses it.
    """
    return f"{path}#rg{row_group_id}"


def _with_io_retry(f: Callable[[], R], description: str) -> R:
    """Run ``f``, retrying the transient IO errors configured on the context.

    ``ParquetFileFragment.subset`` and ``.metadata`` both open the file to read
    its footer, so on remote storage they fail with the same transient errors
    (S3 timeouts, throttling) the rest of the read path already retries.
    """
    from ray.data.context import DataContext

    return call_with_retry(
        f,
        description=description,
        match=DataContext.get_current().retried_io_errors,
    )


def _fragments_from_row_group_ids(
    fragment: pds.ParquetFileFragment,
    row_group_ids: Iterable[int],
    *,
    per_row_group_offsets: bool,
) -> List[ReadUnitFragment]:
    """Slice ``fragment`` to the explicit physical ``row_group_ids`` of one bin.

    Used by the footer-based chunking path, where ``ParquetRowGroupChunkMetadata``
    names the exact surviving row groups for a file (predicate pruning + bin
    packing already happened upstream), so no size-based reconciliation is needed.

    Returns one :class:`ReadUnitFragment` per sub-fragment.

    When ``per_row_group_offsets`` is False (the common case) the file's groups are
    scanned together as a single sub-fragment with a row offset of 0 -- this
    lets PyArrow coalesce reads across the groups -- and the read unit names
    the whole file. When True (a synthesized column needs read unit
    boundaries), one sub-fragment per row group is returned, each paired with
    a :class:`ReadUnit` named ``"<path>#rg<index>"`` and its cumulative
    pre-filter row offset within the file, so per-row values stay unique and
    match physical row positions even when pruned groups make the surviving
    set non-contiguous. This branch reads the footer once for the offsets.
    """
    ids = sorted(row_group_ids)
    if not ids:
        return []

    def _subset(rg_ids: List[int]) -> pds.ParquetFileFragment:
        return _with_io_retry(
            lambda: fragment.subset(row_group_ids=rg_ids),
            f"subset row groups {rg_ids} of {fragment.path}",
        )

    path = fragment.path
    if not per_row_group_offsets:
        return [ReadUnitFragment(_subset(ids), ReadUnit(id=path, source=path, count=1))]

    metadata = _with_io_retry(
        lambda: fragment.metadata, f"read Parquet footer for {path}"
    )
    # prefix[i] == pre-filter index in the file of row group i's first row.
    prefix = [0] * (metadata.num_row_groups + 1)
    for i in range(metadata.num_row_groups):
        prefix[i + 1] = prefix[i] + metadata.row_group(i).num_rows
    return [
        ReadUnitFragment(
            _subset([rg_id]),
            ReadUnit(
                id=_row_group_unit_id(path, rg_id),
                source=path,
                index=rg_id,
                count=metadata.num_row_groups,
                num_rows=metadata.row_group(rg_id).num_rows,
            ),
            unit_start_row=prefix[rg_id],
        )
        for rg_id in ids
    ]
