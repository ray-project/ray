import pytest

from ray.data._internal.datasource_v2.formats.parquet.parquet_row_group_coalescing import (
    coalesce_row_groups,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import UnitRun


def _rg(idx: int, size: int, rows: int = 10, fully_matched: bool = True) -> UnitRun:
    return UnitRun(
        unit_ids=(idx,), size_bytes=size, num_rows=rows, fully_matched=fully_matched
    )


@pytest.mark.parametrize(
    "per_rg, target, expected",
    [
        pytest.param(
            [_rg(idx=0, size=10), _rg(idx=1, size=20), _rg(idx=2, size=30)],
            0,
            [(0, 1, 10, 10), (1, 1, 20, 10), (2, 1, 30, 10)],
            id="disabled-is-identity",
        ),
        pytest.param(
            [
                _rg(idx=0, size=10, rows=1),
                _rg(idx=1, size=10, rows=2),
                _rg(idx=2, size=10, rows=3),
            ],
            25,
            [(0, 3, 30, 6)],
            id="merge-contiguous-until-target",
        ),
        pytest.param(
            [
                _rg(idx=0, size=10, fully_matched=True),
                _rg(idx=1, size=10, fully_matched=False),
            ],
            1000,
            [(0, 1, 10, 10), (1, 1, 10, 10)],
            id="break-on-fully-matched-change",
        ),
        pytest.param(
            [_rg(idx=0, size=10), _rg(idx=2, size=10)],
            1000,
            [(0, 1, 10, 10), (2, 1, 10, 10)],
            id="break-on-index-gap",
        ),
    ],
)
def test_coalesce(
    per_rg: list[UnitRun],
    target: int,
    expected: list[tuple[int, int, int, int]],
) -> None:
    out = coalesce_row_groups(per_rg, target)

    assert [
        (c.unit_ids[0], len(c.unit_ids), c.size_bytes, c.num_rows) for c in out
    ] == expected
    # Every physical row group is covered exactly once.
    covered = [i for c in out for i in c.unit_ids]
    assert sorted(covered) == sorted(r.unit_ids[0] for r in per_rg)
    # Per-group breakdown is attached iff the run has more than one group.
    for c in out:
        if len(c.unit_ids) > 1:
            assert len(c.unit_sizes) == len(c.unit_ids)
            assert len(c.unit_rows) == len(c.unit_ids)
        else:
            assert c.unit_sizes == () and c.unit_rows == ()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
