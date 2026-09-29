from typing import Any

import pytest

from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import (
    FileChunks,
    RowGroupInfo,
)
from ray.data._internal.datasource_v2.chunkers.parquet_row_group_coalescing import (
    coalesce_row_groups,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.footer_file_indexer import (
    _file_chunks_to_manifest,
)
from ray.data._internal.datasource_v2.partitioners.online_bin_packer import (
    OnlineBinPacker,
)


def _rg(
    idx: int, size: int, rows: int = 10, fully_matched: bool = True
) -> RowGroupInfo:
    return RowGroupInfo(
        rg_idx=idx, uncompressed_size=size, num_rows=rows, fully_matched=fully_matched
    )


# ---------------------------------------------------------------------------
# coalesce_row_groups (pure)
# ---------------------------------------------------------------------------


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
    per_rg: list[RowGroupInfo],
    target: int,
    expected: list[tuple[int, int, int, int]],
) -> None:
    out = coalesce_row_groups(per_rg, target)

    assert [
        (c.rg_idx, c.rg_count, c.uncompressed_size, c.num_rows) for c in out
    ] == expected
    # Every physical row group is covered exactly once.
    covered = [i for c in out for i in range(c.rg_idx, c.rg_idx + c.rg_count)]
    assert sorted(covered) == sorted(r.rg_idx for r in per_rg)
    # Per-group breakdown is attached iff the chunk is coalesced (rg_count > 1).
    for c in out:
        if c.rg_count > 1:
            assert len(c.rg_sizes) == c.rg_count and len(c.rg_rows) == c.rg_count
        else:
            assert c.rg_sizes == () and c.rg_rows == ()


# ---------------------------------------------------------------------------
# OnlineBinPacker (pure)
# ---------------------------------------------------------------------------


def _manifest_map(manifest: FileManifest) -> dict[str, list[int]]:
    """A sealed bin's manifest as ``{path: sorted physical row-group ids}``."""
    return {
        str(path): sorted(meta["row_group_ids"])
        for path, meta in zip(manifest.paths, manifest.file_chunk_metadatas)
    }


def _pack(
    file_chunks_list: list[FileChunks],
    max_bin_bytes: int,
    **kwargs: Any,
) -> list[dict[str, list[int]]]:
    packer = OnlineBinPacker(max_bin_bytes, **kwargs)
    bins = []
    for file_chunks in file_chunks_list:
        packer.add_input(_file_chunks_to_manifest(file_chunks))
        while packer.has_partition():
            bins.append(_manifest_map(packer.next_partition()))
    packer.finalize()
    while packer.has_partition():
        bins.append(_manifest_map(packer.next_partition()))
    return bins


def _pairs(bins: list[dict[str, list[int]]]) -> list[tuple[str, int]]:
    """All ``(path, rg_id)`` pairs across bins, sorted."""
    return sorted((p, i) for b in bins for p, ids in b.items() for i in ids)


@pytest.mark.parametrize(
    "files, max_bin, expected_bins",
    [
        pytest.param(
            [
                FileChunks(path="a", size=10, row_groups=(_rg(idx=0, size=10),)),
                FileChunks(path="b", size=10, row_groups=(_rg(idx=0, size=10),)),
            ],
            1000,
            [{"a": [0], "b": [0]}],
            id="light-colours-share-a-bin",
        ),
        pytest.param(
            [FileChunks(path="a", size=500, row_groups=(_rg(idx=0, size=500),))],
            100,
            [{"a": [0]}],
            id="oversize-group-gets-own-bin",
        ),
    ],
)
def test_packer_placement(
    files: list[FileChunks],
    max_bin: int,
    expected_bins: list[dict[str, list[int]]],
) -> None:
    assert _pack(files, max_bin) == expected_bins


def test_packer_heavy_colour_spans_multiple_bins() -> None:
    # A file far heavier than one bin spills into several bins (exact split point
    # depends on the light->heavy threshold, so assert coverage, not layout).
    files = [
        FileChunks(
            path="a",
            size=400,
            row_groups=tuple(_rg(idx=i, size=100) for i in range(4)),
        )
    ]
    bins = _pack(files, max_bin_bytes=100)
    assert len(bins) == 4
    assert _pairs(bins) == [("a", i) for i in range(4)]


def test_full_heavy_bin_is_sealed_immediately() -> None:
    packer = OnlineBinPacker(max_bin_bytes=100)

    # The first two row groups are light and fill a shared bin, which seals
    # immediately. The third makes this colour heavy and exactly fills its
    # dedicated bin, which must also seal for early scheduling.
    packer.add_input(
        _file_chunks_to_manifest(
            FileChunks(
                path="a",
                size=200,
                row_groups=(
                    _rg(idx=0, size=60),
                    _rg(idx=1, size=40),
                    _rg(idx=2, size=100),
                ),
            )
        )
    )
    assert _manifest_map(packer.next_partition()) == {"a": [0, 1]}
    assert _manifest_map(packer.next_partition()) == {"a": [2]}
    assert not packer.has_partition()

    packer.finalize()
    assert not packer.has_partition()


@pytest.mark.parametrize("split_coalesced", [False, True])
def test_packer_covers_every_row_group_exactly_once(split_coalesced: bool) -> None:
    files = [
        FileChunks(
            path="a",
            size=120,
            row_groups=tuple(_rg(idx=i, size=30) for i in range(4)),
        ),
        FileChunks(
            path="b",
            size=90,
            row_groups=tuple(_rg(idx=i, size=30) for i in range(3)),
        ),
    ]
    pairs = _pairs(_pack(files, max_bin_bytes=100, split_coalesced=split_coalesced))
    expected = sorted([("a", i) for i in range(4)] + [("b", i) for i in range(3)])
    assert pairs == expected
    assert len(pairs) == len(set(pairs))  # no duplicates


def test_split_coalesced_is_noop_without_coalescing() -> None:
    # With every rg_count == 1, the split flag must not change the packing.
    files = [
        FileChunks(
            path="a",
            size=120,
            row_groups=tuple(_rg(idx=i, size=40) for i in range(3)),
        ),
        FileChunks(
            path="b",
            size=80,
            row_groups=tuple(_rg(idx=i, size=40) for i in range(2)),
        ),
    ]
    assert _pack(files, 100, split_coalesced=False) == _pack(
        files, 100, split_coalesced=True
    )


def test_split_coalesced_prefers_bin_that_fits_largest_prefix() -> None:
    # Set up shared bins with 30 and 65 bytes free, respectively. The coalesced
    # item has three 30-byte units: the first bin fits one exactly, while the
    # second bin fits two with 5 bytes remaining.
    coalesced = RowGroupInfo(
        rg_idx=0,
        uncompressed_size=90,
        num_rows=30,
        rg_count=3,
        rg_sizes=(30, 30, 30),
        rg_rows=(10, 10, 10),
    )
    bins = _pack(
        [
            FileChunks(path="a", size=70, row_groups=(_rg(idx=0, size=70),)),
            FileChunks(path="b", size=35, row_groups=(_rg(idx=0, size=35),)),
            FileChunks(path="c", size=90, row_groups=(coalesced,)),
        ],
        max_bin_bytes=100,
        split_coalesced=True,
    )

    # Prefer the bin that can swallow c's first two row groups; the remaining
    # row group then fills the other bin.
    assert bins == [
        {"a": [0], "c": [2]},
        {"b": [0], "c": [0, 1]},
    ]


def test_split_coalesced_splits_oversize_run_at_boundaries() -> None:
    # A coalesced chunk (rg 0..2) that can't fit whole in a 50-byte bin is cut at
    # physical row-group boundaries; every group still appears exactly once.
    coalesced = RowGroupInfo(
        rg_idx=0,
        uncompressed_size=90,
        num_rows=30,
        rg_count=3,
        rg_sizes=(30, 30, 30),
        rg_rows=(10, 10, 10),
    )
    bins = _pack(
        [FileChunks(path="a", size=90, row_groups=(coalesced,))],
        max_bin_bytes=50,
        split_coalesced=True,
    )
    assert _pairs(bins) == [("a", 0), ("a", 1), ("a", 2)]
    assert len(bins) >= 2  # 90 bytes across 50-byte bins


def test_oversize_coalesced_run_fills_shared_bin() -> None:
    # "big" is the file's first chunk, so it has not reached the per-file
    # isolation threshold. Because this coalesced run is splittable, its
    # 40-byte first row group fills the space left by "light" (100 - 60).
    coalesced = RowGroupInfo(
        rg_idx=0,
        uncompressed_size=120,
        num_rows=12,
        rg_count=2,
        rg_sizes=(40, 80),
        rg_rows=(4, 8),
    )
    bins = _pack(
        [
            FileChunks(path="light", size=60, row_groups=(_rg(idx=0, size=60),)),
            FileChunks(path="big", size=120, row_groups=(coalesced,)),
        ],
        max_bin_bytes=100,
        split_coalesced=True,
    )

    assert bins == [{"light": [0], "big": [0]}, {"big": [1]}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
