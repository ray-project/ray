from typing import Any, Optional

import pytest

from ray.data._internal.datasource_v2.common.online_bin_packer import OnlineBinPacker
from ray.data._internal.datasource_v2.interfaces.file_manifest import (
    ChunkMetadata,
    FileManifest,
    UnitRun,
)


def _run(
    ids: tuple[int, ...],
    size: int,
    rows: int = 10,
    fully_matched: bool = True,
    unit_sizes: tuple[int, ...] = (),
    unit_rows: tuple[int, ...] = (),
) -> ChunkMetadata:
    return UnitRun(
        unit_ids=ids,
        num_rows=rows,
        size_bytes=size,
        fully_matched=fully_matched,
        unit_sizes=unit_sizes,
        unit_rows=unit_rows,
    ).to_metadata()


def _units(path: str, sizes: list[int]) -> FileManifest:
    """A file listed as one single-unit run per size, in unit order."""
    return _file(path, sum(sizes), [_run((i,), s) for i, s in enumerate(sizes)])


def _file(path: str, size: int, runs: list[ChunkMetadata]) -> FileManifest:
    return FileManifest.construct_manifest(
        paths=[path] * len(runs), sizes=[size] * len(runs), chunk_metadatas=list(runs)
    )


def _manifest_map(manifest: FileManifest) -> dict[str, Optional[list[int]]]:
    """A sealed bin's manifest as ``{path: sorted unit ids}``; ``None`` = whole file."""
    return {
        str(path): None if meta is None else sorted(meta["unit_ids"])
        for path, meta in zip(manifest.paths, manifest.file_chunk_metadatas)
    }


def _pack(
    manifests: list[FileManifest],
    max_bin_bytes: int,
    **kwargs: Any,
) -> list[dict[str, Optional[list[int]]]]:
    packer = OnlineBinPacker(max_bin_bytes, **kwargs)
    bins = []
    for manifest in manifests:
        packer.add_input(manifest)
        while packer.has_partition():
            bins.append(_manifest_map(packer.next_partition()))
    packer.finalize()
    while packer.has_partition():
        bins.append(_manifest_map(packer.next_partition()))
    return bins


def _pairs(bins: list[dict[str, Optional[list[int]]]]) -> list[tuple[str, int]]:
    """All ``(path, unit_id)`` pairs across bins, sorted."""
    return sorted((p, i) for b in bins for p, ids in b.items() for i in ids or ())


@pytest.mark.parametrize(
    "files, max_bin, expected_bins",
    [
        pytest.param(
            [_units("a", [10]), _units("b", [10])],
            1000,
            [{"a": [0], "b": [0]}],
            id="light-colours-share-a-bin",
        ),
        pytest.param(
            [_units("a", [500])],
            100,
            [{"a": [0]}],
            id="oversize-unit-gets-own-bin",
        ),
    ],
)
def test_packer_placement(
    files: list[FileManifest],
    max_bin: int,
    expected_bins: list[dict[str, Optional[list[int]]]],
) -> None:
    assert _pack(files, max_bin) == expected_bins


def test_packer_heavy_colour_spans_multiple_bins() -> None:
    # A file far heavier than one bin spills into several bins (exact split point
    # depends on the light->heavy threshold, so assert coverage, not layout).
    bins = _pack([_units("a", [100] * 4)], max_bin_bytes=100)
    assert len(bins) == 4
    assert _pairs(bins) == [("a", i) for i in range(4)]


def test_full_heavy_bin_is_sealed_immediately() -> None:
    packer = OnlineBinPacker(max_bin_bytes=100)

    # The first two units are light and fill a shared bin, which seals
    # immediately. The third makes this colour heavy and exactly fills its
    # dedicated bin, which must also seal for early scheduling.
    packer.add_input(_units("a", [60, 40, 100]))
    assert _manifest_map(packer.next_partition()) == {"a": [0, 1]}
    assert _manifest_map(packer.next_partition()) == {"a": [2]}
    assert not packer.has_partition()

    packer.finalize()
    assert not packer.has_partition()


@pytest.mark.parametrize("split_coalesced", [False, True])
def test_packer_covers_every_unit_exactly_once(split_coalesced: bool) -> None:
    files = [_units("a", [30] * 4), _units("b", [30] * 3)]
    pairs = _pairs(_pack(files, max_bin_bytes=100, split_coalesced=split_coalesced))
    expected = sorted([("a", i) for i in range(4)] + [("b", i) for i in range(3)])
    assert pairs == expected
    assert len(pairs) == len(set(pairs))  # no duplicates


def test_split_coalesced_is_noop_without_coalescing() -> None:
    # With every run a single unit, the split flag must not change the packing.
    files = [_units("a", [40] * 3), _units("b", [40] * 2)]
    assert _pack(files, 100, split_coalesced=False) == _pack(
        files, 100, split_coalesced=True
    )


def test_split_coalesced_prefers_bin_that_fits_largest_prefix() -> None:
    # Set up shared bins with 30 and 65 bytes free, respectively. The coalesced
    # run has three 30-byte units: the first bin fits one exactly, while the
    # second bin fits two with 5 bytes remaining.
    coalesced = _run(
        (0, 1, 2), 90, rows=30, unit_sizes=(30, 30, 30), unit_rows=(10, 10, 10)
    )
    bins = _pack(
        [_units("a", [70]), _units("b", [35]), _file("c", 90, [coalesced])],
        max_bin_bytes=100,
        split_coalesced=True,
    )

    # Prefer the bin that can swallow c's first two units; the remaining unit
    # then fills the other bin.
    assert bins == [
        {"a": [0], "c": [2]},
        {"b": [0], "c": [0, 1]},
    ]


def test_split_coalesced_splits_oversize_run_at_boundaries() -> None:
    # A run (units 0..2) that can't fit whole in a 50-byte bin is cut at unit
    # boundaries; every unit still appears exactly once.
    coalesced = _run(
        (0, 1, 2), 90, rows=30, unit_sizes=(30, 30, 30), unit_rows=(10, 10, 10)
    )
    bins = _pack([_file("a", 90, [coalesced])], max_bin_bytes=50, split_coalesced=True)
    assert _pairs(bins) == [("a", 0), ("a", 1), ("a", 2)]
    assert len(bins) >= 2  # 90 bytes across 50-byte bins


def test_oversize_coalesced_run_fills_shared_bin() -> None:
    # "big" is the file's first run, so it has not reached the per-file
    # isolation threshold. Because this run is splittable, its 40-byte first
    # unit fills the space left by "light" (100 - 60).
    coalesced = _run((0, 1), 120, rows=12, unit_sizes=(40, 80), unit_rows=(4, 8))
    bins = _pack(
        [_units("light", [60]), _file("big", 120, [coalesced])],
        max_bin_bytes=100,
        split_coalesced=True,
    )

    assert bins == [{"light": [0], "big": [0]}, {"big": [1]}]


def test_whole_file_rows_pack_by_file_size() -> None:
    # Rows with no chunk metadata (a plain listing) are indivisible items
    # sized by the file, and come out of the packer as whole-file rows again.
    def whole(path: str, size: int) -> FileManifest:
        return FileManifest.construct_manifest(
            paths=[path], sizes=[size], chunk_metadatas=[None]
        )

    packer = OnlineBinPacker(max_bin_bytes=100)
    bins = []
    for manifest in [whole("big", 150), whole("a", 60), whole("b", 30)]:
        packer.add_input(manifest)
        while packer.has_partition():
            bins.append(packer.next_partition())
    packer.finalize()
    while packer.has_partition():
        bins.append(packer.next_partition())

    assert [_manifest_map(b) for b in bins] == [
        {"big": None},
        {"a": None, "b": None},
    ]
    assert dict(zip(bins[1].paths, bins[1].file_sizes)) == {"a": 60, "b": 30}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
