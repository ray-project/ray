"""Tests for the footer-based Parquet chunking path.

Covers the pure-logic pieces (row-group coalescing and the online bin packer,
including the split-coalesced no-op invariant) without a Ray cluster, plus a few
end-to-end reads that exercise ``FooterFileIndexer`` through
``ray.data.read_parquet`` with predicate / limit / projection push-down.
"""

import pytest

from ray.data._internal.datasource_v2.chunkers.parquet_footer_types import (
    FileChunks,
    RowGroupInfo,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.footer_reader import coalesce_row_groups
from ray.data._internal.datasource_v2.partitioners.online_bin_packer import (
    OnlineBinPacker,
)


def _rg(idx, size, rows=10, fully_matched=True):
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
            [_rg(0, 10), _rg(1, 20), _rg(2, 30)],
            0,
            [(0, 1, 10, 10), (1, 1, 20, 10), (2, 1, 30, 10)],
            id="disabled-is-identity",
        ),
        pytest.param(
            [_rg(0, 10, 1), _rg(1, 10, 2), _rg(2, 10, 3)],
            25,
            [(0, 3, 30, 6)],
            id="merge-contiguous-until-target",
        ),
        pytest.param(
            [_rg(0, 10, fully_matched=True), _rg(1, 10, fully_matched=False)],
            1000,
            [(0, 1, 10, 10), (1, 1, 10, 10)],
            id="break-on-fully-matched-change",
        ),
        pytest.param(
            [_rg(0, 10), _rg(2, 10)],
            1000,
            [(0, 1, 10, 10), (2, 1, 10, 10)],
            id="break-on-index-gap",
        ),
    ],
)
def test_coalesce(per_rg, target, expected):
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


def _manifest_map(manifest: FileManifest):
    """A sealed bin's manifest as ``{path: sorted physical row-group ids}``."""
    return {
        str(path): sorted(meta["row_group_ids"])
        for path, meta in zip(manifest.paths, manifest.file_chunk_metadatas)
    }


def _pack(file_chunks_list, max_bin_bytes, **kwargs):
    packer = OnlineBinPacker(max_bin_bytes, **kwargs)
    bins = []
    for file_chunks in file_chunks_list:
        packer.add_file_chunks(file_chunks)
        while packer.has_partition():
            bins.append(_manifest_map(packer.next_partition()))
    packer.finalize()
    while packer.has_partition():
        bins.append(_manifest_map(packer.next_partition()))
    return bins


def _pairs(bins):
    """All ``(path, rg_id)`` pairs across bins, sorted."""
    return sorted((p, i) for b in bins for p, ids in b.items() for i in ids)


@pytest.mark.parametrize(
    "files, max_bin, expected_bins",
    [
        pytest.param(
            [FileChunks("a", 10, (_rg(0, 10),)), FileChunks("b", 10, (_rg(0, 10),))],
            1000,
            [{"a": [0], "b": [0]}],
            id="light-colours-share-a-bin",
        ),
        pytest.param(
            [FileChunks("a", 500, (_rg(0, 500),))],
            100,
            [{"a": [0]}],
            id="oversize-group-gets-own-bin",
        ),
    ],
)
def test_packer_placement(files, max_bin, expected_bins):
    assert _pack(files, max_bin) == expected_bins


def test_packer_heavy_colour_spans_multiple_bins():
    # A file far heavier than one bin spills into several bins (exact split point
    # depends on the light->heavy threshold, so assert coverage, not layout).
    files = [FileChunks("a", 400, tuple(_rg(i, 100) for i in range(4)))]
    bins = _pack(files, max_bin_bytes=100)
    assert len(bins) == 4
    assert _pairs(bins) == [("a", i) for i in range(4)]


@pytest.mark.parametrize("split_coalesced", [False, True])
def test_packer_covers_every_row_group_exactly_once(split_coalesced):
    files = [
        FileChunks("a", 120, tuple(_rg(i, 30) for i in range(4))),
        FileChunks("b", 90, tuple(_rg(i, 30) for i in range(3))),
    ]
    pairs = _pairs(_pack(files, max_bin_bytes=100, split_coalesced=split_coalesced))
    expected = sorted([("a", i) for i in range(4)] + [("b", i) for i in range(3)])
    assert pairs == expected
    assert len(pairs) == len(set(pairs))  # no duplicates


def test_split_coalesced_is_noop_without_coalescing():
    # With every rg_count == 1, the split flag must not change the packing.
    files = [
        FileChunks("a", 120, tuple(_rg(i, 40) for i in range(3))),
        FileChunks("b", 80, tuple(_rg(i, 40) for i in range(2))),
    ]
    assert _pack(files, 100, split_coalesced=False) == _pack(
        files, 100, split_coalesced=True
    )


def test_split_coalesced_splits_oversize_run_at_boundaries():
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
    bins = _pack([FileChunks("a", 90, (coalesced,))], 50, split_coalesced=True)
    assert _pairs(bins) == [("a", 0), ("a", 1), ("a", 2)]
    assert len(bins) >= 2  # 90 bytes across 50-byte bins


# ---------------------------------------------------------------------------
# End-to-end through ray.data.read_parquet (footer path)
# ---------------------------------------------------------------------------

_N_PER_FILE = 400
_N_FILES = 3


@pytest.fixture
def footer_parquet(tmp_path, monkeypatch):
    """Write a small multi-row-group Parquet dataset; enable the footer path."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from ray.data.context import DataContext

    # Footer chunking is the default for Parquet V2; just keep the pool small.
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "2")
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", "2")

    ctx = DataContext.get_current()
    prev_v2 = ctx.use_datasource_v2
    ctx.use_datasource_v2 = True

    for f in range(_N_FILES):
        start = f * _N_PER_FILE
        table = pa.table(
            {
                "id": list(range(start, start + _N_PER_FILE)),
                "val": [f"v{i}" for i in range(_N_PER_FILE)],
            }
        )
        pq.write_table(table, str(tmp_path / f"part_{f}.parquet"), row_group_size=100)

    try:
        yield str(tmp_path)
    finally:
        ctx.use_datasource_v2 = prev_v2


def test_e2e_footer_read_matches_expected(footer_parquet):
    import ray

    total = _N_PER_FILE * _N_FILES
    ds = ray.data.read_parquet(footer_parquet)
    assert ds.count() == total
    assert sorted(r["id"] for r in ds.take_all()) == list(range(total))


@pytest.mark.parametrize(
    "op, expected",
    [
        pytest.param(lambda ds: ds.filter(expr="id < 50").count(), 50, id="filter"),
        pytest.param(lambda ds: ds.limit(10).count(), 10, id="limit"),
        pytest.param(
            lambda ds: ds.select_columns(["id"]).schema().names,
            ["id"],
            id="projection",
        ),
    ],
)
def test_e2e_footer_pushdowns(footer_parquet, op, expected):
    import ray

    assert op(ray.data.read_parquet(footer_parquet)) == expected


# Filter and limit push down together: the limit stops listing early once the
# ``num_rows`` of *fully matched* row groups reaches it, so that classification
# has to be exact. Nulls are the interesting case -- Parquet min/max statistics
# are computed over non-null values only, so a group whose non-null values all
# satisfy the filter looks fully matched by bounds alone while its null rows do
# not survive. Deliberately lopsided at 10 survivors per 100 rows: the stop is
# evaluated per file, so a fixture with a small shortfall can pass by luck when
# the last file's overshoot covers the deficit.
_NULL_FILES = 20
_NULL_ROWS_PER_FILE = 100
_NULL_TOTAL_SURVIVORS = _NULL_FILES * 10


@pytest.fixture
def nullable_parquet(tmp_path, monkeypatch):
    """Multi-file, multi-row-group data whose filtered column holds nulls."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from ray.data.context import DataContext

    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "2")
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", "2")

    ctx = DataContext.get_current()
    prev_v2 = ctx.use_datasource_v2
    ctx.use_datasource_v2 = True

    for f in range(_NULL_FILES):
        ids = [
            3 + f * 1000 + i if i % 10 == 0 else None
            for i in range(_NULL_ROWS_PER_FILE)
        ]
        pq.write_table(
            pa.table({"id": pa.array(ids, pa.int64())}),
            str(tmp_path / f"part_{f}.parquet"),
            row_group_size=25,
        )

    try:
        yield str(tmp_path)
    finally:
        ctx.use_datasource_v2 = prev_v2


@pytest.mark.parametrize(
    "limit", [1, 10, 100, _NULL_TOTAL_SURVIVORS, 10 * _NULL_TOTAL_SURVIVORS]
)
def test_e2e_filter_then_limit_with_nulls(nullable_parquet, limit):
    """``filter(...).limit(n)`` delivers ``n`` rows whenever ``n`` survivors exist.

    If nulls were ever counted as survivors, listing would stop short and
    ``Limit`` would return fewer rows than asked for, with no error.
    """
    import ray
    from ray.data.expressions import col

    ds = ray.data.read_parquet(nullable_parquet)
    rows = ds.filter(expr=col("id") > 2).limit(limit).take_all()

    assert len(rows) == min(limit, _NULL_TOTAL_SURVIVORS)
    assert all(r["id"] is not None and r["id"] > 2 for r in rows)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
