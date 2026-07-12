"""End-to-end test: ``ds.repartition()`` routed through v3 hash shuffle.

Unlike ``test_hash_shuffle_external_smoke.py`` which hand-drives the operator pair,
this test goes through the **real** Ray Data planner + StreamingExecutor.
That exercises:

  * ``DataContext.use_external_hash_shuffle`` dispatch in
    ``plan_all_to_all_op._plan_hash_shuffle_repartition_external``
  * the StreamingExecutor's backpressure / scheduling loop driving
    ``ExternalHashShuffleMapOp`` and ``ExternalHashShuffleReduceOp`` (not the smoke harness)
  * full lifecycle: from a ``range`` source through map + reduce, then
    consumed via ``take_all()`` — and ShuffleManager actors getting
    ref-counted down by Ray GC on bundle destruction

If the v2 path regresses or the v3 wiring breaks, one of these fails
fast. Each test restores the flag in ``finally`` so cross-test bleed
doesn't poison later cases.

Run with::

    pytest python/ray/data/tests/test_hash_shuffle_external_repartition.py -xvs
"""

from contextlib import contextmanager

import pytest

import ray
from ray.data.context import DataContext


@pytest.fixture(scope="module")
def ray_cluster():
    if not ray.is_initialized():
        ray.init(num_cpus=4, include_dashboard=False, ignore_reinit_error=True)
    yield


@contextmanager
def _v3_flag(enabled: bool):
    """Flip ``use_external_hash_shuffle`` for the duration of the test and
    put it back on the way out — regardless of pass / fail."""
    ctx = DataContext.get_current()
    prev = ctx.use_external_hash_shuffle
    ctx.use_external_hash_shuffle = enabled
    try:
        yield ctx
    finally:
        ctx.use_external_hash_shuffle = prev


def _keys_per_block(ds, columns):
    """Return, for each output block, the set of distinct key tuples it holds.

    Used to assert the hash-shuffle co-location guarantee: a key must appear
    in exactly one block.
    """
    per_block = []
    for ref_bundle in ds.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            block = ray.get(block_ref)
            cols = [block[c].to_pylist() for c in columns]
            per_block.append(set(zip(*cols)))
    return per_block


def _assert_keys_colocated(per_block):
    """Every key tuple appears in at most one block."""
    all_keys = [k for block in per_block for k in block]
    assert len(all_keys) == len(
        set(all_keys)
    ), f"A key landed in more than one block: {per_block}"


# ─────────────────────────── correctness ─────────────────────────────────


@pytest.mark.parametrize(
    "num_rows,num_partitions",
    [
        (100, 4),
        (1000, 8),
        (50, 3),  # rows not evenly divisible by partitions
    ],
)
def test_v3_repartition_row_count(ray_cluster, num_rows, num_partitions):
    """Row count must be preserved end-to-end."""
    with _v3_flag(True):
        ds = ray.data.range(num_rows).repartition(num_partitions)
        rows = ds.take_all()
    assert len(rows) == num_rows
    assert {r["id"] for r in rows} == set(range(num_rows))


def test_v3_repartition_partition_count(ray_cluster):
    """Output materialized block count should be close to num_partitions
    (one block per non-empty partition in the typical case — exact count
    can vary with target_max_block_size reshape; we just bound it)."""
    num_rows, num_parts = 200, 4
    with _v3_flag(True):
        ds = ray.data.range(num_rows).repartition(num_parts)
        ds = ds.materialize()
        num_blocks = ds.num_blocks()
    # Each non-empty reducer emits at least one block.  With small data
    # we won't hit reshape thresholds, so the count tracks num_parts but
    # may be <= when partitions hash to empty.
    assert 1 <= num_blocks <= num_parts * 2, (
        f"unexpected block count {num_blocks} for num_parts={num_parts}"
    )


def test_v3_repartition_with_key(ray_cluster):
    """Keyed repartition: hash on the named column, same row-count
    invariant. ``add_column`` builds rows with a known partition column."""
    num_rows, num_parts = 300, 5
    with _v3_flag(True):
        ds = ray.data.range(num_rows).repartition(num_parts, keys=["id"])
        rows = ds.take_all()
    assert len(rows) == num_rows
    assert {r["id"] for r in rows} == set(range(num_rows))


def test_v3_same_key_lands_in_same_block(ray_cluster):
    """Hash co-location: all rows sharing a key must land in one block."""
    with _v3_flag(True):
        ds = ray.data.range(500, override_num_blocks=10).map(
            lambda row: {"k": row["id"] % 25, "v": row["id"]}
        )
        out = ds.repartition(5, keys=["k"]).materialize()

    _assert_keys_colocated(_keys_per_block(out, ["k"]))
    assert out.count() == 500


def test_v3_multi_column_keys(ray_cluster):
    """Composite keys hash on all columns: every distinct (a, b) tuple lands
    in exactly one block."""
    with _v3_flag(True):
        ds = ray.data.range(500, override_num_blocks=10).map(
            lambda row: {"a": row["id"] % 5, "b": row["id"] % 7, "v": row["id"]}
        )
        out = ds.repartition(4, keys=["a", "b"]).materialize()

    _assert_keys_colocated(_keys_per_block(out, ["a", "b"]))
    assert out.count() == 500


def test_v3_more_partitions_than_keys_emits_empty_blocks(ray_cluster):
    """More partitions than distinct keys → the surplus partitions still
    emit 0-row blocks that carry the dataset schema (empty-partition fast
    path in ``ExternalHashShuffleReduceOp._emit_empty_partition``)."""
    with _v3_flag(True):
        # 3 distinct keys into 50 partitions → at most 3 non-empty blocks.
        ds = ray.data.range(600, override_num_blocks=10).map(
            lambda row: {"k": row["id"] % 3, "v": row["id"]}
        )
        out = ds.repartition(50, keys=["k"]).materialize()

    assert out.count() == 600
    assert out.num_blocks() == 50

    rows_per_block = []
    schemas = []
    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            block = ray.get(block_ref)
            rows_per_block.append(block.num_rows)
            schemas.append(block.schema)

    # At least 47 empty blocks (50 - 3 distinct keys); schemas must all agree.
    assert rows_per_block.count(0) >= 47
    assert all(schema.equals(schemas[0]) for schema in schemas)

    _assert_keys_colocated(_keys_per_block(out, ["k"]))


# ────────────────────────── regression / safety ─────────────────────────


def test_v3_flag_off_keeps_v2_path(ray_cluster):
    """With flag=False, the same call must still work via the v2 path —
    confirms our dispatch doesn't accidentally hijack the default."""
    with _v3_flag(False):
        ds = ray.data.range(200).repartition(4)
        rows = ds.take_all()
    assert len(rows) == 200
    assert {r["id"] for r in rows} == set(range(200))


def test_v3_repartition_sorted(ray_cluster):
    """``repartition(N, keys=[...], sort=True)`` — each output partition's
    rows must be non-decreasing by the sort key (per-partition local
    sort, mirroring v2's contract). Global ordering across partitions is
    NOT guaranteed by hash shuffle."""
    num_rows, num_parts = 300, 4
    with _v3_flag(True):
        ds = (
            ray.data.range(num_rows)
            .repartition(num_parts, keys=["id"], sort=True)
            .materialize()
        )
        # Walk each output block; rows within a block must be sorted.
        # Cross-block ordering is not promised — hash shuffle scatters by
        # hash, not by range.
        rows = ds.take_all()
    assert len(rows) == num_rows
    assert {r["id"] for r in rows} == set(range(num_rows))

    # Per-block monotonicity check — iterate via block-level API so
    # we can verify each block independently.
    for block_ref in ds.get_internal_block_refs():
        block = ray.get(block_ref)
        ids = block["id"].to_pylist()
        assert ids == sorted(ids), f"block not sorted: {ids[:20]}..."


def test_v3_sort_without_keys_rejected(ray_cluster):
    """``sort=True`` with no keys has no meaning — must error clearly."""
    with _v3_flag(True), pytest.raises(ValueError, match="keys"):
        ray.data.range(50).repartition(4, sort=True).materialize()


def test_v3_zero_rows(ray_cluster):
    """Empty source: v3 must complete without hanging or crashing.
    Edge case for the partition_fn / empty-shard path in the reducer."""
    with _v3_flag(True):
        ds = ray.data.range(0).repartition(2)
        rows = ds.take_all()
    assert rows == []


# ─────────────────────────── chained ops ────────────────────────────────


def test_v3_repartition_then_map(ray_cluster):
    """Downstream consumer (map) sees correct schema + row count from v3
    output bundles — catches schema / metadata propagation bugs."""
    with _v3_flag(True):
        ds = (
            ray.data.range(100)
            .repartition(4)
            .map(lambda r: {"id": r["id"], "doubled": r["id"] * 2})
        )
        rows = ds.take_all()
    assert len(rows) == 100
    assert all(r["doubled"] == r["id"] * 2 for r in rows)


def test_v3_two_repartitions_chained(ray_cluster):
    """Two v3 repartitions in a row — verifies bundle plumbing between
    v3 output and v3 input (the same operator on both sides)."""
    with _v3_flag(True):
        ds = ray.data.range(100).repartition(4).repartition(2)
        rows = ds.take_all()
    assert len(rows) == 100
    assert {r["id"] for r in rows} == set(range(100))
