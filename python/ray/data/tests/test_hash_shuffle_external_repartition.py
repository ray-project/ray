"""End-to-end tests: ``ds.repartition()`` routed through the external
(file-transport) hash-shuffle variant.

Parity block mirrors ``test_hash_shuffle_v2.py`` — same setup, same
assertions, same names — with ``use_external_hash_shuffle`` flipped on so
we exercise the ExternalHashShuffle{Map,Reduce}Op pair via the real
planner + StreamingExecutor. External-only regressions (flag-off default,
chained ops) live in a second section below.
"""

import pytest

import ray
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403


def _keys_per_block(ds, columns):
    """Return, for each output block, the set of distinct key tuples it holds.

    Used to assert the hash-shuffle co-location guarantee: a key must appear in
    exactly one block.
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


# --- Parity with test_hash_shuffle_v2.py --------------------------------------


@pytest.mark.parametrize("num_partitions", [1, 4, 8])
def test_external_repartition_keys_preserves_rows(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
    num_partitions,
):
    """No rows are lost or duplicated; key totals are preserved."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(1000, override_num_blocks=10)
    out = ds.repartition(num_partitions, keys=["id"])
    assert out.count() == 1000
    assert out.sum("id") == sum(range(1000))


def test_external_repartition_block_number_matched(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All-non-empty partitions => exactly num_partitions output blocks."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    # 1000 distinct keys over 8 buckets => all 8 partitions are non-empty.
    ds = ray.data.range(1000, override_num_blocks=20)
    out = ds.repartition(8, keys=["id"]).materialize()
    assert out.num_blocks() == 8


def test_external_same_key_lands_in_same_block(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All rows sharing a key should end up in one block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 25, "v": row["id"]}
    )
    out = ds.repartition(5, keys=["k"])

    _assert_keys_colocated(_keys_per_block(out, ["k"]))
    assert out.count() == 500


def test_external_multi_column_keys(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Composite keys hash on all columns: every distinct (a, b) tuple lands in
    exactly one block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"a": row["id"] % 5, "b": row["id"] % 7, "v": row["id"]}
    )
    out = ds.repartition(4, keys=["a", "b"])

    _assert_keys_colocated(_keys_per_block(out, ["a", "b"]))
    assert out.count() == 500


def test_external_more_partitions_than_keys_emits_empty_blocks(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Requesting more partitions than there are distinct keys emits the extra
    partitions as empty (0-row) blocks that still carry the dataset schema."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    # 3 distinct keys into 50 partitions => at most 3 non-empty, >=47 empty.
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

    assert rows_per_block.count(0) >= 47
    assert all(schema.equals(schemas[0]) for schema in schemas)

    _assert_keys_colocated(_keys_per_block(out, ["k"]))


def test_external_repartition_empty_dataset(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Empty dataset should still output N blocks"""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(100, override_num_blocks=4).filter(lambda row: False)
    out = ds.repartition(4, keys=["id"]).materialize()
    assert out.count() == 0
    assert out.num_blocks() == 4
    rows_per_block = [
        ray.get(block_ref).num_rows
        for ref_bundle in out.iter_internal_ref_bundles()
        for block_ref in ref_bundle.block_refs
    ]
    assert rows_per_block == [0, 0, 0, 0]


def test_external_repartition_with_sort_produces_sorted_partitions(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Check that rows are sorted in every partition."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(200, override_num_blocks=4)
    out = ds.repartition(4, keys=["id"], sort=True)

    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            ids = ray.get(block_ref)["id"].to_pylist()
            assert ids == sorted(ids)


# --- External-only: regression + chained ops ---------------------------------


def test_external_flag_off_keeps_v2_path(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """With flag=False the same call must still work via the v2 path —
    confirms our dispatch doesn't accidentally hijack the default."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = False

    ds = ray.data.range(200).repartition(4, keys=["id"])
    assert ds.count() == 200


def test_external_repartition_then_map(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Downstream consumer (map) sees correct schema + row count from external
    output bundles — catches schema / metadata propagation bugs."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    ds = (
        ray.data.range(100)
        .repartition(4, keys=["id"])
        .map(lambda r: {"id": r["id"], "doubled": r["id"] * 2})
    )
    rows = ds.take_all()
    assert len(rows) == 100
    assert all(r["doubled"] == r["id"] * 2 for r in rows)


def test_external_two_repartitions_chained(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Two external repartitions in a row — verifies bundle plumbing between
    external output and external input (the same operator on both sides)."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = True

    ds = ray.data.range(100).repartition(4, keys=["id"]).repartition(2, keys=["id"])
    rows = ds.take_all()
    assert len(rows) == 100
    assert {r["id"] for r in rows} == set(range(100))
