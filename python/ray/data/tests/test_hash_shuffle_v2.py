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


@pytest.mark.parametrize("num_partitions", [1, 4, 8])
def test_repartition_keys_preserves_rows(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
    num_partitions,
):
    """No rows are lost or duplicated; key totals are preserved."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(1000, override_num_blocks=10)
    out = ds.repartition(num_partitions, keys=["id"])
    assert out.count() == 1000
    assert out.sum("id") == sum(range(1000))


def test_repartition_block_number_matched(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All-non-empty partitions => exactly num_partitions output blocks."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    # 1000 distinct keys over 8 buckets => all 8 partitions are non-empty.
    ds = ray.data.range(1000, override_num_blocks=20)
    out = ds.repartition(8, keys=["id"]).materialize()
    assert out.num_blocks() == 8


def test_same_key_lands_in_same_block(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """All rows sharing a key end up in one block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 25, "v": row["id"]}
    )
    out = ds.repartition(5, keys=["k"])

    _assert_keys_colocated(_keys_per_block(out, ["k"]))
    assert out.count() == 500


def test_multi_column_keys(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Composite keys hash on all columns: every distinct (a, b) tuple lands in
    exactly one block."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(500, override_num_blocks=10).map(
        lambda row: {"a": row["id"] % 5, "b": row["id"] % 7, "v": row["id"]}
    )
    out = ds.repartition(4, keys=["a", "b"])

    _assert_keys_colocated(_keys_per_block(out, ["a", "b"]))
    assert out.count() == 500


def test_repartition_empty_dataset(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Repartitioning an all-empty must not crash; it exercises the zero-row map path and preserves the empty count."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(100, override_num_blocks=4).filter(lambda row: False)
    out = ds.repartition(4, keys=["id"]).materialize()
    assert out.count() == 0


def test_repartition_with_sort_produces_sorted_partitions(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Check that rows are sorted in every partition."""
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    ds = ray.data.range(200, override_num_blocks=4)
    out = ds.repartition(4, keys=["id"], sort=True)

    for ref_bundle in out.iter_internal_ref_bundles():
        for block_ref in ref_bundle.block_refs:
            ids = ray.get(block_ref)["id"].to_pylist()
            assert ids == sorted(ids)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
