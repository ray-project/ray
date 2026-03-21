"""Tests for the actorless hash shuffle operator."""
import pytest

import ray
from ray.data.context import DataContext, ShuffleStrategy


@pytest.fixture(autouse=True)
def setup_shuffle_strategy():
    ctx = DataContext.get_current()
    original = ctx.shuffle_strategy
    ctx.shuffle_strategy = ShuffleStrategy.ACTORLESS_HASH_SHUFFLE
    yield
    ctx.shuffle_strategy = original


def _sum_dataset(ds, col="id"):
    """Sum a column without triggering aggregate's shuffle strategy assertion."""
    return sum(r[col] for r in ds.take_all())


# ---------------------------------------------------------------------------
# No-compactor mode tests
# ---------------------------------------------------------------------------


def test_basic_repartition():
    ds = ray.data.range(100).repartition(4, shuffle=True)
    assert _sum_dataset(ds) == sum(range(100))
    assert ds.materialize().num_blocks() == 4


def test_increase_partitions():
    ds = ray.data.range(50).repartition(10, shuffle=True)
    assert _sum_dataset(ds) == sum(range(50))


def test_decrease_partitions():
    ds = ray.data.range(100).repartition(2, shuffle=True)
    assert _sum_dataset(ds) == sum(range(100))
    assert ds.materialize().num_blocks() == 2


def test_large_dataset():
    n = 10_000
    ds = ray.data.range(n).repartition(8, shuffle=True)
    assert _sum_dataset(ds) == sum(range(n))


def test_deterministic_partition():
    ds1 = ray.data.range(100).repartition(4, shuffle=True)
    ds2 = ray.data.range(100).repartition(4, shuffle=True)
    for b1, b2 in zip(ds1.iter_batches(), ds2.iter_batches()):
        assert list(b1["id"]) == list(b2["id"])


def test_sort():
    ds = ray.data.range(100).sort("id")
    blocks = ds.get_internal_block_refs()
    for block_ref in blocks:
        block = ray.get(block_ref)
        col = block.column("id").to_pylist()
        assert col == sorted(col), f"Block not sorted: {col}"


def test_single_partition():
    ds = ray.data.range(50).repartition(1, shuffle=True)
    assert _sum_dataset(ds) == sum(range(50))
    assert ds.materialize().num_blocks() == 1


def test_empty_dataset():
    ds = ray.data.range(0).repartition(4, shuffle=True)
    assert ds.count() == 0


# ---------------------------------------------------------------------------
# Compaction mode tests
# ---------------------------------------------------------------------------


def test_compaction_basic():
    """Compaction with a low threshold should produce correct results."""
    ctx = DataContext.get_current()
    ctx.set_config("actorless_shuffle_compaction_threshold", 2)
    try:
        ds = ray.data.range(100, override_num_blocks=20).repartition(4, shuffle=True)
        assert _sum_dataset(ds) == sum(range(100))
        assert ds.materialize().num_blocks() == 4
    finally:
        ctx.set_config("actorless_shuffle_compaction_threshold", 10)


def test_compaction_disabled():
    """Threshold=0 disables compaction; results should still be correct."""
    ctx = DataContext.get_current()
    ctx.set_config("actorless_shuffle_compaction_threshold", 0)
    try:
        ds = ray.data.range(100, override_num_blocks=10).repartition(4, shuffle=True)
        assert _sum_dataset(ds) == sum(range(100))
    finally:
        ctx.set_config("actorless_shuffle_compaction_threshold", 10)


def test_compaction_large():
    """Compaction with many input blocks and low threshold."""
    ctx = DataContext.get_current()
    ctx.set_config("actorless_shuffle_compaction_threshold", 3)
    try:
        n = 5_000
        ds = ray.data.range(n, override_num_blocks=50).repartition(8, shuffle=True)
        assert _sum_dataset(ds) == sum(range(n))
    finally:
        ctx.set_config("actorless_shuffle_compaction_threshold", 10)


def test_compaction_with_sort():
    """Compaction + sort should produce correctly sorted partitions."""
    ctx = DataContext.get_current()
    ctx.set_config("actorless_shuffle_compaction_threshold", 2)
    try:
        ds = ray.data.range(100, override_num_blocks=20).sort("id")
        blocks = ds.get_internal_block_refs()
        for block_ref in blocks:
            block = ray.get(block_ref)
            col = block.column("id").to_pylist()
            assert col == sorted(col), f"Block not sorted: {col}"
    finally:
        ctx.set_config("actorless_shuffle_compaction_threshold", 10)


# ---------------------------------------------------------------------------
# Pre-map merge tests
# ---------------------------------------------------------------------------


@pytest.fixture
def pre_map_merge_ctx():
    """Enable pre-map merge with a low threshold for testing."""
    ctx = DataContext.get_current()
    ctx.set_config("actorless_shuffle_compaction_strategy", "pre_map_merge")
    # 1 byte threshold forces a flush after every input bundle.
    # Use a realistic threshold (e.g., 1 KB) to batch multiple blocks.
    yield ctx
    ctx.set_config("actorless_shuffle_compaction_strategy", "none")
    ctx.remove_config("actorless_shuffle_pre_map_merge_threshold")


def test_pre_map_merge_basic(pre_map_merge_ctx):
    """Pre-map merge with a small threshold should produce correct results."""
    pre_map_merge_ctx.set_config(
        "actorless_shuffle_pre_map_merge_threshold", 1024
    )  # 1 KB
    ds = ray.data.range(100, override_num_blocks=20).repartition(4, shuffle=True)
    assert _sum_dataset(ds) == sum(range(100))
    assert ds.materialize().num_blocks() == 4


def test_pre_map_merge_large_threshold(pre_map_merge_ctx):
    """Large threshold merges all blocks into one map task."""
    pre_map_merge_ctx.set_config(
        "actorless_shuffle_pre_map_merge_threshold", 1024 * 1024 * 1024
    )  # 1 GB
    n = 5_000
    ds = ray.data.range(n, override_num_blocks=50).repartition(8, shuffle=True)
    assert _sum_dataset(ds) == sum(range(n))


def test_pre_map_merge_with_sort(pre_map_merge_ctx):
    """Pre-map merge + sort should produce correctly sorted partitions."""
    pre_map_merge_ctx.set_config(
        "actorless_shuffle_pre_map_merge_threshold", 1024
    )  # 1 KB
    ds = ray.data.range(100, override_num_blocks=20).sort("id")
    blocks = ds.get_internal_block_refs()
    for block_ref in blocks:
        block = ray.get(block_ref)
        col = block.column("id").to_pylist()
        assert col == sorted(col), f"Block not sorted: {col}"


def test_pre_map_merge_single_block(pre_map_merge_ctx):
    """Single input block should work fine with pre-map merge."""
    pre_map_merge_ctx.set_config(
        "actorless_shuffle_pre_map_merge_threshold", 1024
    )  # 1 KB
    ds = ray.data.range(50, override_num_blocks=1).repartition(4, shuffle=True)
    assert _sum_dataset(ds) == sum(range(50))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
