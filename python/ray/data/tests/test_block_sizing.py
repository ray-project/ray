import pytest

import ray
from ray.data.block import BlockAccessor
from ray.data.context import DataContext
from ray.data.dataset import Dataset
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.conftest import (
    assert_blocks_expected_in_plasma,
    get_initial_core_execution_metrics_snapshot,
)
from ray.tests.conftest import *  # noqa


def test_map(shutdown_only, restore_data_context):
    ray.init(
        _system_config={
            "max_direct_call_object_size": 10_000,
        },
        num_cpus=2,
        object_store_memory=int(100e6),
    )

    ctx = DataContext.get_current()
    ctx.target_min_block_size = 10_000 * 8
    ctx.target_max_block_size = 10_000 * 8
    num_blocks_expected = 10
    last_snapshot = get_initial_core_execution_metrics_snapshot()

    # Test read.
    ds = ray.data.range(100_000, override_num_blocks=1).materialize()
    assert (
        num_blocks_expected <= ds._plan.initial_num_blocks() <= num_blocks_expected + 1
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected,
        block_size_expected=ctx.target_max_block_size,
    )

    # Test read -> map.
    # NOTE(swang): For some reason BlockBuilder's estimated memory usage when a
    # map fn is used is 2x the actual memory usage.
    ds = (
        ray.data.range(100_000, override_num_blocks=1)
        .map(lambda row: row)
        .materialize()
    )
    assert (
        num_blocks_expected * 2
        <= ds._plan.initial_num_blocks()
        <= num_blocks_expected * 2 + 1
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected * 2,
        block_size_expected=ctx.target_max_block_size // 2,
    )

    # Test adjusted block size.
    ctx.target_max_block_size *= 2
    num_blocks_expected //= 2

    # Test read.
    ds = ray.data.range(100_000, override_num_blocks=1).materialize()
    assert (
        num_blocks_expected <= ds._plan.initial_num_blocks() <= num_blocks_expected + 1
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected,
        block_size_expected=ctx.target_max_block_size,
    )

    # Test read -> map.
    ds = (
        ray.data.range(100_000, override_num_blocks=1)
        .map(lambda row: row)
        .materialize()
    )
    assert (
        num_blocks_expected * 2
        <= ds._plan.initial_num_blocks()
        <= num_blocks_expected * 2 + 1
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected * 2,
        block_size_expected=ctx.target_max_block_size // 2,
    )

    # Setting the shuffle block size prints a warning and actually resets
    # target_max_block_size
    ctx.target_shuffle_max_block_size = ctx.target_max_block_size / 2
    num_blocks_expected *= 2

    # Test read.
    ds = ray.data.range(100_000, override_num_blocks=1).materialize()
    assert (
        num_blocks_expected <= ds._plan.initial_num_blocks() <= num_blocks_expected + 1
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected,
        block_size_expected=ctx.target_max_block_size,
    )

    # Test read -> map.
    ds = (
        ray.data.range(100_000, override_num_blocks=1)
        .map(lambda row: row)
        .materialize()
    )

    # NOTE: `initial_num_blocks` is based on estimate, hence we bake in 50% margin
    assert (
        num_blocks_expected * 2
        <= ds._plan.initial_num_blocks()
        <= num_blocks_expected * 3
    )

    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_blocks_expected * 2,
        block_size_expected=ctx.target_max_block_size // 2,
    )


# TODO: Test that map stage output blocks are the correct size for groupby and
# repartition. Currently we only have access to the reduce stage output block
# size.
SHUFFLE_ALL_TO_ALL_OPS = [
    (Dataset.random_shuffle, {}, True),
    (Dataset.sort, {"key": "id"}, False),
]


@pytest.mark.parametrize(
    "shuffle_op",
    SHUFFLE_ALL_TO_ALL_OPS,
)
def test_shuffle(shutdown_only, restore_data_context, shuffle_op):
    ray.init(
        _system_config={
            "max_direct_call_object_size": 250,
        },
        num_cpus=2,
        object_store_memory=int(100e6),
    )

    # Test AllToAll and Map -> AllToAll Datasets. Check that Map inherits
    # AllToAll's target block size.
    ctx = DataContext.get_current()
    ctx.read_op_min_num_blocks = 1
    ctx.target_min_block_size = 1

    N = 100_000
    mem_size = 800_000

    shuffle_fn, kwargs, fusion_supported = shuffle_op

    ctx.target_max_block_size = 10_000 * 8
    num_blocks_expected = mem_size // ctx.target_max_block_size
    last_snapshot = get_initial_core_execution_metrics_snapshot()

    ds = shuffle_fn(ray.data.range(N), **kwargs).materialize()
    assert (
        num_blocks_expected
        <= ds._plan.initial_num_blocks()
        <= num_blocks_expected * 1.5
    )

    def _estimate_intermediate_blocks(fusion_supported: bool, num_blocks_expected: int):
        return num_blocks_expected**2 + num_blocks_expected * (
            2 if fusion_supported else 4
        )

    # map * reduce intermediate blocks + 1 metadata ref per map/reduce task.
    # If fusion is not supported, the un-fused map stage produces 1 data and 1
    # metadata per task.
    num_intermediate_blocks = _estimate_intermediate_blocks(
        fusion_supported, num_blocks_expected
    )

    print(f">>> Asserting {num_intermediate_blocks} blocks are in plasma")

    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        # Dataset.sort produces some empty intermediate blocks because the
        # input range is already partially sorted.
        num_intermediate_blocks,
    )

    ds = shuffle_fn(ray.data.range(N).map(lambda x: x), **kwargs).materialize()
    if not fusion_supported:
        # TODO(swang): For some reason BlockBuilder's estimated
        # memory usage for range(1000)->map is 2x the actual memory usage.
        # Remove once https://github.com/ray-project/ray/issues/40246 is fixed.
        num_blocks_expected = int(num_blocks_expected * 2.2)
    assert (
        num_blocks_expected
        <= ds._plan.initial_num_blocks()
        <= num_blocks_expected * 1.5
    )
    num_intermediate_blocks = _estimate_intermediate_blocks(
        fusion_supported, num_blocks_expected
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        # Dataset.sort produces some empty intermediate blocks because the
        # input range is already partially sorted.
        num_intermediate_blocks,
    )

    ctx.target_max_block_size //= 2
    num_blocks_expected = mem_size // ctx.target_max_block_size
    block_size_expected = ctx.target_max_block_size

    ds = shuffle_fn(ray.data.range(N), **kwargs).materialize()
    assert (
        num_blocks_expected
        <= ds._plan.initial_num_blocks()
        <= num_blocks_expected * 1.5
    )
    num_intermediate_blocks = _estimate_intermediate_blocks(
        fusion_supported, num_blocks_expected
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_intermediate_blocks,
    )

    ds = shuffle_fn(ray.data.range(N).map(lambda x: x), **kwargs).materialize()
    if not fusion_supported:
        num_blocks_expected = int(num_blocks_expected * 2.2)
        block_size_expected //= 2.2
    assert (
        num_blocks_expected
        <= ds._plan.initial_num_blocks()
        <= num_blocks_expected * 1.5
    )
    num_intermediate_blocks = _estimate_intermediate_blocks(
        fusion_supported, num_blocks_expected
    )
    last_snapshot = assert_blocks_expected_in_plasma(
        last_snapshot,
        num_intermediate_blocks,
    )

    # Setting target max block size does not affect map ops when there is a
    # shuffle downstream.
    ctx.target_max_block_size = ctx.target_max_block_size * 2
    num_blocks_expected //= 2

    ds = shuffle_fn(ray.data.range(N).map(lambda x: x), **kwargs).materialize()
    assert (
        num_blocks_expected
        <= ds._plan.initial_num_blocks()
        <= num_blocks_expected * 1.5
    )

    num_intermediate_blocks = _estimate_intermediate_blocks(
        fusion_supported, num_blocks_expected
    )

    assert_blocks_expected_in_plasma(
        last_snapshot,
        num_intermediate_blocks,
    )


def test_target_max_block_size_infinite_or_default_disables_splitting_globally(
    shutdown_only, restore_data_context
):
    """Test that setting target_max_block_size to None disables block splitting globally."""
    ray.init(num_cpus=2)

    # Create a large dataset that would normally trigger block splitting
    N = 1_000_000  # ~8MB worth of data

    # First, test with normal target_max_block_size (should split into multiple blocks)
    ctx = DataContext.get_current()
    ctx.target_max_block_size = 1_000_000  # ~1MB

    ds_with_limit = ray.data.range(N, override_num_blocks=1).materialize()
    blocks_with_limit = ds_with_limit._plan.initial_num_blocks()

    # Now test with target_max_block_size = None (should not split)
    ctx.target_max_block_size = None  # Disable block size limit

    ds_unlimited = (
        ray.data.range(N, override_num_blocks=1).map(lambda x: x).materialize()
    )
    blocks_unlimited = ds_unlimited._plan.initial_num_blocks()

    # Verify that unlimited creates fewer blocks (no splitting)
    assert blocks_unlimited <= blocks_with_limit
    # With target_max_block_size=None, it should maintain the original block structure
    assert blocks_unlimited == 1


@pytest.mark.parametrize(
    "compute,mode",
    [
        (ray.data.TaskPoolStrategy(), "tasks"),
        (ray.data.ActorPoolStrategy(min_size=1, max_size=1), "actors"),
    ],
)
def test_target_max_block_rows(shutdown_only, restore_data_context, compute, mode):
    """Test that target_max_block_rows limits block size by row count."""
    ray.init(num_cpus=2)
    ctx = DataContext.get_current()
    
    # Disable byte-based blocking to isolate row-based blocking
    ctx.target_max_block_size = None
    
    # Set a small row limit
    rows_per_block = 10
    ctx.target_max_block_rows = rows_per_block
    
    if mode == "tasks":
        fn = lambda x: x
    else:
        class Identity:
            def __call__(self, row):
                return row
        fn = Identity

    total_rows = 100
    # Create 1 block initially
    ds = ray.data.range(total_rows, override_num_blocks=1)
    
    # Force materialization to trigger block splitting
    ds = ds.materialize()
    
    # Check if we have approximately total_rows / rows_per_block blocks
    # Note: range() might produce more blocks if it splits internally, 
    # but materialize should respect the limit if it splits.
    # However, Read ops might not fully respect target_max_block_rows if the reader doesn't support it,
    # but the subsequent map/materialize should.
    
    # Let's verify with map to ensure we are testing the map transformer/block builder logic
    ds = ray.data.range(total_rows, override_num_blocks=1).map(fn, compute=compute).materialize()
    
    expected_blocks = total_rows // rows_per_block
    assert ds.num_blocks() >= expected_blocks
    
    for block_ref in ds.get_internal_block_refs():
        block = ray.get(block_ref)
        assert BlockAccessor.for_block(block).num_rows() <= rows_per_block


def test_read_target_max_block_rows(ray_start_regular_shared, tmp_path, restore_data_context):
    ctx = DataContext.get_current()
    # Disable byte-based blocking
    ctx.target_max_block_size = None
    rows_per_block = 10
    ctx.target_max_block_rows = rows_per_block

    # Create a CSV file with 100 rows
    path = str(tmp_path / "test.csv")
    ds = ray.data.range(100, override_num_blocks=1)
    ds.write_csv(path)

    # Read back with limit
    ds_read = ray.data.read_csv(path)
    
    # Materialize to trigger execution and block splitting
    ds_read = ds_read.materialize()
    
    # Check if blocks are split correctly
    expected_blocks = 100 // rows_per_block
    assert ds_read.num_blocks() >= expected_blocks

    for block_ref in ds_read.get_internal_block_refs():
        block = ray.get(block_ref)
        assert BlockAccessor.for_block(block).num_rows() <= rows_per_block
