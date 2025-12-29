import numpy as np
import pytest

import ray
from ray.data._internal.logical.optimizers import PhysicalOptimizer
from ray.data._internal.planner import create_planner
from ray.data.block import BlockAccessor
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

RANDOM_SEED = 123


def test_repartition_shuffle(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.sum() == 190

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2._plan.initial_num_blocks() == 5
    assert ds2.sum() == 190

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3._plan.initial_num_blocks() == 20
    assert ds3.sum() == 190

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20, shuffle=True)
    assert large._plan.initial_num_blocks() == 20
    assert large.sum() == 49995000


def test_key_based_repartition_shuffle(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    context = DataContext.get_current()

    context.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    context.hash_shuffle_operator_actor_num_cpus_override = 0.001

    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(3, keys=["id"])
    assert ds2._plan.initial_num_blocks() == 3
    assert ds2.sum() == 190

    ds3 = ds.repartition(5, keys=["id"])
    assert ds3._plan.initial_num_blocks() == 5
    assert ds3.sum() == 190

    large = ray.data.range(10000, override_num_blocks=100)
    large = large.repartition(20, keys=["id"])
    assert large._plan.initial_num_blocks() == 20

    # Assert block sizes distribution
    assert sum(large._block_num_rows()) == 10000
    assert 495 < np.mean(large._block_num_rows()) < 505

    assert large.sum() == 49995000


def test_repartition_noshuffle(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=False)
    assert ds2._plan.initial_num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [4, 4, 4, 4, 4]

    ds3 = ds2.repartition(20, shuffle=False)
    assert ds3._plan.initial_num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [1] * 20

    # Test num_partitions > num_rows
    ds4 = ds.repartition(40, shuffle=False)
    assert ds4._plan.initial_num_blocks() == 40

    assert ds4.sum() == 190
    assert ds4._block_num_rows() == [1] * 20 + [0] * 20

    ds5 = ray.data.range(22).repartition(4)
    assert ds5._plan.initial_num_blocks() == 4
    assert ds5._block_num_rows() == [5, 6, 5, 6]

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20)
    assert large._block_num_rows() == [500] * 20


def test_repartition_shuffle_arrow(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._plan.initial_num_blocks() == 10
    assert ds.count() == 20

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2._plan.initial_num_blocks() == 5
    assert ds2.count() == 20

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3._plan.initial_num_blocks() == 20
    assert ds3.count() == 20

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20, shuffle=True)
    assert large._plan.initial_num_blocks() == 20
    assert large.count() == 10000


@pytest.mark.parametrize(
    "total_rows,target_num_rows_per_block,expected_num_blocks",
    [
        (128, 1, 128),
        (128, 2, 64),
        (128, 4, 32),
        (128, 8, 16),
        (128, 128, 1),
    ],
)
def test_repartition_target_num_rows_per_block(
    ray_start_regular_shared_2_cpus,
    total_rows,
    target_num_rows_per_block,
    expected_num_blocks,
    disable_fallback_to_object_extension,
):
    num_blocks = 16

    # Each block is 8 ints
    ds = ray.data.range(total_rows, override_num_blocks=num_blocks).repartition(
        target_num_rows_per_block=target_num_rows_per_block,
    )

    num_blocks = 0
    num_rows = 0
    all_data = []

    for ref_bundle in ds.iter_internal_ref_bundles():
        block, block_metadata = (
            ray.get(ref_bundle.blocks[0][0]),
            ref_bundle.blocks[0][1],
        )

        # NOTE: Because our block rows % target_num_rows_per_block == 0, we can
        #       assert equality here
        assert block_metadata.num_rows == target_num_rows_per_block

        num_blocks += 1
        num_rows += block_metadata.num_rows

        block_data = (
            BlockAccessor.for_block(block).to_pandas().to_dict(orient="records")
        )
        all_data.extend(block_data)

    # Verify total rows match
    assert num_rows == total_rows
    assert num_blocks == expected_num_blocks

    # Verify data consistency
    all_values = [row["id"] for row in all_data]
    assert sorted(all_values) == list(range(total_rows))


@pytest.mark.parametrize(
    "num_blocks, target_num_rows_per_block, shuffle, expected_exception_msg",
    [
        (
            4,
            10,
            False,
            "Only one of `num_blocks` or `target_num_rows_per_block` must be set, but not both.",
        ),
        (
            None,
            None,
            False,
            "Either `num_blocks` or `target_num_rows_per_block` must be set",
        ),
        (
            None,
            10,
            True,
            "`shuffle` must be False when `target_num_rows_per_block` is set.",
        ),
    ],
)
def test_repartition_invalid_inputs(
    ray_start_regular_shared_2_cpus,
    num_blocks,
    target_num_rows_per_block,
    shuffle,
    expected_exception_msg,
    disable_fallback_to_object_extension,
):
    with pytest.raises(ValueError, match=expected_exception_msg):
        ray.data.range(10).repartition(
            num_blocks=num_blocks,
            target_num_rows_per_block=target_num_rows_per_block,
            shuffle=shuffle,
        )


@pytest.mark.parametrize("shuffle", [True, False])
def test_repartition_empty_datasets(ray_start_regular_shared_2_cpus, shuffle):
    # Test repartitioning an empty dataset with shuffle=True
    num_partitions = 5
    ds_empty = ray.data.range(100).filter(lambda row: False)
    ds_repartitioned = ds_empty.repartition(num_partitions, shuffle=shuffle)

    ref_bundles = list(ds_repartitioned.iter_internal_ref_bundles())
    assert len(ref_bundles) == num_partitions
    for ref_bundle in ref_bundles:
        assert len(ref_bundle.blocks) == 1
        metadata = ref_bundle.blocks[0][1]
        assert metadata.num_rows == 0
        assert metadata.size_bytes == 0


@pytest.mark.parametrize("streaming_repartition_first", [True, False])
@pytest.mark.parametrize("n_target_num_rows", [1, 5])
def test_streaming_repartition_write_with_operator_fusion(
    ray_start_regular_shared_2_cpus,
    tmp_path,
    disable_fallback_to_object_extension,
    streaming_repartition_first,
    n_target_num_rows,
):
    """Test that write with streaming repartition produces exact partitions
    with operator fusion.
    This test verifies:
    * StreamingRepartition and MapBatches operators are fused, with both orders
    """
    target_num_rows = 20

    def fn(batch):
        # Get number of rows from the first column (batch is a dict of column_name -> array)
        num_rows = len(batch["id"])
        assert num_rows == b_s, f"Expected batch size {b_s}, got {num_rows}"
        return batch

    # Configure shuffle strategy
    ctx = DataContext.get_current()
    ctx._shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    num_rows = 100
    partition_col = "skewed_key"

    # Create sample data with skewed partitioning
    # 1 occurs for every 5th row (20 rows), 0 for others (80 rows)
    table = [{"id": n, partition_col: 1 if n % 5 == 0 else 0} for n in range(num_rows)]
    ds = ray.data.from_items(table)

    # Repartition by key to simulate shuffle
    ds = ds.repartition(num_blocks=2, keys=[partition_col])

    # mess up with the block size
    ds = ds.repartition(target_num_rows_per_block=30)

    # Verify fusion of StreamingRepartition and MapBatches operators
    b_s = target_num_rows * n_target_num_rows
    if streaming_repartition_first:
        ds = ds.repartition(target_num_rows_per_block=target_num_rows)
        ds = ds.map_batches(fn, batch_size=b_s)
    else:
        ds = ds.map_batches(fn, batch_size=b_s)
        ds = ds.repartition(target_num_rows_per_block=target_num_rows)
    planner = create_planner()
    physical_plan = planner.plan(ds._logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    if streaming_repartition_first:
        # Not fused
        assert physical_op.name == "MapBatches(fn)"
    else:
        assert (
            physical_op.name
            == f"MapBatches(fn)->StreamingRepartition[num_rows_per_block={target_num_rows}]"
        )

    # Write output to local Parquet files partitioned by key
    ds.write_parquet(path=tmp_path, partition_cols=[partition_col])

    # Verify data can be read back correctly with expected row count
    ds_read_back = ray.data.read_parquet(str(tmp_path))
    assert (
        ds_read_back.count() == num_rows
    ), f"Expected {num_rows} total rows when reading back"

    # Verify per-partition row counts
    partition_0_ds = ray.data.read_parquet(str(tmp_path / f"{partition_col}=0"))
    partition_1_ds = ray.data.read_parquet(str(tmp_path / f"{partition_col}=1"))

    assert partition_0_ds.count() == 80, "Expected 80 rows in partition 0"
    assert partition_1_ds.count() == 20, "Expected 20 rows in partition 1"


def test_streaming_repartition_fusion_output_shape(
    ray_start_regular_shared_2_cpus,
    tmp_path,
    disable_fallback_to_object_extension,
):
    """
    When we use `map_batches -> streaming_repartition`, the output shape should be exactly the same as batch_size.
    """

    def fn(batch):
        # Get number of rows from the first column (batch is a dict of column_name -> array)
        num_rows = len(batch["id"])
        assert num_rows == 20, f"Expected batch size 20, got {num_rows}"
        return batch

    # Configure shuffle strategy
    ctx = DataContext.get_current()
    ctx._shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    num_rows = 100
    partition_col = "skewed_key"

    # Create sample data with skewed partitioning
    # 1 occurs for every 5th row (20 rows), 0 for others (80 rows)
    table = [{"id": n, partition_col: 1 if n % 5 == 0 else 0} for n in range(num_rows)]
    ds = ray.data.from_items(table)

    # Repartition by key to simulate shuffle
    ds = ds.repartition(num_blocks=2, keys=[partition_col])

    # mess up with the block size
    ds = ds.repartition(target_num_rows_per_block=30)

    # Verify fusion of StreamingRepartition and MapBatches operators
    ds = ds.map_batches(fn, batch_size=20)
    ds = ds.repartition(target_num_rows_per_block=20)
    planner = create_planner()
    physical_plan = planner.plan(ds._logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    assert (
        physical_op.name
        == "MapBatches(fn)->StreamingRepartition[num_rows_per_block=20]"
    )

    for block in ds.iter_batches(batch_size=None):
        assert len(block["id"]) == 20


@pytest.mark.parametrize(
    "num_rows,override_num_blocks_list,target_num_rows_per_block",
    [
        (128 * 4, [2, 4, 16], 128),  # testing split, exact and merge blocks
        (
            128 * 4 + 4,
            [2, 4, 16],
            128,
        ),  # Four blocks of 129 rows each, requiring rows to be merged across blocks.
    ],
)
def test_repartition_guarantee_row_num_to_be_exact(
    ray_start_regular_shared_2_cpus,
    num_rows,
    override_num_blocks_list,
    target_num_rows_per_block,
    disable_fallback_to_object_extension,
):
    """Test that repartition with target_num_rows_per_block guarantees exact row counts per block."""
    for override_num_blocks in override_num_blocks_list:
        ds = ray.data.range(num_rows, override_num_blocks=override_num_blocks)
        ds = ds.repartition(
            target_num_rows_per_block=target_num_rows_per_block,
        )
        ds = ds.materialize()

        block_row_counts = [
            metadata.num_rows
            for bundle in ds.iter_internal_ref_bundles()
            for metadata in bundle.metadata
        ]
        # Assert that every block has exactly target_num_rows_per_block rows except at most one
        # block, which may have fewer rows if the total doesn't divide evenly. The smaller block
        # may appear anywhere in the output order, therefore we cannot assume it is last.
        expected_remaining_rows = num_rows % target_num_rows_per_block
        remaining_blocks = [
            c for c in block_row_counts if c != target_num_rows_per_block
        ]

        assert len(remaining_blocks) <= (1 if expected_remaining_rows > 0 else 0), (
            "Expected at most one block with a non-target row count when there is a remainder. "
            f"Found counts {block_row_counts} with target {target_num_rows_per_block}."
        )

        if expected_remaining_rows == 0:
            assert (
                not remaining_blocks
            ), f"All blocks should have exactly {target_num_rows_per_block} rows, got {block_row_counts}."
        elif remaining_blocks:
            assert remaining_blocks[0] == expected_remaining_rows, (
                f"Expected remainder block to have {expected_remaining_rows} rows, "
                f"got {remaining_blocks[0]}. Block counts: {block_row_counts}"
            )


def test_streaming_repartition_with_partial_last_block(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    """Test repartition with target_num_rows_per_block where last block has fewer rows.
    This test verifies:
    1. N-1 blocks have exactly target_num_rows_per_block rows
    2. Only the last block can have fewer rows (remainder)
    """
    # Configure shuffle strategy
    ctx = DataContext.get_current()
    ctx._shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    num_rows = 101

    table = [{"id": n} for n in range(num_rows)]
    ds = ray.data.from_items(table)

    ds = ds.repartition(target_num_rows_per_block=20)

    ds = ds.materialize()

    block_row_counts = []
    for ref_bundle in ds.iter_internal_ref_bundles():
        for _, metadata in ref_bundle.blocks:
            block_row_counts.append(metadata.num_rows)

    assert sum(block_row_counts) == num_rows, f"Expected {num_rows} total rows"

    # Verify that all blocks have 20 rows except one block with 10 rows
    # The block with 10 rows should be the last one
    assert (
        block_row_counts[-1] == 1
    ), f"Expected last block to have 1 row, got {block_row_counts[-1]}"
    assert all(
        count == 20 for count in block_row_counts[:-1]
    ), f"Expected all blocks except last to have 20 rows, got {block_row_counts}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
