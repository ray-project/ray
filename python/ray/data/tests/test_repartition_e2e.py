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
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2._plan.initial_num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3._plan.initial_num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [2] * 10 + [0] * 10

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


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
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2._plan.initial_num_blocks() == 5
    assert ds2.count() == 20
    assert ds2._block_num_rows() == [10, 10, 0, 0, 0]

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3._plan.initial_num_blocks() == 20
    assert ds3.count() == 20
    assert ds3._block_num_rows() == [2] * 10 + [0] * 10

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20, shuffle=True)
    assert large._block_num_rows() == [500] * 20


@pytest.mark.parametrize(
    "total_rows,target_num_rows_per_block",
    [
        (128, 1),
        (128, 2),
        (128, 4),
        (128, 8),
        (128, 128),
    ],
)
def test_repartition_target_num_rows_per_block(
    ray_start_regular_shared_2_cpus,
    total_rows,
    target_num_rows_per_block,
    disable_fallback_to_object_extension,
):
    ds = ray.data.range(total_rows).repartition(
        target_num_rows_per_block=target_num_rows_per_block,
    )
    rows_count = 0
    all_data = []
    for ref_bundle in ds.iter_internal_ref_bundles():
        block, block_metadata = (
            ray.get(ref_bundle.blocks[0][0]),
            ref_bundle.blocks[0][1],
        )
        assert block_metadata.num_rows <= target_num_rows_per_block
        rows_count += block_metadata.num_rows
        block_data = (
            BlockAccessor.for_block(block).to_pandas().to_dict(orient="records")
        )
        all_data.extend(block_data)

    assert rows_count == total_rows

    # Verify total rows match
    assert rows_count == total_rows

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


def test_streaming_repartition_write_no_operator_fusion(
    ray_start_regular_shared_2_cpus, tmp_path, disable_fallback_to_object_extension
):
    """Test that write with streaming repartition produces exact partitions
    without operator fusion.
    This test verifies:
    1. StreamingRepartition and Write operators are not fused
    2. Exact partition structure is maintained
    3. Skewed data is properly distributed across partitions
    """

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

    # Further rebalance to meet target row size
    ds = ds.repartition(target_num_rows_per_block=20)

    # Verify non-fusion of map_batches with repartition
    ds = ds.map_batches(lambda x: x)
    planner = create_planner()
    physical_plan = planner.plan(ds._logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    assert physical_op.name == "MapBatches(<lambda>)"
    assert len(physical_op.input_dependencies) == 1

    # Verify that StreamingRepartition physical operator has supports_fusion=False
    up_physical_op = physical_op.input_dependencies[0]
    assert up_physical_op.name == "StreamingRepartition"
    assert not getattr(
        up_physical_op, "_supports_fusion", True
    ), "StreamingRepartition should have supports_fusion=False"

    # Write output to local Parquet files partitioned by key
    ds.write_parquet(path=tmp_path, partition_cols=[partition_col])

    # Verify exact number of files created based on target_num_rows_per_block=20
    # 80 rows with key=0 should create 4 files (80/20=4)
    # 20 rows with key=1 should create 1 file (20/20=1)
    # Total should be 5 files
    # Note: Partition column values are returned as strings when reading partitioned Parquet
    partition_0_files = list((tmp_path / f"{partition_col}=0").glob("*.parquet"))
    partition_1_files = list((tmp_path / f"{partition_col}=1").glob("*.parquet"))

    assert (
        len(partition_0_files) == 4
    ), f"Expected 4 files in partition 0, got {len(partition_0_files)}"
    assert (
        len(partition_1_files) == 1
    ), f"Expected 1 file in partition 1, got {len(partition_1_files)}"

    total_files = len(partition_0_files) + len(partition_1_files)
    assert (
        total_files == 5
    ), f"Expected exactly 5 parquet files total, got {total_files}"

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
