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
    assert ds._logical_plan.initial_num_blocks() == 10
    assert ds.sum() == 190

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2._logical_plan.initial_num_blocks() == 5
    assert ds2.sum() == 190

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3._logical_plan.initial_num_blocks() == 20
    assert ds3.sum() == 190

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20, shuffle=True)
    assert large._logical_plan.initial_num_blocks() == 20
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
    assert ds._logical_plan.initial_num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(3, keys=["id"])
    assert ds2._logical_plan.initial_num_blocks() == 3
    assert ds2.sum() == 190

    ds3 = ds.repartition(5, keys=["id"])
    assert ds3._logical_plan.initial_num_blocks() == 5
    assert ds3.sum() == 190

    large = ray.data.range(10000, override_num_blocks=100)
    large = large.repartition(20, keys=["id"])
    assert large._logical_plan.initial_num_blocks() == 20

    # Assert block sizes distribution
    assert sum(large._block_num_rows()) == 10000
    assert 495 < np.mean(large._block_num_rows()) < 505

    assert large.sum() == 49995000


def test_repartition_noshuffle(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._logical_plan.initial_num_blocks() == 10
    assert ds.sum() == 190
    assert ds._block_num_rows() == [2] * 10

    ds2 = ds.repartition(5, shuffle=False)
    assert ds2._logical_plan.initial_num_blocks() == 5
    assert ds2.sum() == 190
    assert ds2._block_num_rows() == [4, 4, 4, 4, 4]

    ds3 = ds2.repartition(20, shuffle=False)
    assert ds3._logical_plan.initial_num_blocks() == 20
    assert ds3.sum() == 190
    assert ds3._block_num_rows() == [1] * 20

    # Test num_partitions > num_rows
    ds4 = ds.repartition(40, shuffle=False)
    assert ds4._logical_plan.initial_num_blocks() == 40

    assert ds4.sum() == 190
    assert ds4._block_num_rows() == [1] * 20 + [0] * 20

    ds5 = ray.data.range(22).repartition(4)
    assert ds5._logical_plan.initial_num_blocks() == 4
    assert ds5._block_num_rows() == [5, 6, 5, 6]

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20)
    assert large._block_num_rows() == [500] * 20


def test_repartition_shuffle_arrow(
    ray_start_regular_shared_2_cpus, disable_fallback_to_object_extension
):
    ds = ray.data.range(20, override_num_blocks=10)
    assert ds._logical_plan.initial_num_blocks() == 10
    assert ds.count() == 20

    ds2 = ds.repartition(5, shuffle=True)
    assert ds2._logical_plan.initial_num_blocks() == 5
    assert ds2.count() == 20

    ds3 = ds2.repartition(20, shuffle=True)
    assert ds3._logical_plan.initial_num_blocks() == 20
    assert ds3.count() == 20

    large = ray.data.range(10000, override_num_blocks=10)
    large = large.repartition(20, shuffle=True)
    assert large._logical_plan.initial_num_blocks() == 20
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
        strict=True,
    )

    num_blocks = 0
    num_rows = 0
    all_data = []

    for ref_bundle in ds.iter_internal_ref_bundles():
        first = ref_bundle.blocks[0]
        block, block_metadata = ray.get(first.ref), first.metadata

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
        metadata = ref_bundle.blocks[0].metadata
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
    ds = ds.repartition(target_num_rows_per_block=30, strict=True)

    # Verify fusion of StreamingRepartition and MapBatches operators
    b_s = target_num_rows * n_target_num_rows
    if streaming_repartition_first:
        ds = ds.repartition(target_num_rows_per_block=target_num_rows, strict=True)
        ds = ds.map_batches(fn, batch_size=b_s)
    else:
        ds = ds.map_batches(fn, batch_size=b_s)
        ds = ds.repartition(target_num_rows_per_block=target_num_rows, strict=True)
    planner = create_planner()
    physical_plan, _ = planner.plan(ds._logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    if streaming_repartition_first:
        # Not fused
        assert physical_op.name == "MapBatches(fn)"
    else:
        assert (
            physical_op.name
            == f"MapBatches(fn)->StreamingRepartition[num_rows_per_block={target_num_rows},strict=True]"
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
    ds = ds.repartition(target_num_rows_per_block=30, strict=True)

    # Verify fusion of StreamingRepartition and MapBatches operators
    ds = ds.map_batches(fn, batch_size=20)
    ds = ds.repartition(target_num_rows_per_block=20, strict=True)
    planner = create_planner()
    physical_plan, _ = planner.plan(ds._logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    assert (
        physical_op.name
        == "MapBatches(fn)->StreamingRepartition[num_rows_per_block=20,strict=True]"
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
            strict=True,
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
    2. Exactly one block has fewer rows, and it can appear anywhere
       in the output, StreamingRepartition does not guarantee ordering.
    """
    # Configure shuffle strategy
    ctx = DataContext.get_current()
    ctx._shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    num_rows = 101
    target_num_rows_per_block = 20

    table = [{"id": n} for n in range(num_rows)]
    ds = ray.data.from_items(table)

    ds = ds.repartition(
        target_num_rows_per_block=target_num_rows_per_block, strict=True
    )

    ds = ds.materialize()

    block_row_counts = []
    for ref_bundle in ds.iter_internal_ref_bundles():
        for entry in ref_bundle.blocks:
            block_row_counts.append(entry.metadata.num_rows)

    assert sum(block_row_counts) == num_rows, f"Expected {num_rows} total rows"

    # Verify that all blocks have 20 rows except one block with 1 row
    # The smaller block may appear anywhere in the output order
    remainder_blocks = [c for c in block_row_counts if c != target_num_rows_per_block]
    assert (
        len(remainder_blocks) == 1
    ), f"Expected exactly one remainder block, got {block_row_counts}"
    assert remainder_blocks[0] == num_rows % target_num_rows_per_block, (
        f"Expected remainder block to have {num_rows % target_num_rows_per_block} rows, "
        f"got {remainder_blocks[0]}. Block counts: {block_row_counts}"
    )


def test_streaming_repartition_non_strict_mode(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """Test non-strict mode streaming repartition behavior.

    This test verifies:
    1. Non-strict mode produces at most 1 block < target per input block
    2. No stitching across input blocks
    """
    num_rows = 100
    target = 20

    # Create dataset with varying block sizes
    ds = ray.data.range(num_rows, override_num_blocks=10)  # 10 blocks of 10 rows each

    # Non-strict mode: should split each input block independently
    ds_non_strict = ds.repartition(target_num_rows_per_block=target, strict=False)
    ds_non_strict = ds_non_strict.materialize()

    # Collect block row counts
    block_row_counts = [
        metadata.num_rows
        for bundle in ds_non_strict.iter_internal_ref_bundles()
        for metadata in bundle.metadata
    ]

    # Verify non-strict mode behavior: no stitching across input blocks
    # For non-strict mode with input blocks of 10 rows and target of 20:
    # Each input block (10 rows) should produce exactly 1 block of 10 rows
    # (since 10 < 20, no splitting needed, and no stitching with other blocks)
    assert sum(block_row_counts) == num_rows, f"Expected {num_rows} total rows"
    assert (
        len(block_row_counts) == 10
    ), f"Expected 10 blocks, got {len(block_row_counts)}"
    assert all(
        count == 10 for count in block_row_counts
    ), f"Expected all blocks to have 10 rows (no stitching), got {block_row_counts}"


@pytest.mark.parametrize("batch_size", [30, 35, 45])
def test_streaming_repartition_fusion_non_strict(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    batch_size,
):
    """Test that non-strict mode can fuse with any batch_size.

    This test verifies:
    1. MapBatches -> StreamingRepartition(strict=False) can fuse regardless of batch_size
    """
    num_rows = 100
    target = 20

    def fn(batch):
        # Just pass through, but verify we got data
        assert len(batch["id"]) > 0, "Batch should not be empty"
        return batch

    # Create dataset with 10 blocks (10 rows each) to ensure varied input block sizes
    ds = ray.data.range(num_rows, override_num_blocks=10)

    # Non-strict mode should fuse even when batch_size % target != 0
    ds = ds.map_batches(fn, batch_size=batch_size)
    ds = ds.repartition(target_num_rows_per_block=target, strict=False)

    # Verify fusion happened
    planner = create_planner()
    physical_plan, _ = planner.plan(ds._logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert (
        f"MapBatches(fn)->StreamingRepartition[num_rows_per_block={target},strict=False]"
        in physical_op.name
    ), (
        f"Expected fusion for batch_size={batch_size}, target={target}, "
        f"but got operator name: {physical_op.name}"
    )

    # Verify correctness: count total rows and verify output block sizes
    assert ds.count() == num_rows, f"Expected {num_rows} rows"

    # In non-strict mode, blocks are NOT guaranteed to be exactly target size
    # because no stitching happens across input blocks from map_batches.
    # Just verify that data is preserved correctly.
    result = sorted([row["id"] for row in ds.take_all()])
    expected = list(range(num_rows))
    assert result == expected, "Data should be preserved correctly after fusion"


@pytest.mark.timeout(60)
def test_streaming_repartition_empty_dataset(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """Test streaming repartition with empty dataset (0 rows).

    This test reproduces the scenario where:
    1. Upstream produces empty results (e.g., filter, map, etc.)
    2. Repartition with target_num_rows_per_block is applied

    The test ensures that operation completes without hanging.
    Previously, empty bundles would get stuck in _pending_bundles.
    """
    # Create empty dataset via filter, then repartition
    ds = (
        ray.data.range(10)
        .filter(lambda x: x["id"] > 100)
        .repartition(target_num_rows_per_block=8)
    )

    # Verify dataset is empty
    assert ds.count() == 0, "Expected empty dataset"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))


@pytest.mark.parametrize(
    "op_builder,expected_fused_name,expected_count",
    [
        # filter -> streaming_repartition (non-strict): fused, rows halved.
        (
            lambda ds: ds.filter(lambda r: r["id"] % 2 == 0),
            "Filter(<lambda>)->StreamingRepartition",
            500,
        ),
        # map_rows -> streaming_repartition (non-strict): fused, rows unchanged.
        (
            lambda ds: ds.map(lambda r: {"id": r["id"] + 1, "value": r["id"]}),
            "Map(<lambda>)->StreamingRepartition",
            1000,
        ),
        # flat_map -> streaming_repartition (non-strict): fused, rows doubled
        # (each input row expands to two rows with distinct ids).
        (
            lambda ds: ds.flat_map(
                lambda r: [
                    {"id": r["id"], "value": r["id"] + 1},
                    {"id": r["id"] + 1000, "value": r["id"]},
                ]
            ),
            "FlatMap(<lambda>)->StreamingRepartition",
            2000,
        ),
        # expression-based filter -> streaming_repartition (non-strict): fused.
        (
            lambda ds: ds.filter(expr="id >= 50"),
            "Filter(col('id') >= 50)->StreamingRepartition",
            950,
        ),
    ],
    ids=["filter", "map_rows", "flat_map", "expression_filter"],
)
def test_streaming_repartition_non_mapbatches_fusion_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    op_builder,
    expected_fused_name,
    expected_count,
):
    """E2E test that Filter/MapRows/FlatMap/expression-Filter fuse with
    StreamingRepartition in non-strict mode, and that data is preserved."""
    num_rows = 1000
    target = 20
    ds = ray.data.range(num_rows, override_num_blocks=10).map(
        lambda r: {"id": r["id"], "value": r["id"]}
    )
    ds = op_builder(ds).repartition(target_num_rows_per_block=target)

    # Verify fusion in the optimized physical plan.
    planner = create_planner()
    physical_plan, _ = planner.plan(ds._logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    assert (
        expected_fused_name in physical_op.name
    ), f"Expected '{expected_fused_name}' in operator name, got: {physical_op.name}"

    # Verify data correctness: no rows lost or duplicated.
    assert (
        ds.count() == expected_count
    ), f"Expected {expected_count} rows, got {ds.count()}"
    ids = sorted(row["id"] for row in ds.take_all())
    assert (
        len(ids) == expected_count
    ), f"Expected {expected_count} rows from take_all, got {len(ids)}"
    # No rows duplicated or lost across the fused boundary.
    assert len(set(ids)) == expected_count, f"Duplicate rows after fusion: {ids}"


def test_streaming_repartition_filter_all_rows_removed_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """E2E test that a filter removing all rows still fuses with
    StreamingRepartition and yields an empty dataset."""
    ds = (
        ray.data.range(100, override_num_blocks=4)
        .map(lambda r: {"id": r["id"], "value": r["id"]})
        .filter(lambda r: False)
        .repartition(target_num_rows_per_block=20)
    )

    assert ds.count() == 0
    assert list(ds.iter_rows()) == []


def test_streaming_repartition_filter_then_map_batches_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """E2E test of the original issue pipeline: filter -> repartition ->
    map_batches. Filter must fuse into StreamingRepartition, while the
    downstream map_batches stays separate by design (to preserve parallelism)."""
    num_rows = 1000
    target = 20
    ds = (
        ray.data.range(num_rows, override_num_blocks=10)
        .map(lambda r: {"id": r["id"], "value": r["id"]})
        .filter(lambda r: r["id"] % 2 == 0)
        .repartition(target_num_rows_per_block=target)
        .map_batches(lambda batch: batch, batch_size=16)
    )

    # Verify filter fused into streaming repartition. The downstream
    # map_batches stays a separate operator on top of the DAG (by design), so
    # search the DAG for the fused operator instead of only checking the root.
    planner = create_planner()
    physical_plan, _ = planner.plan(ds._logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)

    def find_fused(op):
        if "Filter(<lambda>)->StreamingRepartition" in op.name:
            return op.name
        for inp in op.input_dependencies:
            found = find_fused(inp)
            if found:
                return found
        return None

    fused_name = find_fused(physical_plan.dag)
    assert fused_name is not None, (
        f"Expected Filter fused into StreamingRepartition, DAG root: "
        f"{physical_plan.dag.name}"
    )
    # The downstream map_batches must NOT fuse into the repartition (this would
    # reduce parallelism), so the fused operator name must not contain it.
    assert "->MapBatches" not in fused_name, (
        f"map_batches should stay separate after StreamingRepartition: " f"{fused_name}"
    )

    # Data correctness through the full pipeline.
    assert ds.count() == num_rows // 2
    ids = sorted(row["id"] for row in ds.take_all())
    assert ids == list(range(0, num_rows, 2))


def test_streaming_repartition_filter_partial_empty_blocks_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """E2E test that a filter emptying only some input blocks (while others
    stay non-empty) still fuses with StreamingRepartition without hanging or
    dropping rows. Regression-guards the mixed empty/non-empty bundle path
    (see test_streaming_repartition_empty_dataset for the all-empty case)."""
    ds = (
        ray.data.range(100, override_num_blocks=4)
        .filter(lambda r: r["id"] >= 75)  # blocks 0-2 fully filtered out
        .repartition(target_num_rows_per_block=20)
    )

    assert ds.count() == 25
    assert sorted(r["id"] for r in ds.take_all()) == list(range(75, 100))

    stats = ds.stats()
    assert "Filter(<lambda>)->StreamingRepartition" in stats, stats


def test_streaming_repartition_fused_block_shapes_non_strict_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """E2E test that fusion preserves the non-strict mode block-shape
    invariant: each input block is split independently with no cross-block
    stitching, so no output block combines rows from different input blocks.

    This mirrors test_streaming_repartition_non_strict_mode, but for the
    fused (Filter -> StreamingRepartition) path."""
    # Case 1: each input block (25 rows) keeps 10 rows; target 20 -> the 10-row
    # blocks must NOT be stitched together into 20-row blocks.
    ds = (
        ray.data.range(100, override_num_blocks=4)
        .filter(lambda r: r["id"] % 25 < 10)
        .repartition(target_num_rows_per_block=20)
        .materialize()
    )
    counts = sorted(
        m.num_rows for b in ds.iter_internal_ref_bundles() for m in b.metadata
    )
    assert counts == [10, 10, 10, 10], f"cross-block stitching detected: {counts}"

    # Case 2: each input block keeps all 25 rows; target 20 -> each input block
    # yields [20, 5], i.e. at most one partial trailing block per input block.
    ds = (
        ray.data.range(100, override_num_blocks=4)
        .filter(lambda r: r["id"] % 25 < 30)
        .repartition(target_num_rows_per_block=20)
        .materialize()
    )
    counts = sorted(
        m.num_rows for b in ds.iter_internal_ref_bundles() for m in b.metadata
    )
    assert counts == [
        5,
        5,
        5,
        5,
        20,
        20,
        20,
        20,
    ], f"unexpected block shape after fusion: {counts}"

    stats = ds.stats()
    assert "Filter(<lambda>)->StreamingRepartition" in stats, stats


@pytest.mark.parametrize(
    "target_num_rows_per_block,expected_block_counts",
    [
        # target=1: every surviving row becomes its own block.
        (1, [1, 1, 1, 1, 1]),
        # target >> total rows: blocks pass through unsplit (filter halves rows:
        # 4 input blocks of 25 -> 4 blocks of 12/13).
        (1000, [12, 12, 13, 13]),
    ],
    ids=["target_one", "target_large"],
)
def test_streaming_repartition_fused_target_boundaries_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
    target_num_rows_per_block,
    expected_block_counts,
):
    """E2E test of target_num_rows_per_block boundary values on the fused
    Filter -> StreamingRepartition path."""
    ds = (
        ray.data.range(
            10 if target_num_rows_per_block == 1 else 100, override_num_blocks=4
        )
        .filter(lambda r: r["id"] % 2 == 0)
        .repartition(target_num_rows_per_block=target_num_rows_per_block)
        .materialize()
    )
    counts = sorted(
        m.num_rows for b in ds.iter_internal_ref_bundles() for m in b.metadata
    )
    assert counts == expected_block_counts, f"unexpected block counts: {counts}"


def test_streaming_repartition_fused_single_input_block_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """E2E test with a single input block (override_num_blocks=1): the fused
    operator runs one task and must still split correctly."""
    ds = (
        ray.data.range(100, override_num_blocks=1)
        .filter(lambda r: r["id"] % 2 == 0)
        .repartition(target_num_rows_per_block=20)
        .materialize()
    )
    counts = sorted(
        m.num_rows for b in ds.iter_internal_ref_bundles() for m in b.metadata
    )
    assert counts == [10, 20, 20], f"unexpected block counts: {counts}"
    assert ds.count() == 50


def test_streaming_repartition_fused_empty_input_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """E2E test of a zero-block input dataset (range(0)) fused with
    StreamingRepartition: must not hang and must yield an empty dataset."""
    ds = (
        ray.data.range(0)
        .filter(lambda r: True)
        .repartition(target_num_rows_per_block=20)
    )
    assert ds.count() == 0
    assert list(ds.iter_rows()) == []


def test_streaming_repartition_expr_filter_after_pushdown_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """E2E test of the interaction between predicate pushdown and fusion: an
    expression filter written AFTER repartition gets pushed before the
    StreamingRepartition (PASSTHROUGH), after which the fusion rule fuses it
    into a single operator. Data must be preserved."""
    ds = (
        ray.data.range(100, override_num_blocks=4)
        .repartition(target_num_rows_per_block=20)
        .filter(expr="id >= 50")
    )

    assert ds.count() == 50
    ids = sorted(r["id"] for r in ds.take_all())
    assert ids == list(range(50, 100))

    stats = ds.stats()
    assert "Filter(col('id') >= 50)->StreamingRepartition" in stats, stats


def test_streaming_repartition_remote_args_fn_not_fused_e2e(
    ray_start_regular_shared_2_cpus,
    disable_fallback_to_object_extension,
):
    """E2E test that a filter with ray_remote_args_fn is NOT fused with
    StreamingRepartition (are_op_remote_args_compatible rejects it), and the
    pipeline still works."""
    ds = (
        ray.data.range(100, override_num_blocks=4)
        .filter(lambda r: r["id"] % 2 == 0, ray_remote_args_fn=lambda: {})
        .repartition(target_num_rows_per_block=20)
    )

    assert ds.count() == 50

    stats = ds.stats()
    assert "Filter(<lambda>)->StreamingRepartition" not in stats, stats
