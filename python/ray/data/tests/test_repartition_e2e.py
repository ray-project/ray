import numpy as np
import pytest

import ray
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
    context.hash_shuffle_operator_actor_num_cpus_per_partition_override = 0.001

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
