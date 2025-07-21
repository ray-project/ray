from typing import Optional
from unittest.mock import MagicMock

import pandas as pd
import pytest

import ray
from ray.data import DataContext, Dataset
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.util import GiB, MiB
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


@pytest.fixture
def nullify_shuffle_aggregator_num_cpus():
    ctx = ray.data.context.DataContext.get_current()

    original = ctx.join_operator_actor_num_cpus_per_partition_override
    # NOTE: We override this to reduce hardware requirements
    #       for every aggregator
    ctx.join_operator_actor_num_cpus_per_partition_override = 0.001

    yield

    ctx.join_operator_actor_num_cpus_per_partition_override = original


@pytest.mark.parametrize(
    "num_rows_left,num_rows_right,partition_size_hint",
    [
        (32, 32, 1 * MiB),
        (32, 16, None),
        (16, 32, None),
        # "Degenerate" cases with mostly empty partitions
        (32, 1, None),
        (1, 32, None),
    ],
)
def test_simple_inner_join(
    ray_start_regular_shared_2_cpus,
    nullify_shuffle_aggregator_num_cpus,
    num_rows_left: int,
    num_rows_right: int,
    partition_size_hint: Optional[int],
):
    # NOTE: We override max-block size to make sure that in cases when a partition
    #       size hint is not provided, we're not over-estimating amount of memory
    #       required for the aggregators
    DataContext.get_current().target_max_block_size = 1 * MiB

    doubles = ray.data.range(num_rows_left).map(
        lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
    )

    squares = ray.data.range(num_rows_right).map(
        lambda row: {"id": row["id"], "square": int(row["id"]) ** 2}
    )

    doubles_pd = doubles.to_pandas()
    squares_pd = squares.to_pandas()

    # Join using Pandas (to assert against)
    expected_pd = doubles_pd.join(squares_pd.set_index("id"), on="id", how="inner")
    expected_pd_sorted = expected_pd.sort_values(by=["id"]).reset_index(drop=True)

    # Join using Ray Data
    joined: Dataset = doubles.join(
        squares,
        join_type="inner",
        num_partitions=16,
        on=("id",),
        partition_size_hint=partition_size_hint,
    )

    # TODO use native to_pandas() instead
    joined_pd = pd.DataFrame(joined.take_all())

    # Sort resulting frame and reset index (to be able to compare with expected one)
    joined_pd_sorted = joined_pd.sort_values(by=["id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_pd_sorted, joined_pd_sorted)


@pytest.mark.parametrize(
    "join_type",
    [
        "left_outer",
        "right_outer",
    ],
)
@pytest.mark.parametrize(
    "num_rows_left,num_rows_right",
    [
        (32, 32),
        (32, 16),
        (16, 32),
        # "Degenerate" cases with mostly empty partitions
        (1, 32),
        (32, 1),
    ],
)
def test_simple_left_right_outer_join(
    ray_start_regular_shared_2_cpus,
    nullify_shuffle_aggregator_num_cpus,
    join_type,
    num_rows_left,
    num_rows_right,
):
    # NOTE: We override max-block size to make sure that in cases when a partition
    #       size hint is not provided, we're not over-estimating amount of memory
    #       required for the aggregators
    DataContext.get_current().target_max_block_size = 1 * MiB

    doubles = ray.data.range(num_rows_left).map(
        lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
    )

    squares = ray.data.range(num_rows_right).map(
        lambda row: {"id": row["id"], "square": int(row["id"]) ** 2}
    )

    doubles_pd = doubles.to_pandas()
    squares_pd = squares.to_pandas()

    # Join using Pandas (to assert against)
    if join_type == "left_outer":
        pd_join_type = "left"
        squares_pd = squares_pd.set_index("id")
    elif join_type == "right_outer":
        pd_join_type = "right"
        doubles_pd = doubles_pd.set_index("id")
    else:
        raise ValueError(f"Unsupported join type: {join_type}")

    expected_pd = doubles_pd.join(squares_pd, on="id", how=pd_join_type).reset_index(
        drop=True
    )

    # Join using Ray Data
    joined: Dataset = doubles.join(
        squares,
        join_type=join_type,
        num_partitions=16,
        on=("id",),
    )

    joined_pd = pd.DataFrame(joined.take_all())

    # Sort resulting frame and reset index (to be able to compare with expected one)
    joined_pd_sorted = joined_pd.sort_values(by=["id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_pd, joined_pd_sorted)


@pytest.mark.parametrize(
    "num_rows_left,num_rows_right",
    [
        (32, 32),
        (32, 16),
        (16, 32),
        # # "Degenerate" cases with mostly empty partitions
        (1, 32),
        (32, 1),
    ],
)
def test_simple_full_outer_join(
    ray_start_regular_shared_2_cpus,
    nullify_shuffle_aggregator_num_cpus,
    num_rows_left,
    num_rows_right,
):
    # NOTE: We override max-block size to make sure that in cases when a partition
    #       size hint is not provided, we're not over-estimating amount of memory
    #       required for the aggregators
    DataContext.get_current().target_max_block_size = 1 * MiB

    doubles = ray.data.range(num_rows_left).map(
        lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
    )

    squares = ray.data.range(num_rows_right).map(
        lambda row: {"id": row["id"] + num_rows_left, "square": int(row["id"]) ** 2}
    )

    doubles_pd = doubles.to_pandas()
    squares_pd = squares.to_pandas()

    # Join using Pandas (to assert against)
    expected_pd = doubles_pd.join(
        squares_pd.set_index("id"), on="id", how="outer"
    ).reset_index(drop=True)

    # Join using Ray Data
    joined: Dataset = doubles.join(
        squares,
        join_type="full_outer",
        num_partitions=16,
        on=("id",),
        # NOTE: We override this to reduce hardware requirements
        #       for every aggregator (by default requiring 1 logical CPU)
        aggregator_ray_remote_args={"num_cpus": 0.01},
    )

    joined_pd = pd.DataFrame(joined.take_all())

    # Sort resulting frame and reset index (to be able to compare with expected one)
    joined_pd_sorted = joined_pd.sort_values(by=["id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_pd, joined_pd_sorted)


@pytest.mark.parametrize("left_suffix", [None, "_left"])
@pytest.mark.parametrize("right_suffix", [None, "_right"])
def test_simple_self_join(ray_start_regular_shared_2_cpus, left_suffix, right_suffix):
    # NOTE: We override max-block size to make sure that in cases when a partition
    #       size hint is not provided, we're not over-estimating amount of memory
    #       required for the aggregators
    DataContext.get_current().target_max_block_size = 1 * MiB

    doubles = ray.data.range(100).map(
        lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
    )

    doubles_pd = doubles.to_pandas()

    # Self-join
    joined: Dataset = doubles.join(
        doubles,
        join_type="inner",
        num_partitions=16,
        on=("id",),
        left_suffix=left_suffix,
        right_suffix=right_suffix,
        # NOTE: We override this to reduce hardware requirements
        #       for every aggregator (by default requiring 1 logical CPU)
        aggregator_ray_remote_args={"num_cpus": 0.01},
    )

    if left_suffix is None and right_suffix is None:
        with pytest.raises(RayTaskError) as exc_info:
            joined.count()

        assert 'Field "double" exists 2 times' in str(exc_info.value.cause)
    else:

        joined_pd = pd.DataFrame(joined.take_all())

        # Sort resulting frame and reset index (to be able to compare with expected one)
        joined_pd_sorted = joined_pd.sort_values(by=["id"]).reset_index(drop=True)

        # Join using Pandas (to assert against)
        expected_pd = doubles_pd.join(
            doubles_pd.set_index("id"),
            on="id",
            how="inner",
            lsuffix=left_suffix,
            rsuffix=right_suffix,
        ).reset_index(drop=True)

        pd.testing.assert_frame_equal(expected_pd, joined_pd_sorted)


def test_invalid_join_config(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(32)

    with pytest.raises(ValueError) as exc_info:
        ds.join(
            ds,
            "inner",
            num_partitions=16,
            on="id",  # has to be tuple/list
            validate_schemas=True,
        )

    assert str(exc_info.value) == "Expected tuple or list as `on` (got str)"

    with pytest.raises(ValueError) as exc_info:
        ds.join(
            ds,
            "inner",
            num_partitions=16,
            on=("id",),
            right_on="id",  # has to be tuple/list
            validate_schemas=True,
        )

    assert str(exc_info.value) == "Expected tuple or list as `right_on` (got str)"


@pytest.mark.parametrize("join_type", [jt for jt in JoinType])  # noqa: C416
def test_invalid_join_not_matching_key_columns(
    ray_start_regular_shared_2_cpus, join_type
):
    # Case 1: Check on missing key column
    empty_ds = ray.data.range(0)

    non_empty_ds = ray.data.range(32)

    with pytest.raises(ValueError) as exc_info:
        empty_ds.join(
            non_empty_ds,
            join_type,
            num_partitions=16,
            on=("id",),
            validate_schemas=True,
        )

    assert (
        str(exc_info.value)
        == "Key columns are expected to be present and have the same types in both "
        "left and right operands of the join operation: left has None, but right "
        "has Column  Type\n------  ----\nid      int64"
    )

    # Case 2: Check mismatching key column
    id_int_type_ds = ray.data.range(32).map(lambda row: {"id": int(row["id"])})

    id_float_type_ds = ray.data.range(32).map(lambda row: {"id": float(row["id"])})

    with pytest.raises(ValueError) as exc_info:
        id_int_type_ds.join(
            id_float_type_ds,
            join_type,
            num_partitions=16,
            on=("id",),
            validate_schemas=True,
        )

    assert (
        str(exc_info.value)
        == "Key columns are expected to be present and have the same types in both "
        "left and right operands of the join operation: left has "
        "Column  Type\n------  ----\nid      int64, but right has "
        "Column  Type\n------  ----\nid      double"
    )


def test_default_shuffle_aggregator_args():
    parent_op_mock = MagicMock(PhysicalOperator)
    parent_op_mock._output_dependencies = []

    op = JoinOperator(
        left_input_op=parent_op_mock,
        right_input_op=parent_op_mock,
        data_context=DataContext.get_current(),
        left_key_columns=("id",),
        right_key_columns=("id",),
        join_type=JoinType.INNER,
        num_partitions=16,
    )

    # - 1 partition per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=16,
        num_aggregators=16,
        partition_size_hint=None,
    )

    assert {
        "num_cpus": 0.125,
        "memory": 939524096,
        "scheduling_strategy": "SPREAD",
    } == args

    # - 4 partitions per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=64,
        num_aggregators=16,
        partition_size_hint=None,
    )

    assert {
        "num_cpus": 0.5,
        "memory": 1744830464,
        "scheduling_strategy": "SPREAD",
    } == args

    # - 4 partitions per aggregator
    # - No partition size hint
    args = op._get_default_aggregator_ray_remote_args(
        num_partitions=64,
        num_aggregators=16,
        partition_size_hint=1 * GiB,
    )

    assert {
        "num_cpus": 0.5,
        "memory": 13958643712,
        "scheduling_strategy": "SPREAD",
    } == args


# Broadcast Join Tests


@pytest.mark.parametrize(
    "join_type",
    ["inner", "left_outer", "right_outer", "full_outer"],
)
@pytest.mark.parametrize(
    "num_rows_left,num_rows_right",
    [
        (32, 16),  # Typical case - smaller right dataset
        (100, 10),  # Larger difference
        (10, 5),  # Small datasets
    ],
)
def test_broadcast_join_basic(
    ray_start_regular_shared_2_cpus,
    join_type,
    num_rows_left,
    num_rows_right,
):
    """Test basic broadcast join functionality for all join types."""

    # Create test datasets
    left_ds = ray.data.range(num_rows_left).map(
        lambda row: {"id": row["id"], "left_value": int(row["id"]) * 2}
    )

    right_ds = ray.data.range(num_rows_right).map(
        lambda row: {"id": row["id"], "right_value": int(row["id"]) ** 2}
    )

    # Perform broadcast join
    broadcast_result = left_ds.join(
        right_ds,
        join_type=join_type,
        num_partitions=4,
        on=("id",),
        broadcast=True,
    )

    # Perform regular join for comparison
    regular_result = left_ds.join(
        right_ds,
        join_type=join_type,
        num_partitions=4,
        on=("id",),
        broadcast=False,
    )

    # Convert to pandas for comparison
    broadcast_df = (
        pd.DataFrame(broadcast_result.take_all())
        .sort_values(by=["id"])
        .reset_index(drop=True)
    )
    regular_df = (
        pd.DataFrame(regular_result.take_all())
        .sort_values(by=["id"])
        .reset_index(drop=True)
    )

    # Results should be identical
    pd.testing.assert_frame_equal(broadcast_df, regular_df)


def test_broadcast_join_with_suffixes(ray_start_regular_shared_2_cpus):
    """Test broadcast join with column name suffixes."""

    # Create datasets with overlapping column names
    left_ds = ray.data.from_items(
        [
            {"id": 1, "value": "left_1"},
            {"id": 2, "value": "left_2"},
            {"id": 3, "value": "left_3"},
        ]
    )

    right_ds = ray.data.from_items(
        [
            {"id": 1, "value": "right_1"},
            {"id": 2, "value": "right_2"},
        ]
    )

    # Test broadcast join with suffixes
    result = left_ds.join(
        right_ds,
        join_type="inner",
        num_partitions=2,
        on=("id",),
        left_suffix="_left",
        right_suffix="_right",
        broadcast=True,
    )

    result_df = (
        pd.DataFrame(result.take_all()).sort_values(by=["id"]).reset_index(drop=True)
    )

    # Verify suffixes are applied correctly
    expected_columns = {"id", "value_left", "value_right"}
    assert set(result_df.columns) == expected_columns

    # Verify join results
    assert len(result_df) == 2  # Only id=1 and id=2 should match
    assert result_df.loc[0, "value_left"] == "left_1"
    assert result_df.loc[0, "value_right"] == "right_1"


def test_broadcast_join_different_key_names(ray_start_regular_shared_2_cpus):
    """Test broadcast join with different key column names."""

    left_ds = ray.data.from_items(
        [
            {"left_id": 1, "left_data": "a"},
            {"left_id": 2, "left_data": "b"},
            {"left_id": 3, "left_data": "c"},
        ]
    )

    right_ds = ray.data.from_items(
        [
            {"right_id": 1, "right_data": "x"},
            {"right_id": 2, "right_data": "y"},
        ]
    )

    # Test broadcast join with different key names
    result = left_ds.join(
        right_ds,
        join_type="inner",
        num_partitions=2,
        on=("left_id",),
        right_on=("right_id",),
        broadcast=True,
    )

    result_df = (
        pd.DataFrame(result.take_all())
        .sort_values(by=["left_id"])
        .reset_index(drop=True)
    )

    # Verify results
    assert len(result_df) == 2
    expected_columns = {"left_id", "left_data", "right_id", "right_data"}
    assert set(result_df.columns) == expected_columns


def test_broadcast_join_empty_right_dataset(ray_start_regular_shared_2_cpus):
    """Test broadcast join with empty right dataset."""

    left_ds = ray.data.from_items(
        [
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
        ]
    )

    right_ds = ray.data.from_items([])  # Empty dataset

    # Inner join should return empty
    inner_result = left_ds.join(
        right_ds,
        join_type="inner",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    assert inner_result.count() == 0

    # Left outer join should return all left rows
    left_outer_result = left_ds.join(
        right_ds,
        join_type="left_outer",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    assert left_outer_result.count() == 2


def test_broadcast_join_performance_with_small_right(ray_start_regular_shared_2_cpus):
    """Test that broadcast join is used appropriately for small right datasets."""

    # Large left dataset
    left_ds = ray.data.range(1000).map(
        lambda row: {"id": row["id"], "left_value": row["id"] * 2}
    )

    # Small right dataset (perfect for broadcasting)
    right_ds = ray.data.range(10).map(
        lambda row: {"id": row["id"], "right_value": row["id"] ** 2}
    )

    # Test broadcast join
    result = left_ds.join(
        right_ds,
        join_type="inner",
        num_partitions=8,
        on=("id",),
        broadcast=True,
    )

    # Should only return 10 rows (matching the small right dataset)
    assert result.count() == 10

    # Verify correctness by comparing a few values
    result_df = (
        pd.DataFrame(result.take_all()).sort_values(by=["id"]).reset_index(drop=True)
    )

    for i in range(min(5, len(result_df))):
        assert result_df.loc[i, "left_value"] == result_df.loc[i, "id"] * 2
        assert result_df.loc[i, "right_value"] == result_df.loc[i, "id"] ** 2


def test_broadcast_join_concurrency_equals_num_partitions(
    ray_start_regular_shared_2_cpus,
):
    """Test that broadcast join actor concurrency matches num_partitions."""

    left_ds = ray.data.range(100).map(lambda row: {"id": row["id"], "value": row["id"]})

    right_ds = ray.data.range(50).map(
        lambda row: {"id": row["id"], "value": row["id"] * 2}
    )

    # Test with different num_partitions values
    for num_partitions in [2, 4, 8]:
        result = left_ds.join(
            right_ds,
            join_type="inner",
            num_partitions=num_partitions,
            on=("id",),
            broadcast=True,
        )

        # Should complete successfully and produce correct results
        assert result.count() == 50  # All right dataset rows should match


def test_broadcast_join_vs_regular_join_equivalence(ray_start_regular_shared_2_cpus):
    """Test that broadcast join produces identical results to regular join."""

    # Test data with various patterns
    left_ds = ray.data.from_items(
        [
            {"id": 1, "category": "A", "value": 10},
            {"id": 2, "category": "B", "value": 20},
            {"id": 3, "category": "A", "value": 30},
            {"id": 4, "category": "C", "value": 40},
            {"id": 5, "category": "B", "value": 50},
        ]
    )

    right_ds = ray.data.from_items(
        [
            {"id": 2, "score": 85},
            {"id": 3, "score": 92},
            {"id": 6, "score": 78},  # This won't match any left row
        ]
    )

    for join_type in ["inner", "left_outer", "right_outer", "full_outer"]:
        # Broadcast join
        broadcast_result = left_ds.join(
            right_ds,
            join_type=join_type,
            num_partitions=3,
            on=("id",),
            broadcast=True,
        )

        # Regular join
        regular_result = left_ds.join(
            right_ds,
            join_type=join_type,
            num_partitions=3,
            on=("id",),
            broadcast=False,
        )

        # Compare results
        broadcast_df = (
            pd.DataFrame(broadcast_result.take_all())
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )
        regular_df = (
            pd.DataFrame(regular_result.take_all())
            .sort_values(by=["id"])
            .reset_index(drop=True)
        )

        pd.testing.assert_frame_equal(
            broadcast_df, regular_df, check_dtype=False
        )  # Allow minor dtype differences


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
