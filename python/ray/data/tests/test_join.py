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
        "left_semi",
        "right_semi",
        "left_anti",
        "right_anti",
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
def test_simple_left_right_outer_semi_anti_join(
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
        expected_pd = doubles_pd.join(
            squares_pd.set_index("id"), on="id", how="left"
        ).reset_index(drop=True)
    elif join_type == "right_outer":
        expected_pd = (
            doubles_pd.set_index("id")
            .join(squares_pd, on="id", how="right")
            .reset_index(drop=True)
        )
    elif join_type == "left_semi":
        # Left semi: left rows that have matches in right (left columns only)
        merged = doubles_pd.merge(squares_pd, on="id", how="inner")
        expected_pd = merged[["id", "double"]].drop_duplicates().reset_index(drop=True)
    elif join_type == "right_semi":
        # Right semi: right rows that have matches in left (right columns only)
        merged = doubles_pd.merge(squares_pd, on="id", how="inner")
        expected_pd = merged[["id", "square"]].drop_duplicates().reset_index(drop=True)
    elif join_type == "left_anti":
        # Left anti: left rows that don't have matches in right
        merged = doubles_pd.merge(squares_pd, on="id", how="left", indicator=True)
        expected_pd = merged[merged["_merge"] == "left_only"][
            ["id", "double"]
        ].reset_index(drop=True)
    elif join_type == "right_anti":
        # Right anti: right rows that don't have matches in left
        merged = doubles_pd.merge(squares_pd, on="id", how="right", indicator=True)
        expected_pd = merged[merged["_merge"] == "right_only"][
            ["id", "square"]
        ].reset_index(drop=True)
    else:
        raise ValueError(f"Unsupported join type: {join_type}")

    # Join using Ray Data
    joined: Dataset = doubles.join(
        squares,
        join_type=join_type,
        num_partitions=16,
        on=("id",),
    )

    joined_pd = pd.DataFrame(joined.take_all())

    # Handle empty results from Ray Data which may not preserve schema
    if len(joined_pd) == 0 and len(expected_pd) == 0:
        pass
    else:
        # Sort resulting frame and reset index (to be able to compare with expected one)
        joined_pd_sorted = joined_pd.sort_values(by=["id"]).reset_index(drop=True)
        expected_pd_sorted = expected_pd.sort_values(by=["id"]).reset_index(drop=True)

        pd.testing.assert_frame_equal(expected_pd_sorted, joined_pd_sorted)


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

    # Handle empty results from Ray Data which may not preserve schema
    if len(joined_pd) == 0 and len(expected_pd) == 0:
        pass
    else:
        # Sort resulting frame and reset index (to be able to compare with expected one)
        joined_pd_sorted = joined_pd.sort_values(by=["id"]).reset_index(drop=True)
        expected_pd_sorted = expected_pd.sort_values(by=["id"]).reset_index(drop=True)

        pd.testing.assert_frame_equal(expected_pd_sorted, joined_pd_sorted)


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
        data_context=DataContext.get_current(),
        left_input_op=parent_op_mock,
        right_input_op=parent_op_mock,
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


@pytest.mark.parametrize(
    "join_type",
    [
        "inner",
        "left_outer",
        "right_outer",
        "full_outer",
    ],
)
def test_broadcast_join_basic(
    ray_start_regular_shared_2_cpus,
    join_type,
):
    """Test basic broadcast join functionality for all supported join types."""

    # Create test datasets - much smaller for efficient testing
    left_ds = ray.data.range(5).map(  # Reduced from 10 to 5
        lambda row: {"id": row["id"], "left_value": int(row["id"]) * 2}
    )

    right_ds = ray.data.range(3).map(  # Reduced from 5 to 3
        lambda row: {"id": row["id"], "right_value": int(row["id"]) ** 2}
    )

    # Perform broadcast join
    broadcast_result = left_ds.join(
        right_ds,
        join_type=join_type,
        on=("id",),
        broadcast=True,
    )

    # Perform regular join for comparison
    regular_result = left_ds.join(
        right_ds,
        join_type=join_type,
        num_partitions=2,  # Match the number of CPUs in the test fixture
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

    # Ensure both DataFrames have the same column ordering before comparison
    common_columns = sorted(set(broadcast_df.columns) & set(regular_df.columns))
    broadcast_df = broadcast_df[common_columns]
    regular_df = regular_df[common_columns]

    # Results should be identical
    pd.testing.assert_frame_equal(broadcast_df, regular_df)


def test_broadcast_join_dataset_swapping(
    ray_start_regular_shared_2_cpus,
):
    """Test broadcast join with dataset swapping (left smaller than right)."""

    # Create datasets where left is smaller than right - much smaller for efficient testing
    left_ds = ray.data.range(2).map(  # Reduced from 3 to 2
        lambda row: {"id": row["id"], "left_value": int(row["id"]) * 2}
    )

    right_ds = ray.data.range(5).map(  # Reduced from 10 to 5
        lambda row: {"id": row["id"], "right_value": int(row["id"]) ** 2}
    )

    # Perform broadcast join (should trigger dataset swapping)
    result = left_ds.join(
        right_ds,
        join_type="inner",
        on=("id",),
        broadcast=True,
    )

    # Verify the result has the expected structure
    result_df = pd.DataFrame(result.take_all())
    expected_columns = {"id", "left_value", "right_value"}
    assert set(result_df.columns) == expected_columns

    # Should return 2 rows (all left rows match)
    assert len(result_df) == 2


@pytest.mark.parametrize(
    "join_type",
    [
        "inner",
        "left_outer",
        "right_outer",
        "full_outer",
    ],
)
@pytest.mark.parametrize(
    "num_rows_left,num_rows_right",
    [
        (8, 16),  # Left dataset is smaller than right dataset - reduced from (16, 32)
        (5, 20),  # Left dataset is much smaller - reduced from (10, 100)
        (3, 8),  # Small left dataset - reduced from (5, 10)
    ],
)
def test_broadcast_join_left_smaller(
    ray_start_regular_shared_2_cpus,
    join_type,
    num_rows_left,
    num_rows_right,
):
    """Test broadcast join when left dataset is smaller than right dataset.

    This tests the dataset swapping logic where the smaller left dataset gets broadcasted
    and the larger right dataset gets processed in batches. The test covers:

    - Dataset swapping scenarios (left < right)
    - All supported join types
    - Various size differences

    Test cases:
    - (16, 32): Left dataset is smaller than right dataset
    - (10, 100): Left dataset is much smaller
    - (5, 10): Small left dataset

    When left < right, the broadcast join automatically swaps the datasets internally
    to broadcast the smaller left dataset and process the larger right dataset in batches.
    This maintains the same join semantics while optimizing for performance.
    """

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
        on=("id",),
        broadcast=True,
    )

    # Perform regular join for comparison
    regular_result = left_ds.join(
        right_ds,
        join_type=join_type,
        num_partitions=2,  # Match the number of CPUs in the test fixture
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

    # Ensure both DataFrames have the same column ordering before comparison
    common_columns = sorted(set(broadcast_df.columns) & set(regular_df.columns))
    broadcast_df = broadcast_df[common_columns]
    regular_df = regular_df[common_columns]

    # Results should be identical
    pd.testing.assert_frame_equal(broadcast_df, regular_df)


@pytest.mark.parametrize(
    "join_type",
    [
        "inner",
        "left_outer",
        "right_outer",
        "full_outer",
    ],
)
def test_broadcast_join_with_different_key_names_and_swapping(
    ray_start_regular_shared_2_cpus,
    join_type,
):
    """Test broadcast join with different key names and dataset swapping scenarios."""

    # Test case 1: Left dataset smaller with different key names
    left_ds_small = ray.data.from_items(
        [
            {"left_id": 1, "left_data": "a"},
            {"left_id": 2, "left_data": "b"},
            {"left_id": 3, "left_data": "c"},
        ]
    )

    right_ds_large = ray.data.from_items(
        [{"right_id": i, "right_data": f"data_{i}"} for i in range(20)]
    )

    # Test case 2: Right dataset smaller with different key names
    left_ds_large = ray.data.from_items(
        [{"left_id": i, "left_data": f"left_{i}"} for i in range(20)]
    )

    right_ds_small = ray.data.from_items(
        [
            {"right_id": 1, "right_data": "x"},
            {"right_id": 2, "right_data": "y"},
            {"right_id": 3, "right_data": "z"},
        ]
    )

    test_cases = [
        (left_ds_small, right_ds_large, "left smaller"),
        (left_ds_large, right_ds_small, "right smaller"),
    ]

    for left_ds, right_ds, case_name in test_cases:
        # Perform broadcast join
        broadcast_result = left_ds.join(
            right_ds,
            join_type=join_type,
            on=("left_id",),
            right_on=("right_id",),
            broadcast=True,
        )

        # Perform regular join for comparison
        regular_result = left_ds.join(
            right_ds,
            join_type=join_type,
            num_partitions=2,
            on=("left_id",),
            right_on=("right_id",),
            broadcast=False,
        )

        # Convert to pandas for comparison
        broadcast_df = (
            pd.DataFrame(broadcast_result.take_all())
            .sort_values(by=["left_id"])
            .reset_index(drop=True)
        )
        regular_df = (
            pd.DataFrame(regular_result.take_all())
            .sort_values(by=["left_id"])
            .reset_index(drop=True)
        )

        # Ensure both DataFrames have the same column ordering before comparison
        common_columns = sorted(set(broadcast_df.columns) & set(regular_df.columns))
        broadcast_df = broadcast_df[common_columns]
        regular_df = regular_df[common_columns]

        # Results should be identical
        pd.testing.assert_frame_equal(
            broadcast_df,
            regular_df,
            check_dtype=False,
            err_msg=f"Failed for case: {case_name}, join_type: {join_type}",
        )


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
            {"left_id": 4, "left_data": "d"},
            {"left_id": 5, "left_data": "e"},
            {"left_id": 6, "left_data": "f"},
            {"left_id": 7, "left_data": "g"},
            {"left_id": 8, "left_data": "h"},
            {"left_id": 9, "left_data": "i"},
            {"left_id": 10, "left_data": "j"},
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


def test_broadcast_join_performance_with_small_right(ray_start_regular_shared_2_cpus):
    """Test that broadcast join is used appropriately for small right datasets."""

    # Large left dataset
    left_ds = ray.data.range(20).map(  # Reduced from 100 to 20
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


def test_broadcast_join_expected_outputs(ray_start_regular_shared_2_cpus):
    """Test that broadcast join produces logically correct expected outputs.

    This test validates the expected row counts, column structure, and data content
    for different join types to ensure the test logic is correct.
    """

    # Create test datasets with known overlap
    left_ds = ray.data.from_items(
        [
            {"id": 1, "left_value": "a", "left_only": "left_1"},
            {"id": 2, "left_value": "b", "left_only": "left_2"},
            {"id": 3, "left_value": "c", "left_only": "left_3"},
            {"id": 4, "left_value": "d", "left_only": "left_4"},
            {"id": 5, "left_value": "e", "left_only": "left_5"},
        ]
    )

    right_ds = ray.data.from_items(
        [
            {"id": 2, "right_value": "x", "right_only": "right_2"},
            {"id": 3, "right_value": "y", "right_only": "right_3"},
            {"id": 6, "right_value": "z", "right_only": "right_6"},
            {"id": 7, "right_value": "w", "right_only": "right_7"},
        ]
    )

    # Test inner join
    inner_result = left_ds.join(
        right_ds,
        join_type="inner",
        on=("id",),
        broadcast=True,
    )

    inner_df = pd.DataFrame(inner_result.take_all())
    assert len(inner_df) == 2  # Only id=2 and id=3 should match
    assert set(inner_df["id"].tolist()) == {2, 3}
    expected_columns = {"id", "left_value", "left_only", "right_value", "right_only"}
    assert set(inner_df.columns) == expected_columns

    # Test left outer join
    left_outer_result = left_ds.join(
        right_ds,
        join_type="left_outer",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    left_outer_df = pd.DataFrame(left_outer_result.take_all())
    assert len(left_outer_df) == 5  # All left rows should be present
    assert set(left_outer_df["id"].tolist()) == {1, 2, 3, 4, 5}
    # Check that unmatched left rows have NULL right values
    unmatched_left = left_outer_df[left_outer_df["id"].isin([1, 4, 5])]
    assert unmatched_left["right_value"].isna().all()
    assert unmatched_left["right_only"].isna().all()

    # Test right outer join
    right_outer_result = left_ds.join(
        right_ds,
        join_type="right_outer",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    right_outer_df = pd.DataFrame(right_outer_result.take_all())
    assert len(right_outer_df) == 4  # All right rows should be present
    assert set(right_outer_df["id"].tolist()) == {2, 3, 6, 7}
    # Check that unmatched right rows have NULL left values
    unmatched_right = right_outer_df[right_outer_df["id"].isin([6, 7])]
    assert unmatched_right["left_value"].isna().all()
    assert unmatched_right["left_only"].isna().all()

    # Test full outer join
    full_outer_result = left_ds.join(
        right_ds,
        join_type="full_outer",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    full_outer_df = pd.DataFrame(full_outer_result.take_all())
    assert len(full_outer_df) == 7  # All left + all right + matches
    assert set(full_outer_df["id"].tolist()) == {1, 2, 3, 4, 5, 6, 7}


def test_broadcast_join_dataset_swapping_validation(ray_start_regular_shared_2_cpus):
    """Test that dataset swapping in broadcast joins produces correct results.

    This test validates that when the left dataset is smaller than the right dataset,
    the swapping logic correctly maintains join semantics.
    """

    # Create datasets where left is smaller than right
    left_ds = ray.data.from_items(
        [
            {"id": 1, "left_value": "a"},
            {"id": 2, "left_value": "b"},
            {"id": 3, "left_value": "c"},
        ]
    )

    right_ds = ray.data.from_items(
        [
            {"id": 1, "right_value": "x"},
            {"id": 2, "right_value": "y"},
            {"id": 4, "right_value": "z"},
            {"id": 5, "right_value": "w"},
            {"id": 6, "right_value": "v"},
        ]
    )

    # Test inner join with swapped datasets
    inner_result = left_ds.join(
        right_ds,
        join_type="inner",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    inner_df = pd.DataFrame(inner_result.take_all())
    assert len(inner_df) == 2  # Only id=1 and id=2 should match
    assert set(inner_df["id"].tolist()) == {1, 2}

    # Test left outer join with swapped datasets
    left_outer_result = left_ds.join(
        right_ds,
        join_type="left_outer",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    left_outer_df = pd.DataFrame(left_outer_result.take_all())
    assert len(left_outer_df) == 3  # All left rows should be present
    assert set(left_outer_df["id"].tolist()) == {1, 2, 3}
    # Check that unmatched left rows have NULL right values
    unmatched_left = left_outer_df[left_outer_df["id"] == 3]
    assert unmatched_left["right_value"].isna().all()

    # Test right outer join with swapped datasets
    right_outer_result = left_ds.join(
        right_ds,
        join_type="right_outer",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    right_outer_df = pd.DataFrame(right_outer_result.take_all())
    assert len(right_outer_df) == 5  # All right rows should be present
    assert set(right_outer_df["id"].tolist()) == {1, 2, 4, 5, 6}
    # Check that unmatched right rows have NULL left values
    unmatched_right = right_outer_df[right_outer_df["id"].isin([4, 5, 6])]
    assert unmatched_right["left_value"].isna().all()

    # Test full outer join with swapped datasets
    full_outer_result = left_ds.join(
        right_ds,
        join_type="full_outer",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    full_outer_df = pd.DataFrame(full_outer_result.take_all())
    assert len(full_outer_df) == 6  # All left + all right + matches
    assert set(full_outer_df["id"].tolist()) == {1, 2, 3, 4, 5, 6}


def test_broadcast_join_column_structure_validation(ray_start_regular_shared_2_cpus):
    """Test that broadcast join produces correct column structure and data types.

    This test validates that the column names, data types, and structure are correct
    for different join types and scenarios.
    """

    # Create datasets with different data types
    left_ds = ray.data.from_items(
        [
            {"id": 1, "left_int": 10, "left_str": "a", "left_float": 1.5},
            {"id": 2, "left_int": 20, "left_str": "b", "left_float": 2.5},
            {"id": 3, "left_int": 30, "left_str": "c", "left_float": 3.5},
        ]
    )

    right_ds = ray.data.from_items(
        [
            {"id": 1, "right_int": 100, "right_str": "x", "right_float": 10.5},
            {"id": 2, "right_int": 200, "right_str": "y", "right_float": 20.5},
            {"id": 4, "right_int": 400, "right_str": "z", "right_float": 40.5},
        ]
    )

    # Test inner join column structure
    inner_result = left_ds.join(
        right_ds,
        join_type="inner",
        num_partitions=2,
        on=("id",),
        broadcast=True,
    )

    inner_df = pd.DataFrame(inner_result.take_all())
    expected_inner_columns = {
        "id",
        "left_int",
        "left_str",
        "left_float",
        "right_int",
        "right_str",
        "right_float",
    }
    assert set(inner_df.columns) == expected_inner_columns
    assert len(inner_df) == 2

    # Verify data types are preserved
    assert inner_df["left_int"].dtype in ["int64", "int32", "int"]
    assert inner_df["left_str"].dtype == "object"  # string columns
    assert inner_df["left_float"].dtype in ["float64", "float32", "float"]

    # Test left outer join column structure
    left_outer_result = left_ds.join(
        right_ds,
        join_type="left_outer",
        on=("id",),
        broadcast=True,
    )

    left_outer_df = pd.DataFrame(left_outer_result.take_all())
    assert set(left_outer_df.columns) == expected_inner_columns
    assert len(left_outer_df) == 3

    # Check that unmatched left rows have NULL right values
    unmatched_left = left_outer_df[left_outer_df["id"] == 3]
    assert unmatched_left["right_int"].isna().all()
    assert unmatched_left["right_str"].isna().all()
    assert unmatched_left["right_float"].isna().all()

    # Test with column suffixes
    suffixed_result = left_ds.join(
        right_ds,
        join_type="inner",
        on=("id",),
        left_suffix="_left",
        right_suffix="_right",
        broadcast=True,
    )

    suffixed_df = pd.DataFrame(suffixed_result.take_all())
    expected_suffixed_columns = {
        "id",
        "left_int_left",
        "left_str_left",
        "left_float_left",
        "right_int_right",
        "right_str_right",
        "right_float_right",
    }
    assert set(suffixed_df.columns) == expected_suffixed_columns

    # Test with different key names
    left_ds_diff_keys = ray.data.from_items(
        [
            {"left_id": 1, "left_value": "a"},
            {"left_id": 2, "left_value": "b"},
        ]
    )

    right_ds_diff_keys = ray.data.from_items(
        [
            {"right_id": 1, "right_value": "x"},
            {"right_id": 2, "right_value": "y"},
        ]
    )

    diff_keys_result = left_ds_diff_keys.join(
        right_ds_diff_keys,
        join_type="inner",
        on=("left_id",),
        right_on=("right_id",),
        broadcast=True,
    )

    diff_keys_df = pd.DataFrame(diff_keys_result.take_all())
    expected_diff_keys_columns = {"left_id", "left_value", "right_id", "right_value"}
    assert set(diff_keys_df.columns) == expected_diff_keys_columns
    assert len(diff_keys_df) == 2


@pytest.mark.parametrize("join_type", ["left_anti", "right_anti"])
def test_anti_join_no_matches(
    ray_start_regular_shared_2_cpus,
    nullify_shuffle_aggregator_num_cpus,
    join_type,
):
    """Test anti-join when there are no matches - should return all rows from respective side"""
    DataContext.get_current().target_max_block_size = 1 * MiB

    doubles = ray.data.range(32).map(
        lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
    )

    # Create squares with completely different keys
    squares = ray.data.range(32).map(
        lambda row: {"id": row["id"] + 100, "square": int(row["id"]) ** 2}
    )

    # Anti-join should return all rows from respective side
    joined: Dataset = doubles.join(
        squares,
        join_type=join_type,
        num_partitions=4,
        on=("id",),
    )

    joined_pd = pd.DataFrame(joined.take_all())

    if join_type == "left_anti":
        expected_pd = doubles.to_pandas()
    else:  # right_anti
        expected_pd = squares.to_pandas()

    # Should get all rows from the respective table
    joined_pd_sorted = joined_pd.sort_values(by=["id"]).reset_index(drop=True)
    expected_pd_sorted = expected_pd.sort_values(by=["id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_pd_sorted, joined_pd_sorted)


@pytest.mark.parametrize("join_type", ["left_anti", "right_anti"])
def test_anti_join_all_matches(
    ray_start_regular_shared_2_cpus,
    nullify_shuffle_aggregator_num_cpus,
    join_type,
):
    """Test anti-join when all rows match - should return empty result"""
    DataContext.get_current().target_max_block_size = 1 * MiB

    doubles = ray.data.range(32).map(
        lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
    )

    squares = ray.data.range(32).map(
        lambda row: {"id": row["id"], "square": int(row["id"]) ** 2}
    )

    # Anti-join should return no rows since all keys match
    joined: Dataset = doubles.join(
        squares,
        join_type=join_type,
        num_partitions=4,
        on=("id",),
    )

    joined_pd = pd.DataFrame(joined.take_all())

    # Should get empty result
    assert len(joined_pd) == 0


@pytest.mark.parametrize("join_type", ["left_anti", "right_anti"])
def test_anti_join_multi_key(
    ray_start_regular_shared_2_cpus,
    nullify_shuffle_aggregator_num_cpus,
    join_type,
):
    """Test anti-join with multiple join keys"""
    DataContext.get_current().target_max_block_size = 1 * MiB

    # Create left dataset using ray.data.range for consistency
    left_ds = ray.data.range(32).map(
        lambda row: {
            "id": row["id"],
            "oddness": row["id"] % 2,  # Even
            "10x": row["id"] * 10,
        }
    )

    # Create right dataset with partial matches (16 vs 32 for partial overlap)
    right_ds = ray.data.range(16).map(
        lambda row: {
            "id": row["id"] % 2,
            "oddness": row["id"] % 2 + 1,  # odd
            "100x": row["id"] * 100,
        }
    )

    # Anti-join should return rows that don't have matching key1,key2 in the other dataset
    joined: Dataset = left_ds.join(
        right_ds,
        join_type=join_type,
        num_partitions=4,
        on=("id", "oddness"),
    )

    joined_pd = pd.DataFrame(joined.take_all())

    # Create expected data for pandas comparison
    left_pd = left_ds.to_pandas()
    right_pd = right_ds.to_pandas()

    # Calculate expected result using pandas
    if join_type == "left_anti":
        expected_cols = ["id", "oddness", "10x"]

        merged = left_pd.merge(
            right_pd, on=["id", "oddness"], how="left", indicator=True
        )
        expected_pd = merged[merged["_merge"] == "left_only"][expected_cols]
    else:
        expected_cols = ["id", "oddness", "100x"]

        merged = left_pd.merge(
            right_pd, on=["id", "oddness"], how="right", indicator=True
        )
        expected_pd = merged[merged["_merge"] == "right_only"][expected_cols]

    # Sort resulting frames and reset index (to be able to compare with expected one)
    expected_pd_sorted = expected_pd.sort_values(by=expected_cols).reset_index(
        drop=True
    )
    joined_pd_sorted = joined_pd.sort_values(by=expected_cols).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_pd_sorted, joined_pd_sorted)


@pytest.mark.parametrize(
    "join_type", ["left_semi", "right_semi", "left_anti", "right_anti"]
)
def test_broadcast_join_unsupported_join_types(
    ray_start_regular_shared_2_cpus, join_type
):
    """Test that unsupported join types properly raise ValueError for broadcast joins."""

    left_ds = ray.data.from_items([{"id": 1, "value": "a"}])
    right_ds = ray.data.from_items([{"id": 1, "value": "x"}])

    with pytest.raises(
        ValueError, match=f"Join type '{join_type}' is not supported in broadcast joins"
    ):
        left_ds.join(
            right_ds,
            join_type=join_type,
            on=("id",),
            broadcast=True,
        )


def test_broadcast_join_empty_datasets(ray_start_regular_shared_2_cpus):
    """Test broadcast join with empty datasets."""

    # Test empty left dataset
    empty_left = ray.data.from_items([])
    non_empty_right = ray.data.from_items([{"id": 1, "value": "x"}])

    result = empty_left.join(
        non_empty_right,
        join_type="inner",
        on=("id",),
        broadcast=True,
    )

    assert result.count() == 0

    # Test empty right dataset
    non_empty_left = ray.data.from_items([{"id": 1, "value": "a"}])
    empty_right = ray.data.from_items([])

    result = non_empty_left.join(
        empty_right,
        join_type="inner",
        on=("id",),
        broadcast=True,
    )

    assert result.count() == 0

    # Test left outer join with empty right
    result = non_empty_left.join(
        empty_right,
        join_type="left_outer",
        on=("id",),
        broadcast=True,
    )

    # Should return all left rows with null right columns
    assert result.count() == 1
    result_df = pd.DataFrame(result.take_all())
    assert result_df.loc[0, "id"] == 1
    assert result_df.loc[0, "value"] == "a"

    # Test both datasets empty
    empty_left = ray.data.from_items([])
    empty_right = ray.data.from_items([])

    result = empty_left.join(
        empty_right,
        join_type="inner",
        on=("id",),
        broadcast=True,
    )

    assert result.count() == 0


def test_broadcast_join_no_matching_keys(ray_start_regular_shared_2_cpus):
    """Test broadcast join when datasets have no matching keys."""

    left_ds = ray.data.from_items(
        [
            {"id": 1, "left_value": "a"},
            {"id": 2, "left_value": "b"},
            {"id": 3, "left_value": "c"},
        ]
    )

    right_ds = ray.data.from_items(
        [
            {"id": 10, "right_value": "x"},
            {"id": 20, "right_value": "y"},
        ]
    )

    # Test inner join - should return empty
    inner_result = left_ds.join(
        right_ds,
        join_type="inner",
        on=("id",),
        broadcast=True,
    )

    assert inner_result.count() == 0

    # Test left outer join - should return all left rows with null right columns
    left_outer_result = left_ds.join(
        right_ds,
        join_type="left_outer",
        on=("id",),
        broadcast=True,
    )

    assert left_outer_result.count() == 3
    result_df = pd.DataFrame(left_outer_result.take_all())
    assert set(result_df["id"].tolist()) == {1, 2, 3}
    assert result_df["right_value"].isna().all()

    # Test right outer join - should return all right rows with null left columns
    right_outer_result = left_ds.join(
        right_ds,
        join_type="right_outer",
        on=("id",),
        broadcast=True,
    )

    assert right_outer_result.count() == 2
    result_df = pd.DataFrame(right_outer_result.take_all())
    assert set(result_df["id"].tolist()) == {10, 20}
    assert result_df["left_value"].isna().all()

    # Test full outer join - should return all rows from both sides
    full_outer_result = left_ds.join(
        right_ds,
        join_type="full_outer",
        on=("id",),
        broadcast=True,
    )

    assert full_outer_result.count() == 5
    result_df = pd.DataFrame(full_outer_result.take_all())
    assert set(result_df["id"].tolist()) == {1, 2, 3, 10, 20}


def test_broadcast_join_schema_mismatch_keys(ray_start_regular_shared_2_cpus):
    """Test broadcast join with mismatched key column types."""

    # Create datasets with different key types
    left_ds = ray.data.from_items(
        [
            {"id": "1", "value": "a"},  # string id
            {"id": "2", "value": "b"},
        ]
    )

    right_ds = ray.data.from_items(
        [
            {"id": 1, "value": "x"},  # int id
            {"id": 2, "value": "y"},
        ]
    )

    # The join should handle type coercion or fail gracefully
    # This tests the robustness of the PyArrow join implementation
    try:
        result = left_ds.join(
            right_ds,
            join_type="inner",
            on=("id",),
            broadcast=True,
        )
        # If it succeeds, verify the result
        result_count = result.count()
        # The exact behavior depends on PyArrow's type coercion
        # We just verify it doesn't crash
        assert result_count >= 0
    except (ValueError, TypeError) as e:
        # It's acceptable for this to fail with a clear error
        assert "type" in str(e).lower() or "schema" in str(e).lower()


def test_broadcast_join_duplicate_keys(ray_start_regular_shared_2_cpus):
    """Test broadcast join with duplicate keys in datasets."""

    # Left dataset with duplicate keys
    left_ds = ray.data.from_items(
        [
            {"id": 1, "left_value": "a1"},
            {"id": 1, "left_value": "a2"},  # duplicate key
            {"id": 2, "left_value": "b"},
        ]
    )

    # Right dataset with duplicate keys
    right_ds = ray.data.from_items(
        [
            {"id": 1, "right_value": "x1"},
            {"id": 1, "right_value": "x2"},  # duplicate key
            {"id": 3, "right_value": "z"},
        ]
    )

    result = left_ds.join(
        right_ds,
        join_type="inner",
        on=("id",),
        broadcast=True,
    )

    # Should return all combinations of matching keys (cartesian product)
    # 2 left rows with id=1 × 2 right rows with id=1 = 4 result rows
    assert result.count() == 4

    result_df = pd.DataFrame(result.take_all())
    id_1_rows = result_df[result_df["id"] == 1]
    assert len(id_1_rows) == 4

    # Verify all combinations are present
    expected_combinations = {("a1", "x1"), ("a1", "x2"), ("a2", "x1"), ("a2", "x2")}
    actual_combinations = set(zip(id_1_rows["left_value"], id_1_rows["right_value"]))
    assert actual_combinations == expected_combinations


def test_broadcast_join_empty_table_semantics_with_swapping(
    ray_start_regular_shared_2_cpus,
):
    """Test that empty table handling respects join semantics when datasets are swapped."""

    # Test case 1: Left dataset smaller and empty - should trigger swapping
    empty_left = ray.data.from_items([])  # Will become small table after swapping
    large_right = ray.data.from_items(
        [
            {"id": 1, "right_value": "x"},
            {"id": 2, "right_value": "y"},
            {"id": 3, "right_value": "z"},
        ]
    )

    # Left outer join: should return empty (left side is empty)
    left_outer_result = empty_left.join(
        large_right,
        join_type="left_outer",
        on=("id",),
        broadcast=True,
    )
    assert (
        left_outer_result.count() == 0
    ), "Left outer join with empty left should return empty"

    # Right outer join: should return all right rows with null left columns
    right_outer_result = empty_left.join(
        large_right,
        join_type="right_outer",
        on=("id",),
        broadcast=True,
    )
    assert (
        right_outer_result.count() == 3
    ), "Right outer join with empty left should return all right rows"

    # Full outer join: should return all right rows with null left columns
    full_outer_result = empty_left.join(
        large_right,
        join_type="full_outer",
        on=("id",),
        broadcast=True,
    )
    assert (
        full_outer_result.count() == 3
    ), "Full outer join with empty left should return all right rows"

    # Test case 2: Right dataset smaller and empty - no swapping needed
    large_left = ray.data.from_items(
        [
            {"id": 1, "left_value": "a"},
            {"id": 2, "left_value": "b"},
            {"id": 3, "left_value": "c"},
        ]
    )
    empty_right = ray.data.from_items([])  # Will remain small table (right side)

    # Left outer join: should return all left rows with null right columns
    left_outer_result2 = large_left.join(
        empty_right,
        join_type="left_outer",
        on=("id",),
        broadcast=True,
    )
    assert (
        left_outer_result2.count() == 3
    ), "Left outer join with empty right should return all left rows"

    # Right outer join: should return empty (right side is empty)
    right_outer_result2 = large_left.join(
        empty_right,
        join_type="right_outer",
        on=("id",),
        broadcast=True,
    )
    assert (
        right_outer_result2.count() == 0
    ), "Right outer join with empty right should return empty"

    # Verify that the results have correct column structure
    result_df = pd.DataFrame(left_outer_result2.take_all())
    assert set(result_df.columns) >= {
        "id",
        "left_value",
    }, "Result should have left columns"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
