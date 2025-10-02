from typing import Optional

import numpy as np
import pandas as pd
import pytest
from packaging.version import parse as parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.util import MiB
from ray.data.context import DataContext
from ray.data.dataset import Dataset
from ray.exceptions import RayTaskError
from ray.tests.conftest import *  # noqa


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


@pytest.mark.parametrize("join_type", ["left_anti", "right_anti"])
def test_anti_join_no_matches(
    ray_start_regular_shared_2_cpus,
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


# Helper functions to reduce test code bloat
def _assert_columns_match(result, expected_columns):
    """Assert that result has the expected column schema."""
    actual_columns = set(result[0].keys())
    assert actual_columns == expected_columns


def _assert_list_values(result_by_id, expected_values):
    """Assert list column values match expected values."""
    for row_id, expected_list in expected_values.items():
        assert result_by_id[row_id]["list_col"] == expected_list


def _assert_tensor_values(result_by_id, expected_values):
    """Assert tensor column values match expected tensor data."""
    for row_id, expected_tensor in expected_values.items():
        assert np.array_equal(result_by_id[row_id]["tensor_col"], expected_tensor)


def _assert_none_values(result_by_id, none_checks):
    """Assert that specified columns are None for specified row IDs."""
    for row_id, columns in none_checks.items():
        for column in columns:
            assert result_by_id[row_id][column] is None


def _assert_scalar_values(result_by_id, expected_values):
    """Assert scalar column values match expected values."""
    for row_id, column_values in expected_values.items():
        for column, expected_value in column_values.items():
            assert result_by_id[row_id][column] == expected_value


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("10.0.0"),
    reason="""Joins use empty arrays with type coercion. This pyarrow
    version does not support type coercion of extension types, which
    are needed for tensors.""",
)
@pytest.mark.parametrize(
    "join_type",
    [
        "inner",
        "left_outer",
        "right_outer",
        "full_outer",
        "left_semi",
        "right_semi",
        "left_anti",
        "right_anti",
    ],
)
def test_join_with_unjoinable_non_key_columns(
    ray_start_regular_shared_2_cpus, join_type
):
    """Test that joins work correctly when non-key columns have unjoinable types."""
    # Left dataset with joinable key but unjoinable non-key columns

    # Create test data - centralized for clarity and maintainability
    list_data = [
        [1, 2, 3],  # list for id=0
        [4, 5, 6],  # list for id=1
        [7, 8, 9],  # list for id=2
    ]

    tensor_data = [
        np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32),  # 2x2 tensor for id=0
        np.array([[5.0, 6.0], [7.0, 8.0]], dtype=np.float32),  # 2x2 tensor for id=1
        np.array([[9.0, 10.0], [11.0, 12.0]], dtype=np.float32),  # 2x2 tensor for id=2
    ]

    scalar_data = ["a", "b", "c"]  # scalar data for id=0,1,2

    left_ds = ray.data.from_items(
        [
            {
                "id": 0,
                "list_col": list_data[0],
                "tensor_col": tensor_data[0],
                "data": scalar_data[0],
            },
            {
                "id": 1,
                "list_col": list_data[1],
                "tensor_col": tensor_data[1],
                "data": scalar_data[1],
            },
            {
                "id": 2,
                "list_col": list_data[2],
                "tensor_col": tensor_data[2],
                "data": scalar_data[2],
            },
        ]
    )

    # Right dataset with joinable key and columns
    # ids: 0, 1, 3 (so id=2 from left won't match, id=3 from right won't match)
    right_ds = ray.data.from_items(
        [
            {"id": 0, "value": "x", "score": 10},
            {"id": 1, "value": "y", "score": 20},
            {"id": 3, "value": "z", "score": 30},
        ]
    )

    # Verify the join worked and includes unjoinable columns
    joined = left_ds.join(right_ds, join_type=join_type, on=("id",), num_partitions=2)
    result = joined.take_all()
    result_by_id = {row["id"]: row for row in result}

    # Basic validation - join should succeed with unjoinable non-key columns
    if join_type == "inner":
        # Should have 2 rows (id=0 and id=1 match)
        assert len(result) == 2
        # Verify unjoinable columns are preserved
        _assert_list_values(result_by_id, {i: list_data[i] for i in [0, 1]})
        _assert_tensor_values(result_by_id, {i: tensor_data[i] for i in [0, 1]})

    elif join_type == "left_outer":
        # Should have 3 rows (all from left: id=0, 1, 2)
        assert len(result) == 3
        # All left unjoinable columns preserved
        _assert_list_values(result_by_id, {i: list_data[i] for i in [0, 1, 2]})
        _assert_tensor_values(result_by_id, {i: tensor_data[i] for i in [0, 1, 2]})
        # Unmatched left row (id=2) should have None for right columns
        _assert_none_values(result_by_id, {2: ["value"]})

    elif join_type == "right_outer":
        # Should have 3 rows (all from right: id=0, 1, 3)
        assert len(result) == 3
        # Matched rows should have unjoinable columns from left
        _assert_list_values(result_by_id, {i: list_data[i] for i in [0, 1]})
        _assert_tensor_values(result_by_id, {i: tensor_data[i] for i in [0, 1]})
        _assert_scalar_values(result_by_id, {3: {"value": "z"}})
        # Unmatched right row (id=3) should have None for left unjoinable columns
        _assert_none_values(result_by_id, {3: ["list_col", "tensor_col"]})

    elif join_type == "full_outer":
        # Should have 4 rows (all from both sides: id=0, 1, 2, 3)
        assert len(result) == 4
        # Matched rows (id=0, 1) should have data from both sides
        _assert_list_values(result_by_id, {i: list_data[i] for i in [0, 1, 2]})
        _assert_tensor_values(result_by_id, {i: tensor_data[i] for i in [0, 1, 2]})
        _assert_scalar_values(
            result_by_id,
            {
                0: {"value": "x"},
                1: {"value": "y"},
                2: {"data": scalar_data[2]},
                3: {"value": "z", "score": 30},
            },
        )
        # Unmatched rows should have None for columns from the other side
        _assert_none_values(
            result_by_id, {2: ["value", "score"], 3: ["list_col", "tensor_col", "data"]}
        )

    elif join_type == "left_semi":
        # Should return left rows that have matches in right (id=0, 1)
        assert len(result) == 2
        _assert_columns_match(result, {"id", "list_col", "tensor_col", "data"})
        _assert_list_values(result_by_id, {i: list_data[i] for i in [0, 1]})
        _assert_tensor_values(result_by_id, {i: tensor_data[i] for i in [0, 1]})

    elif join_type == "left_anti":
        # Should return left rows that DON'T have matches in right (id=2)
        assert len(result) == 1
        _assert_columns_match(result, {"id", "list_col", "tensor_col", "data"})
        _assert_list_values(result_by_id, {2: list_data[2]})
        _assert_tensor_values(result_by_id, {2: tensor_data[2]})
        _assert_scalar_values(result_by_id, {2: {"data": scalar_data[2]}})

    elif join_type == "right_semi":
        # Should return right rows that have matches in left (id=0, 1)
        assert len(result) == 2
        _assert_columns_match(result, {"id", "value", "score"})
        _assert_scalar_values(result_by_id, {0: {"value": "x"}, 1: {"value": "y"}})

    elif join_type == "right_anti":
        # Should return right rows that DON'T have matches in left (id=3)
        assert len(result) == 1
        _assert_columns_match(result, {"id", "value", "score"})
        _assert_scalar_values(result_by_id, {3: {"value": "z", "score": 30}})

    # For outer joins, ensure unjoinable columns are present
    if join_type in ["inner", "left_outer", "right_outer", "full_outer"]:
        _assert_columns_match(
            result, {"id", "list_col", "tensor_col", "data", "value", "score"}
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
