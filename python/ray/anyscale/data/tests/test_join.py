import pandas as pd
import pytest

import ray
from ray.anyscale.data._internal.execution.operators import hash_shuffle
from ray.anyscale.data._internal.logical.operators.join_operator import JoinType
from ray.data import Dataset
from ray.tests.conftest import *  # noqa


@pytest.fixture
def nullify_shuffle_aggregator_num_cpus():
    original = hash_shuffle.DEFAULT_SHUFFLE_AGGREGATOR_NUM_CPUS
    # NOTE: We override this to reduce hardware requirements
    #       for every aggregator (by default requiring 1 logical CPU)
    hash_shuffle.DEFAULT_SHUFFLE_AGGREGATOR_NUM_CPUS = 0

    yield

    hash_shuffle.DEFAULT_SHUFFLE_AGGREGATOR_NUM_CPUS = original


@pytest.mark.parametrize(
    "num_rows_left,num_rows_right",
    [
        (32, 32),
        (32, 16),
        (16, 32),
        # "Degenerate" cases with mostly empty partitions
        (32, 1),
        (1, 32),
    ],
)
def test_simple_inner_join(
    ray_start_regular_shared,
    nullify_shuffle_aggregator_num_cpus,
    num_rows_left,
    num_rows_right,
):
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

    # Join using Ray Data
    joined: Dataset = doubles.join(
        squares,
        join_type="inner",
        num_outputs=16,
        key_column_names=("id",),
    )

    print(joined.count())
    print(joined.take_all())

    # TODO use native to_pandas() instead
    joined_pd = pd.DataFrame(joined.take_all())

    # Sort resulting frame and reset index (to be able to compare with expected one)
    result_pd = joined_pd.sort_values(by=["id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_pd, result_pd)


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
    ray_start_regular_shared,
    nullify_shuffle_aggregator_num_cpus,
    join_type,
    num_rows_left,
    num_rows_right,
):
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
        num_outputs=16,
        key_column_names=("id",),
    )

    joined_pd = pd.DataFrame(joined.take_all())

    # Sort resulting frame and reset index (to be able to compare with expected one)
    result_pd = joined_pd.sort_values(by=["id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_pd, result_pd)


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
    ray_start_regular_shared,
    nullify_shuffle_aggregator_num_cpus,
    num_rows_left,
    num_rows_right,
):
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
        num_outputs=16,
        key_column_names=("id",),
        # NOTE: We override this to reduce hardware requirements
        #       for every aggregator (by default requiring 1 logical CPU)
        aggregator_ray_remote_args={"num_cpus": 0},
    )

    joined_pd = pd.DataFrame(joined.take_all())

    # Sort resulting frame and reset index (to be able to compare with expected one)
    result_pd = joined_pd.sort_values(by=["id"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(expected_pd, result_pd)


@pytest.mark.parametrize("join_type", [jt for jt in JoinType])  # noqa: C416
def test_invalid_join_not_matching_key_columns(ray_start_regular_shared, join_type):
    # Case 1: Check on missing key column
    empty_ds = ray.data.range(0)

    non_empty_ds = ray.data.range(32)

    with pytest.raises(ValueError) as exc_info:
        empty_ds.join(non_empty_ds, join_type, num_outputs=16, key_column_names=("id",))

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
            id_float_type_ds, join_type, num_outputs=16, key_column_names=("id",)
        )

    assert (
        str(exc_info.value)
        == "Key columns are expected to be present and have the same types in both "
        "left and right operands of the join operation: left has "
        "Column  Type\n------  ----\nid      int64, but right has "
        "Column  Type\n------  ----\nid      double"
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
