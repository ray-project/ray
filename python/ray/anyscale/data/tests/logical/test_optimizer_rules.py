import pandas as pd
import pytest

import ray
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_execution_optimizer import _check_valid_plan_and_result
from ray.tests.conftest import *  # noqa


def test_apply_local_limit(ray_start_regular_shared):
    def f1(x):
        return x

    ds = ray.data.range(100, parallelism=2).map(f1).limit(1)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> MapRows[Map(f1)] -> Limit[limit=1]",
        [{"id": 0}],
        ["ReadRange->Map(f1->Limit[1])", "limit=1"],
    )
    assert ds._block_num_rows() == [1]

    # Test larger parallelism still only yields one block.
    ds = ray.data.range(10000, parallelism=50).map(f1).limit(50)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> MapRows[Map(f1)] -> Limit[limit=50]",
        [{"id": i} for i in range(50)],
        ["ReadRange->Map(f1->Limit[50])", "limit=50"],
    )
    assert ds._block_num_rows() == [50]


def test_projection_pushdown(ray_start_regular_shared):
    """Tests that Projection Pushdown works for Parquet."""
    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    cols = ["sepal.length", "petal.width"]
    ds = ds.select_columns(cols)
    # check plan
    logical_plan = ds._plan._logical_plan
    op = logical_plan.dag
    assert isinstance(op, Project), op.name

    # Optimize it
    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
    new_op = optimized_logical_plan.dag

    assert isinstance(new_op, ReadFiles), new_op.name
    assert not any(isinstance(op, Project) for op in new_op.post_order_iter())

    readfiles = new_op
    assert readfiles.columns == cols

    target = ray.data.read_parquet(path).to_pandas()[cols]
    df = ds.to_pandas()
    pd.testing.assert_frame_equal(
        df.sort_values(cols).reset_index(drop=True),
        target.sort_values(cols).reset_index(drop=True),
        check_like=True,
    )


def test_projection_pushdown_on_csv(ray_start_regular_shared):
    """Tests that Proj Pushdown works for Native File-Reader codepath"""
    path = "example://iris.csv"
    ds = ray.data.read_csv(path)
    cols = ["sepal.length", "petal.width"]
    ds = ds.select_columns(cols)

    # Optimize it
    optimized_logical_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
    new_op = optimized_logical_plan.dag

    assert isinstance(new_op, ReadFiles), new_op.name
    assert not any(isinstance(op, Project) for op in new_op.post_order_iter())

    readfiles = new_op
    assert readfiles.columns == cols

    target = ray.data.read_csv(path).to_pandas()[cols]
    df = ds.to_pandas()
    pd.testing.assert_frame_equal(
        df.sort_values(cols).reset_index(drop=True),
        target.sort_values(cols).reset_index(drop=True),
        check_like=True,
    )


def test_projection_pushdown_avoided(ray_start_regular_shared):
    """Tests that Proj Pushdown is avoided when UDFs are provided."""
    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    ds = ds.map_batches(lambda d: d)
    cols = ["sepal.length", "petal.width"]
    ds = ds.select_columns(cols)

    logical_plan = ds._plan._logical_plan
    op = logical_plan.dag
    assert isinstance(op, Project), op.name

    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
    new_op = optimized_logical_plan.dag
    assert isinstance(new_op, Project), new_op.name

    target = ray.data.read_parquet(path).to_pandas()[cols]
    df = ds.to_pandas()
    pd.testing.assert_frame_equal(
        df.sort_values(cols).reset_index(drop=True),
        target.sort_values(cols).reset_index(drop=True),
        check_like=True,
    )


def test_projection_pushdown_no_intersection(ray_start_regular_shared):
    """Check that sequential selects with no intersection are not merged."""
    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    ds = ds.select_columns(["sepal.length", "petal.width"])
    ds = ds.select_columns(["sepal.width"])

    logical_plan = ds._plan._logical_plan
    op = logical_plan.dag
    assert isinstance(op, Project), op.name

    error_msg = (
        "Identified projections where the latter is not a subset " "of the former"
    )
    with pytest.raises(RuntimeError, match=error_msg):
        LogicalOptimizer().optimize(logical_plan)


def test_projection_pushdown_merge(ray_start_regular_shared):
    """Check that sequential selects with intersection are merged."""
    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    ds = ds.map_batches(lambda d: d)
    cols = ["sepal.length", "petal.width"]
    ds = ds.select_columns(cols)
    ds = ds.select_columns(["petal.width"])

    logical_plan = ds._plan._logical_plan
    op = logical_plan.dag
    assert isinstance(op, Project), op.name
    assert op.cols == ["petal.width"], op.columns

    optimized_logical_plan = LogicalOptimizer().optimize(logical_plan)
    assert isinstance(optimized_logical_plan.dag, Project)

    select_op = optimized_logical_plan.dag
    assert select_op.cols == ["petal.width"], select_op.columns


def test_pushdown_divergent_branches(ray_start_regular_shared):
    """Check that sequential selects with intersection are merged."""
    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    ds2 = ds.select_columns(["petal.width"])

    # Execute ds2 with projection pushdown
    ds2.take(1)

    # Execute ds without projection pushdown
    result = ds.take(1)[0]
    result_keys = list(result.keys())
    print(result)
    assert all(
        key in result_keys
        for key in [
            "sepal.length",
            "sepal.width",
            "petal.length",
            "petal.width",
        ]
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
