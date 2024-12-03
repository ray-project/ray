import pandas as pd
import pyarrow.compute as pc
import pytest

import ray
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.data._internal.logical.operators.map_operator import Project
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_execution_optimizer import _check_valid_plan_and_result
from ray.tests.conftest import *  # noqa


@pytest.fixture
def parquet_ds(ray_start_regular_shared):
    """Fixture to load the Parquet dataset for testing."""
    ds = ray.data.read_parquet("example://iris.parquet")
    assert ds.count() == 150
    return ds


@pytest.fixture
def csv_ds(ray_start_regular_shared):
    """Fixture to load the CSV dataset for testing."""
    ds = ray.data.read_csv("example://iris.csv")
    assert ds.count() == 150
    return ds


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


def test_filter_with_udfs(parquet_ds):
    """Test filtering with UDFs where predicate pushdown does not occur."""
    filtered_udf_ds = parquet_ds.filter(lambda r: r["sepal.length"] > 5.0)
    filtered_udf_data = filtered_udf_ds.take_all()
    assert filtered_udf_ds.count() == 118
    assert all(record["sepal.length"] > 5.0 for record in filtered_udf_data)
    _check_valid_plan_and_result(
        filtered_udf_ds,
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] -> "
        "ReadFiles[ReadFiles] -> Filter[Filter(<lambda>)]",
        filtered_udf_data,
    )


def test_filter_with_expressions(parquet_ds):
    """Test filtering with expressions where predicate pushdown occurs."""
    filtered_udf_data = parquet_ds.filter(lambda r: r["sepal.length"] > 5.0).take_all()
    filtered_expr_ds = parquet_ds.filter(expr="sepal.length > 5.0")
    _check_valid_plan_and_result(
        filtered_expr_ds,
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] -> "
        "ReadFiles[ReadFiles]",
        filtered_udf_data,
    )


@pytest.mark.parametrize(
    "source_expr,filter_expr,check",
    [
        # Test with PyArrow compute expressions
        (
            pc.greater(pc.field("sepal.length"), pc.scalar(5.0)),
            "sepal.width > 3.0",
            lambda r: r["sepal.length"] > 5.0 and r["sepal.width"] > 3.0,
        ),
        # Test with PyArrow DNF form
        (
            [("sepal.length", "<", 4.0)],
            "sepal.width < 2.0",
            lambda r: r["sepal.length"] < 4.0 and r["sepal.width"] < 2.0,
        ),
        (
            [[("variety", "=", "Setosa"), ("sepal.length", ">", 5.0)]],
            "petal.length > 1.0",
            lambda r: (r["variety"] == "Setosa" and r["sepal.length"] > 5.0)
            and r["petal.length"] > 1.0,
        ),
    ],
)
def test_filter_pushdown_source_and_op(
    ray_start_regular_shared, source_expr, filter_expr, check
):
    """Test filtering when expressions are provided both in source and operator.

    Tests both PyArrow compute expressions and DNF form filters for source.
    """
    ds = ray.data.read_parquet("example://iris.parquet", filter=source_expr).filter(
        expr=filter_expr
    )
    result = ds.take_all()
    assert all(check(k) for k in result)
    _check_valid_plan_and_result(
        ds,
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] -> "
        "ReadFiles[ReadFiles]",
        result,
    )


def test_chained_filter_with_expressions(parquet_ds):
    """Test chained filtering with expressions where combined pushdown occurs."""
    filtered_expr_chained_ds = (
        parquet_ds.filter(expr="sepal.length > 1.0")
        .filter(expr="sepal.length > 2.0")
        .filter(expr="sepal.length > 3.0")
        .filter(expr="sepal.length > 3.0")
        .filter(expr="sepal.length > 5.0")
    )
    filtered_udf_data = parquet_ds.filter(lambda r: r["sepal.length"] > 5.0).take_all()
    _check_valid_plan_and_result(
        filtered_expr_chained_ds,
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] -> "
        "ReadFiles[ReadFiles]",
        filtered_udf_data,
    )


@pytest.mark.parametrize(
    "filter_fn,expected_plan",
    [
        (
            lambda ds: ds.filter(lambda r: r["sepal.length"] > 5.0),
            "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] -> "
            "ReadFiles[ReadFiles] -> Filter[Filter(<lambda>)]",
        ),
        (
            lambda ds: ds.filter(expr="sepal.length > 5.0"),
            "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] -> "
            "ReadFiles[ReadFiles]",
        ),
    ],
)
def test_filter_pushdown_csv(csv_ds, filter_fn, expected_plan):
    """Test filtering on CSV files with and without predicate pushdown."""
    filtered_ds = filter_fn(csv_ds)
    filtered_data = filtered_ds.take_all()
    assert filtered_ds.count() == 118
    assert all(record["sepal.length"] > 5.0 for record in filtered_data)
    _check_valid_plan_and_result(
        filtered_ds,
        expected_plan,
        filtered_data,
    )


def test_filter_mixed(csv_ds):
    """Test that mixed function and expressions work."""
    csv_ds = csv_ds.filter(lambda r: r["sepal.length"] < 5.0)
    csv_ds = csv_ds.filter(expr="sepal.length > 3.0")
    csv_ds = csv_ds.filter(expr="sepal.length > 4.0")
    csv_ds = csv_ds.map(lambda x: x)
    csv_ds = csv_ds.filter(expr="sepal.length > 2.0")
    csv_ds = csv_ds.filter(expr="sepal.length > 1.0")
    filtered_expr_data = csv_ds.take_all()
    assert csv_ds.count() == 22
    assert all(record["sepal.length"] < 5.0 for record in filtered_expr_data)
    assert all(record["sepal.length"] > 4.0 for record in filtered_expr_data)
    _check_valid_plan_and_result(
        csv_ds,
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] -> "
        "ReadFiles[ReadFiles] -> Filter[Filter(<lambda>)] -> "
        "Filter[Filter(<expression>)] -> MapRows[Map(<lambda>)] -> "
        "Filter[Filter(<expression>)]",
        filtered_expr_data,
    )


@pytest.mark.parametrize(
    "ds_creator",
    [
        lambda: ray.data.read_parquet("example://iris.parquet"),
        lambda: ray.data.read_csv("example://iris.csv"),
    ],
)
def test_filter_mixed_expression_first(ds_creator):
    """Test that mixed functional and expressions work."""
    ds = ds_creator()
    ds = ds.filter(expr="sepal.length > 3.0")
    ds = ds.filter(expr="sepal.length > 4.0")
    ds = ds.filter(lambda r: r["sepal.length"] < 5.0)
    filtered_expr_data = ds.take_all()
    assert ds.count() == 22
    assert all(record["sepal.length"] < 5.0 for record in filtered_expr_data)
    assert all(record["sepal.length"] > 4.0 for record in filtered_expr_data)
    _check_valid_plan_and_result(
        ds,
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] -> "
        "ReadFiles[ReadFiles] -> Filter[Filter(<lambda>)]",
        filtered_expr_data,
    )


def test_filter_mixed_expression_not_readfiles(ray_start_regular_shared):
    """Test that mixed functional and expressions work."""
    ds = ray.data.range(100).filter(expr="id > 1.0")
    ds = ds.filter(expr="id > 2.0")
    ds = ds.filter(lambda r: r["id"] < 5.0)
    filtered_expr_data = ds.take_all()
    assert ds.count() == 2
    assert all(record["id"] < 5.0 for record in filtered_expr_data)
    assert all(record["id"] > 2.0 for record in filtered_expr_data)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Filter[Filter(<expression>)] -> "
        "Filter[Filter(<lambda>)]",
        filtered_expr_data,
    )


def test_read_range_union_with_filter_pushdown(ray_start_regular_shared):
    ds1 = ray.data.range(100, parallelism=2)
    ds2 = ray.data.range(100, parallelism=2)
    ds = ds1.union(ds2).filter(expr="id >= 50")
    result = ds.take_all()
    assert ds.count() == 100
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Filter[Filter(<expression>)], "
        "Read[ReadRange] -> Filter[Filter(<expression>)] -> Union[Union]",
        result,
    )


def test_multiple_union_with_filter_pushdown(ray_start_regular_shared):
    ds1 = ray.data.read_parquet("example://iris.parquet")
    ds2 = ray.data.read_parquet("example://iris.parquet")
    ds3 = ray.data.read_parquet("example://iris.parquet")
    ds = ds1.union(ds2).union(ds3).filter(expr="sepal.length > 5.0")
    result = ds.take_all()
    assert ds.count() == 354
    assert all(record["sepal.length"] > 5.0 for record in result)
    _check_valid_plan_and_result(
        ds,
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] "
        "-> ReadFiles[ReadFiles], "
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] "
        "-> ReadFiles[ReadFiles] -> Union[Union], "
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] "
        "-> ReadFiles[ReadFiles] -> Union[Union]",
        result,
    )


def test_multiple_filter_with_union_pushdown_parquet(ray_start_regular_shared):
    ds1 = ray.data.read_parquet("example://iris.parquet")
    ds1 = ds1.filter(expr="sepal.width > 2.0")
    ds2 = ray.data.read_parquet("example://iris.parquet")
    ds2 = ds2.filter(expr="sepal.width > 2.0")
    ds = ds1.union(ds2).filter(expr="sepal.length < 5.0")
    result = ds.take_all()
    assert all(record["sepal.width"] > 2.0 for record in result)
    assert all(record["sepal.length"] < 5.0 for record in result)

    assert ds.count() == 44
    _check_valid_plan_and_result(
        ds,
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] "
        "-> ReadFiles[ReadFiles], "
        "ListFiles[ListFiles] -> PartitionFiles[PartitionFiles] "
        "-> ReadFiles[ReadFiles] -> Union[Union]",
        result,
    )


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
