import re
from typing import Any, List

import pandas as pd
import pyarrow.compute as pc
import pytest

import ray
from ray.data import Dataset
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_execution_optimizer_limit_pushdown import (
    _check_valid_plan_and_result,
)
from ray.tests.conftest import *  # noqa

# Pattern to match read operators in logical plans.
# Matches Read[Read<Format>] where format is Parquet, CSV, Range, etc.
READ_OPERATOR_PATTERN = (
    r"^(Read\[Read\w+\]|ListFiles\[ListFiles\] -> ReadFiles\[ReadFiles\])"
)


def _check_plan_with_flexible_read(
    ds: Dataset, expected_plan_suffix: str, expected_result: List[Any]
):
    """Check the logical plan with flexible read operator matching.

    This function allows flexibility in the read operator part of the plan
    by using a configurable pattern (READ_OPERATOR_PATTERN).

    Args:
        ds: The dataset to check.
        expected_plan_suffix: The expected plan after the read operator(s).
            If empty string, only the read operator is expected.
        expected_result: The expected result data.
    """
    # Optimize the logical plan before checking
    logical_plan = ds._plan._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)
    actual_plan = optimized_plan.dag.dag_str

    match = re.match(READ_OPERATOR_PATTERN, actual_plan)
    assert match, f"Expected plan to start with read operator, got: {actual_plan}"

    # Check if there's a suffix expected
    if expected_plan_suffix:
        # The suffix should appear after the read operator
        expected_full_pattern = (
            f"{READ_OPERATOR_PATTERN} -> {re.escape(expected_plan_suffix)}"
        )
        assert re.match(expected_full_pattern, actual_plan), (
            f"Expected plan to match pattern with suffix '{expected_plan_suffix}', "
            f"got: {actual_plan}"
        )
    # If no suffix, the plan should be just the read operator
    else:
        assert actual_plan == match.group(
            1
        ), f"Expected plan to be just the read operator, got: {actual_plan}"

    # Check the result
    assert ds.take_all() == expected_result


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


def test_filter_with_udfs(parquet_ds):
    """Test filtering with UDFs where predicate pushdown does not occur."""
    filtered_udf_ds = parquet_ds.filter(lambda r: r["sepal.length"] > 5.0)
    filtered_udf_data = filtered_udf_ds.take_all()
    assert filtered_udf_ds.count() == 118
    assert all(record["sepal.length"] > 5.0 for record in filtered_udf_data)
    _check_plan_with_flexible_read(
        filtered_udf_ds,
        "Filter[Filter(<lambda>)]",  # UDF filter doesn't push down
        filtered_udf_data,
    )


def test_filter_with_expressions(parquet_ds):
    """Test filtering with expressions where predicate pushdown occurs."""
    filtered_udf_data = parquet_ds.filter(lambda r: r["sepal.length"] > 5.0).take_all()
    filtered_expr_ds = parquet_ds.filter(expr="sepal.length > 5.0")
    _check_plan_with_flexible_read(
        filtered_expr_ds,
        "",  # Pushed down to read, no additional operators
        filtered_udf_data,
    )


def test_filter_pushdown_source_and_op(ray_start_regular_shared):
    """Test filtering when expressions are provided both in source and operator."""
    # Test with PyArrow compute expressions
    source_expr = pc.greater(pc.field("sepal.length"), pc.scalar(5.0))
    filter_expr = "sepal.width > 3.0"

    ds = ray.data.read_parquet("example://iris.parquet", filter=source_expr).filter(
        expr=filter_expr
    )
    result = ds.take_all()
    assert all(r["sepal.length"] > 5.0 and r["sepal.width"] > 3.0 for r in result)
    _check_plan_with_flexible_read(
        ds,
        "",  # Both filters pushed down to read
        result,
    )


def test_chained_filter_with_expressions(parquet_ds):
    """Test chained filtering with expressions where combined pushdown occurs."""
    filtered_expr_chained_ds = (
        parquet_ds.filter(expr=col("sepal.length") > 1.0)
        .filter(expr=col("sepal.length") > 2.0)
        .filter(expr=col("sepal.length") > 3.0)
        .filter(expr=col("sepal.length") > 3.0)
        .filter(expr=col("sepal.length") > 5.0)
    )
    filtered_udf_data = parquet_ds.filter(lambda r: r["sepal.length"] > 5.0).take_all()
    _check_plan_with_flexible_read(
        filtered_expr_chained_ds,
        "",  # All filters combined and pushed down to read
        filtered_udf_data,
    )


@pytest.mark.parametrize(
    "filter_fn,expected_suffix",
    [
        (
            lambda ds: ds.filter(lambda r: r["sepal.length"] > 5.0),
            "Filter[Filter(<lambda>)]",  # UDF filter doesn't push down
        ),
        (
            lambda ds: ds.filter(expr=col("sepal.length") > 5.0),
            "",  # Expression filter pushes down to read
        ),
    ],
)
def test_filter_pushdown_csv(csv_ds, filter_fn, expected_suffix):
    """Test filtering on CSV files with predicate pushdown."""
    filtered_ds = filter_fn(csv_ds)
    filtered_data = filtered_ds.take_all()
    assert filtered_ds.count() == 118
    assert all(record["sepal.length"] > 5.0 for record in filtered_data)
    _check_plan_with_flexible_read(
        filtered_ds,
        expected_suffix,
        filtered_data,
    )


def test_filter_mixed(csv_ds):
    """Test that mixed function and expressions work (CSV supports predicate pushdown)."""
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
    # After optimization: expression filters before map get fused, expression filters after map get fused
    _check_plan_with_flexible_read(
        csv_ds,
        "Filter[Filter(<lambda>)] -> Filter[Filter(<expression>)] -> "
        "MapRows[Map(<lambda>)] -> Filter[Filter(<expression>)]",
        filtered_expr_data,
    )


def test_filter_mixed_expression_first_parquet(ray_start_regular_shared):
    """Test that mixed functional and expressions work with Parquet (supports predicate pushdown)."""
    ds = ray.data.read_parquet("example://iris.parquet")
    ds = ds.filter(expr="sepal.length > 3.0")
    ds = ds.filter(expr="sepal.length > 4.0")
    ds = ds.filter(lambda r: r["sepal.length"] < 5.0)
    filtered_expr_data = ds.take_all()
    assert ds.count() == 22
    assert all(record["sepal.length"] < 5.0 for record in filtered_expr_data)
    assert all(record["sepal.length"] > 4.0 for record in filtered_expr_data)
    _check_plan_with_flexible_read(
        ds,
        "Filter[Filter(<lambda>)]",  # Expressions pushed down, UDF remains
        filtered_expr_data,
    )


def test_filter_mixed_expression_first_csv(ray_start_regular_shared):
    """Test that mixed functional and expressions work with CSV (supports predicate pushdown)."""
    ds = ray.data.read_csv("example://iris.csv")
    ds = ds.filter(expr="sepal.length > 3.0")
    ds = ds.filter(expr="sepal.length > 4.0")
    ds = ds.filter(lambda r: r["sepal.length"] < 5.0)
    filtered_expr_data = ds.take_all()
    assert ds.count() == 22
    assert all(record["sepal.length"] < 5.0 for record in filtered_expr_data)
    assert all(record["sepal.length"] > 4.0 for record in filtered_expr_data)
    # Expression filters pushed down to read, UDF filter remains
    _check_plan_with_flexible_read(
        ds,
        "Filter[Filter(<lambda>)]",
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

    # For union operations, verify the pattern separately for each branch
    actual_plan = ds._plan._logical_plan.dag.dag_str
    # Check that filter was pushed down into all three reads (no Filter operator in plan)
    assert (
        "Filter[Filter" not in actual_plan
    ), f"Filter should be pushed down, got: {actual_plan}"
    # Check that union operations are present
    assert (
        actual_plan.count("Union[Union]") == 2
    ), f"Expected 2 unions, got: {actual_plan}"
    # Check result
    assert ds.take_all() == result


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

    # For union operations, verify the pattern separately for each branch
    actual_plan = ds._plan._logical_plan.dag.dag_str
    # Check that all filters were pushed down (no Filter operator in plan)
    assert (
        "Filter[Filter" not in actual_plan
    ), f"Filters should be pushed down, got: {actual_plan}"
    # Check that union operation is present
    assert "Union[Union]" in actual_plan, f"Expected union, got: {actual_plan}"
    # Check result
    assert ds.take_all() == result


@pytest.mark.parametrize(
    "operations,output_rename_map,expected_filter_expr,test_id",
    [
        (
            # rename("sepal.length" -> a).filter(a)
            lambda ds: ds.rename_columns({"sepal.length": "a"}).filter(
                expr=col("a") > 2.0
            ),
            {"a": "sepal.length"},
            col("sepal.length") > 2.0,
            "rename_filter",
        ),
        (
            # rename("sepal.length" -> a).filter(a).rename(a -> b)
            lambda ds: ds.rename_columns({"sepal.length": "a"})
            .filter(expr=col("a") > 2.0)
            .rename_columns({"a": "b"}),
            {"b": "sepal.length"},
            col("sepal.length") > 2.0,
            "rename_filter_rename",
        ),
        (
            # rename("sepal.length" -> a).filter(a).rename(a -> b).filter(b)
            lambda ds: ds.rename_columns({"sepal.length": "a"})
            .filter(expr=col("a") > 2.0)
            .rename_columns({"a": "b"})
            .filter(expr=col("b") < 5.0),
            {"b": "sepal.length"},
            (col("sepal.length") > 2.0) & (col("sepal.length") < 5.0),
            "rename_filter_rename_filter",
        ),
        (
            # rename("sepal.length" -> a).filter(a).rename(a -> b).filter(b).rename("sepal.width" -> a)
            # Here column a is referred multiple times in rename
            lambda ds: ds.rename_columns({"sepal.length": "a"})
            .filter(expr=col("a") > 2.0)
            .rename_columns({"a": "b"})
            .filter(expr=col("b") < 5.0)
            .rename_columns({"sepal.width": "a"}),
            {"b": "sepal.length", "a": "sepal.width"},
            (col("sepal.length") > 2.0) & (col("sepal.length") < 5.0),
            "rename_filter_rename_filter_rename",
        ),
    ],
    ids=lambda x: x if isinstance(x, str) else "",
)
def test_pushdown_with_rename_and_filter(
    ray_start_regular_shared,
    operations,
    output_rename_map,
    expected_filter_expr,
    test_id,
):
    """Test predicate pushdown with various combinations of rename and filter operations."""
    path = "example://iris.parquet"
    ds = operations(ray.data.read_parquet(path))
    result = ds.take_all()

    # Check that plan is just the read (filters and renames pushed down/fused)
    _check_plan_with_flexible_read(ds, "", result)

    ds1 = ray.data.read_parquet(path).filter(expr=expected_filter_expr)
    # Convert to pandas to ensure both datasets are fully executed
    df = ds.to_pandas().rename(columns=output_rename_map)
    df1 = ds1.to_pandas()
    assert len(df) == len(df1), f"Expected {len(df)} rows, got {len(df1)} rows"
    pd.testing.assert_frame_equal(df, df1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
