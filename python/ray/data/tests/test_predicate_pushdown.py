import re
from typing import Any, List

import pandas as pd
import pyarrow.compute as pc
import pytest

import ray
from ray.data import Dataset
from ray.data._internal.logical.operators.all_to_all_operator import (
    Repartition,
    Sort,
)
from ray.data._internal.logical.operators.map_operator import Filter, Project
from ray.data._internal.logical.operators.one_to_one_operator import Limit
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.util import rows_same
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_execution_optimizer_limit_pushdown import (
    _check_valid_plan_and_result,
)
from ray.data.tests.test_util import (
    get_operator_types,
    get_operators_of_type,
    plan_has_operator,
    plan_operator_comes_before,
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
            "Filter[Filter(col('sepal.length') > 5.0)]",  # CSV doesn't support predicate pushdown
        ),
    ],
)
def test_filter_pushdown_csv(csv_ds, filter_fn, expected_suffix):
    """Test filtering on CSV files (CSV doesn't support predicate pushdown)."""
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
    """Test that mixed function and expressions work (CSV doesn't support predicate pushdown)."""
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
    # CSV doesn't support predicate pushdown, so filters stay after Read
    _check_plan_with_flexible_read(
        csv_ds,
        "Filter[Filter(<lambda>)] -> Filter[Filter((col('sepal.length') > 4.0) & (col('sepal.length') > 3.0))] -> "
        "MapRows[Map(<lambda>)] -> Filter[Filter((col('sepal.length') > 1.0) & (col('sepal.length') > 2.0))]",
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
    """Test that mixed functional and expressions work with CSV (doesn't support predicate pushdown)."""
    ds = ray.data.read_csv("example://iris.csv")
    ds = ds.filter(expr="sepal.length > 3.0")
    ds = ds.filter(expr="sepal.length > 4.0")
    ds = ds.filter(lambda r: r["sepal.length"] < 5.0)
    filtered_expr_data = ds.take_all()
    assert ds.count() == 22
    assert all(record["sepal.length"] < 5.0 for record in filtered_expr_data)
    assert all(record["sepal.length"] > 4.0 for record in filtered_expr_data)
    # CSV doesn't support predicate pushdown, so expression filters get fused but not pushed down
    _check_plan_with_flexible_read(
        ds,
        "Filter[Filter((col('sepal.length') > 4.0) & (col('sepal.length') > 3.0))] -> Filter[Filter(<lambda>)]",
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
        "Read[ReadRange] -> Filter[Filter((col('id') > 2.0) & (col('id') > 1.0))] -> "
        "Filter[Filter(<lambda>)]",
        filtered_expr_data,
    )


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


def _get_optimized_plan(ds: Dataset) -> str:
    """Get the optimized logical plan as a string."""
    logical_plan = ds._plan._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)
    return optimized_plan.dag.dag_str


def _check_plan_matches_pattern(ds: Dataset, expected_pattern: str):
    """Check that the optimized plan matches the expected regex pattern."""
    actual_plan = _get_optimized_plan(ds)
    assert re.match(expected_pattern, actual_plan), (
        f"Plan mismatch:\n"
        f"Expected pattern: {expected_pattern}\n"
        f"Actual plan: {actual_plan}"
    )


class TestPredicatePushdownIntoRead:
    """Tests for pushing predicates into Read operators.

    When a data source supports predicate pushdown (like Parquet),
    the filter should be absorbed into the Read operator itself.
    """

    @pytest.fixture
    def parquet_ds(self, ray_start_regular_shared):
        return ray.data.read_parquet("example://iris.parquet")

    def test_complex_pipeline_all_filters_push_to_read(self, parquet_ds):
        """Complex pipeline: filters should push through all operators into Read.

        Pipeline: Read -> Filter -> Rename -> Filter -> Sort -> Repartition
                  -> Filter -> Limit -> Filter

        All filters should fuse, push through all operators, rebind through rename,
        and be absorbed into the Read operator.
        """
        ds = (
            parquet_ds.filter(expr=col("sepal.length") > 4.0)
            .rename_columns({"sepal.length": "len", "sepal.width": "width"})
            .filter(expr=col("len") < 7.0)
            .sort("len")
            .repartition(3)
            .filter(expr=col("width") > 2.5)
            .limit(100)
            .filter(expr=col("len") > 4.5)
        )

        # Verify correctness: should apply all filters correctly
        expected = (
            parquet_ds.filter(
                expr=(col("sepal.length") > 4.0)
                & (col("sepal.length") < 7.0)
                & (col("sepal.width") > 2.5)
                & (col("sepal.length") > 4.5)
            )
            .rename_columns({"sepal.length": "len", "sepal.width": "width"})
            .sort("len")
            .repartition(3)
            .limit(100)
        )

        assert rows_same(ds.to_pandas(), expected.to_pandas())

        # Verify plan: all filters pushed into Read, passthrough ops remain
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        assert not plan_has_operator(
            optimized_plan, Filter
        ), "No Filter operators should remain after pushdown into Read"


class TestPassthroughBehavior:
    """Tests for PASSTHROUGH behavior operators.

    Operators: Sort, Repartition, RandomShuffle, Limit
    Predicates pass through unchanged - operators don't affect filtering.
    """

    @pytest.fixture
    def base_ds(self, ray_start_regular_shared):
        return ray.data.range(100)

    @pytest.mark.parametrize(
        "transform,expected_op_type",
        [
            (lambda ds: ds.sort("id"), "Sort"),
            (lambda ds: ds.repartition(10), "Repartition"),
            (lambda ds: ds.random_shuffle(), "RandomShuffle"),
            (lambda ds: ds.limit(50), "Limit"),
        ],
        ids=["sort", "repartition", "random_shuffle", "limit"],
    )
    def test_filter_pushes_through_operator(self, base_ds, transform, expected_op_type):
        """Filter should push through passthrough operators."""
        ds = transform(base_ds).filter(expr=col("id") < 10)

        # Verify correctness against expected result
        expected = base_ds.filter(expr=col("id") < 10)
        assert rows_same(ds.to_pandas(), expected.to_pandas())

        # Filter pushed down, operator remains
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        assert plan_has_operator(
            optimized_plan, Filter
        ), "Filter should exist after pushdown"

        # Verify the passthrough operator is still present
        op_types = get_operator_types(optimized_plan)
        assert expected_op_type in op_types, f"{expected_op_type} should remain in plan"

    def test_filter_pushes_through_multiple_ops(self, base_ds):
        """Filter should push through multiple passthrough operators."""
        ds = base_ds.sort("id").repartition(5).limit(50).filter(expr=col("id") < 10)

        # Verify correctness against expected result
        expected = base_ds.filter(expr=col("id") < 10)
        assert rows_same(ds.to_pandas(), expected.to_pandas())

        # Verify plan: filter pushed down, all operators remain
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        assert plan_has_operator(optimized_plan, Filter), "Filter should exist"
        assert plan_has_operator(optimized_plan, Sort), "Sort should remain"
        assert plan_has_operator(
            optimized_plan, Repartition
        ), "Repartition should remain"
        assert plan_has_operator(optimized_plan, Limit), "Limit should remain"

    def test_multiple_filters_fuse_and_push_through(self, base_ds):
        """Multiple filters should fuse and push through passthrough operators."""
        ds = base_ds.filter(expr=col("id") > 5).sort("id").filter(expr=col("id") < 20)

        # Verify correctness against expected result
        expected = base_ds.filter(expr=(col("id") > 5) & (col("id") < 20))
        assert rows_same(ds.to_pandas(), expected.to_pandas())

        # Verify plan: filters fused and pushed, Sort remains
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        filters = get_operators_of_type(optimized_plan, Filter)
        assert len(filters) == 1, "Multiple filters should be fused into one"
        assert plan_has_operator(optimized_plan, Sort), "Sort should remain"
        assert plan_operator_comes_before(
            optimized_plan, Filter, Sort
        ), "Fused filter should come before Sort"


class TestPassthroughWithSubstitutionBehavior:
    """Tests for PASSTHROUGH_WITH_SUBSTITUTION behavior operators.

    Operator: Project (used by rename_columns, select, with_column)
    Predicates push through but column names must be rebound.
    """

    @pytest.fixture
    def parquet_ds(self, ray_start_regular_shared):
        return ray.data.read_parquet("example://iris.parquet")

    def test_simple_rename_with_filter(self, parquet_ds):
        """Filter after rename should rebind columns and push down."""
        ds = parquet_ds.rename_columns({"sepal.length": "len"}).filter(
            expr=col("len") > 5.0
        )

        # Verify correctness against expected result
        expected = parquet_ds.filter(expr=col("sepal.length") > 5.0).rename_columns(
            {"sepal.length": "len"}
        )
        assert rows_same(ds.to_pandas(), expected.to_pandas())

        # Filter rebound and pushed to Read (no Filter operators should remain)
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        assert not plan_has_operator(
            optimized_plan, Filter
        ), "Filter should be pushed into Read, no Filter operators should remain"

    def test_chained_renames_with_filter(self, parquet_ds):
        """Multiple renames should track through filter pushdown."""
        ds = (
            parquet_ds.rename_columns({"sepal.length": "a"})
            .rename_columns({"a": "b"})
            .filter(expr=col("b") > 5.0)
        )

        # Verify correctness against expected result
        expected = (
            parquet_ds.filter(expr=col("sepal.length") > 5.0)
            .rename_columns({"sepal.length": "a"})
            .rename_columns({"a": "b"})
        )
        assert rows_same(ds.to_pandas(), expected.to_pandas())

        # Filter should be pushed into Read after column rebinding
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        assert not plan_has_operator(
            optimized_plan, Filter
        ), "Filter should be pushed into Read after rebinding through renames"

    def test_multiple_filters_with_renames(self, parquet_ds):
        """Multiple filters with renames should all rebind and push."""
        ds = (
            parquet_ds.rename_columns({"sepal.length": "a"})
            .filter(expr=col("a") > 2.0)
            .rename_columns({"a": "b"})
            .filter(expr=col("b") < 5.0)
        )

        # Verify correctness against expected result
        expected = (
            parquet_ds.filter(
                expr=(col("sepal.length") > 2.0) & (col("sepal.length") < 5.0)
            )
            .rename_columns({"sepal.length": "a"})
            .rename_columns({"a": "b"})
        )
        assert rows_same(ds.to_pandas(), expected.to_pandas())

        # Multiple filters should be fused, rebound, and pushed into Read
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        assert not plan_has_operator(
            optimized_plan, Filter
        ), "All filters should be fused, rebound, and pushed into Read"


class TestProjectionWithFilterEdgeCases:
    """Tests for edge cases with select_columns and with_column followed by filters.

    These tests verify that filters correctly handle:
    - Columns that are kept by select (should push through)
    - Columns that are removed by select (should NOT push through)
    - Computed columns from with_column (should NOT push through)
    """

    @pytest.fixture
    def base_ds(self, ray_start_regular_shared):
        return ray.data.from_items(
            [
                {"a": 1, "b": 2, "c": 3},
                {"a": 2, "b": 5, "c": 8},
                {"a": 3, "b": 6, "c": 9},
            ]
        )

    def test_select_then_filter_on_selected_column(self, base_ds):
        """Filter on selected column should push through select."""
        ds = base_ds.select_columns(["a", "b"]).filter(expr=col("a") > 1)

        # Verify correctness
        result_df = ds.to_pandas()
        expected_df = pd.DataFrame(
            [
                {"a": 2, "b": 5},
                {"a": 3, "b": 6},
            ]
        )
        # Sort columns before comparison
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]
        assert rows_same(result_df, expected_df)

        # Verify plan: filter pushed through select
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        assert plan_operator_comes_before(
            optimized_plan, Filter, Project
        ), "Filter should be pushed before Project"

    def test_select_then_filter_on_removed_column(self, base_ds):
        """Filter on removed column should fail, not push through."""
        ds = base_ds.select_columns(["a"])

        with pytest.raises((KeyError, ray.exceptions.RayTaskError)):
            ds.filter(expr=col("b") == 2).take_all()

    def test_with_column_then_filter_on_computed_column(self, base_ds):
        """Filter on computed column should not push through."""

        from ray.data.expressions import lit

        ds = base_ds.with_column("d", lit(4)).filter(expr=col("d") == 4)

        # Verify correctness - all rows should pass (d is always 4)
        result_df = ds.to_pandas()
        expected_df = pd.DataFrame(
            [
                {"a": 1, "b": 2, "c": 3, "d": 4},
                {"a": 2, "b": 5, "c": 8, "d": 4},
                {"a": 3, "b": 6, "c": 9, "d": 4},
            ]
        )
        # Sort columns before comparison
        result_df = result_df[sorted(result_df.columns)]
        expected_df = expected_df[sorted(expected_df.columns)]
        assert rows_same(result_df, expected_df)

        # Verify plan: filter should NOT push through (stays after with_column)
        optimized_plan = LogicalOptimizer().optimize(ds._plan._logical_plan)
        assert plan_has_operator(
            optimized_plan, Filter
        ), "Filter should remain (not pushed through)"

    def test_rename_then_filter_on_old_column_name(self, base_ds):
        """Filter using old column name after rename should fail."""
        ds = base_ds.rename_columns({"b": "B"})

        with pytest.raises((KeyError, ray.exceptions.RayTaskError)):
            ds.filter(expr=col("b") == 2).take_all()

    @pytest.mark.parametrize(
        "ds_factory,rename_map,filter_col,filter_value,expected_rows",
        [
            # In-memory dataset: rename a->b, b->b_old
            (
                lambda: ray.data.from_items(
                    [
                        {"a": 1, "b": 2, "c": 3},
                        {"a": 2, "b": 5, "c": 8},
                        {"a": 3, "b": 6, "c": 9},
                    ]
                ),
                {"a": "b", "b": "b_old"},
                "b",
                1,
                [{"b": 2, "b_old": 5, "c": 8}, {"b": 3, "b_old": 6, "c": 9}],
            ),
            # Parquet dataset: rename sepal.length->sepal.width, sepal.width->old_width
            (
                lambda: ray.data.read_parquet("example://iris.parquet"),
                {"sepal.length": "sepal.width", "sepal.width": "old_width"},
                "sepal.width",
                5.0,
                None,  # Will verify via alternative computation
            ),
        ],
        ids=["in_memory", "parquet"],
    )
    def test_rename_chain_with_name_reuse(
        self,
        ray_start_regular_shared,
        ds_factory,
        rename_map,
        filter_col,
        filter_value,
        expected_rows,
    ):
        """Test rename chains where an output name matches another rename's input name.

        This tests the fix for a bug where rename(a->b, b->c) followed by filter(b>5)
        would incorrectly block pushdown, even though 'b' is a valid output column
        (created by a->b).

        Example: rename({'a': 'b', 'b': 'temp'}) creates 'b' from 'a' and 'temp' from 'b'.
        A filter on 'b' should be able to push through.
        """
        ds = ds_factory()

        # Apply rename and filter
        ds_renamed_filtered = ds.rename_columns(rename_map).filter(
            expr=col(filter_col) > filter_value
        )

        # Verify correctness
        if expected_rows is not None:
            # For in-memory, compare against expected rows
            result_df = ds_renamed_filtered.to_pandas()
            expected_df = pd.DataFrame(expected_rows)
            result_df = result_df[sorted(result_df.columns)]
            expected_df = expected_df[sorted(expected_df.columns)]
            assert rows_same(result_df, expected_df)
        else:
            # For parquet, compare against alternative computation
            # Filter on original column, then rename
            original_col = next(k for k, v in rename_map.items() if v == filter_col)
            expected = ds.filter(expr=col(original_col) > filter_value).rename_columns(
                rename_map
            )
            assert rows_same(ds_renamed_filtered.to_pandas(), expected.to_pandas())

        # Verify plan optimization
        optimized_plan = LogicalOptimizer().optimize(
            ds_renamed_filtered._plan._logical_plan
        )

        # Determine if the data source supports predicate pushdown by checking
        # if the filter was completely eliminated (pushed into the read operator)
        has_filter = plan_has_operator(optimized_plan, Filter)
        has_project = plan_has_operator(optimized_plan, Project)

        # For file-based reads that support predicate pushdown (e.g., parquet),
        # the filter should be completely pushed into the read operator.
        # We detect this by checking if the filter is gone after optimization.
        if not has_filter and not has_project:
            # Filter was pushed into Read - this is the optimal case
            pass  # Test passes
        elif has_filter and has_project:
            # For in-memory datasets, filter should at least push through projection
            assert plan_operator_comes_before(
                optimized_plan, Filter, Project
            ), "Filter should be pushed before Project after rebinding through rename chain"
        else:
            # Unexpected state - either filter or project but not both
            raise AssertionError(
                f"Unexpected optimization state: has_filter={has_filter}, has_project={has_project}"
            )


class TestPushIntoBranchesBehavior:
    """Tests for PUSH_INTO_BRANCHES behavior operators.

    Operator: Union
    Predicates are duplicated and pushed into each branch.
    """

    def test_simple_union_with_filter(self, ray_start_regular_shared):
        """Filter after union should push into both branches."""
        ds1 = ray.data.range(100, parallelism=2)
        ds2 = ray.data.range(100, parallelism=2)
        ds = ds1.union(ds2).filter(expr=col("id") >= 50)

        # Verify correctness: should have duplicates from both branches
        base = ray.data.range(100)
        expected = base.filter(expr=col("id") >= 50).union(
            base.filter(expr=col("id") >= 50)
        )
        assert rows_same(ds.to_pandas(), expected.to_pandas())

    def test_multiple_unions_with_filter(self, ray_start_regular_shared):
        """Filter should push into all branches of multiple unions."""
        ds1 = ray.data.read_parquet("example://iris.parquet")
        ds2 = ray.data.read_parquet("example://iris.parquet")
        ds3 = ray.data.read_parquet("example://iris.parquet")
        ds = ds1.union(ds2).union(ds3).filter(expr=col("sepal.length") > 5.0)

        # Verify correctness: should have 3x the filtered results
        single_filtered = ray.data.read_parquet("example://iris.parquet").filter(
            expr=col("sepal.length") > 5.0
        )
        expected = single_filtered.union(single_filtered).union(single_filtered)
        assert rows_same(ds.to_pandas(), expected.to_pandas())

    def test_branch_filters_plus_union_filter(self, ray_start_regular_shared):
        """Individual branch filters plus union filter should all push."""
        parquet_ds = ray.data.read_parquet("example://iris.parquet")
        ds1 = parquet_ds.filter(expr=col("sepal.width") > 2.0)
        ds2 = parquet_ds.filter(expr=col("sepal.width") > 2.0)
        ds = ds1.union(ds2).filter(expr=col("sepal.length") < 5.0)

        # Verify correctness: both filters applied
        expected_single = parquet_ds.filter(
            expr=(col("sepal.width") > 2.0) & (col("sepal.length") < 5.0)
        )
        expected = expected_single.union(expected_single)
        assert rows_same(ds.to_pandas(), expected.to_pandas())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
