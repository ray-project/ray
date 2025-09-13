import pandas as pd
import pyarrow as pa
import pytest

from ray.data._expression_evaluator import eval_expr
from ray.data.expressions import col, lit, when


def test_case_expression_pandas_evaluation():
    """Test case expression evaluation with pandas DataFrames."""
    # Create test data
    data = {"age": [25, 35, 55, 15], "score": [85, 92, 78, 95]}
    df = pd.DataFrame(data)

    # Test simple case statement
    expr = (
        when(col("age") > 50, lit("Elder"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )
    result = eval_expr(expr, df)

    # Expected results: [Young, Adult, Elder, Young]
    expected = ["Young", "Adult", "Elder", "Young"]
    assert list(result) == expected

    # Test case with numeric outputs
    expr = (
        when(col("score") >= 90, lit(1))
        .when(col("score") >= 80, lit(2))
        .otherwise(lit(3))
    )
    result = eval_expr(expr, df)
    expected = [2, 1, 3, 1]  # Based on scores [85, 92, 78, 95]
    assert list(result) == expected


def test_case_expression_arrow_evaluation():
    """Test case expression evaluation with PyArrow Tables."""
    # Create test data
    data = {"age": pa.array([25, 35, 55, 15]), "score": pa.array([85, 92, 78, 95])}
    table = pa.table(data)

    # Test simple case statement
    expr = (
        when(col("age") > 50, lit("Elder"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )
    result = eval_expr(expr, table)

    # Expected results: [Young, Adult, Elder, Young]
    expected = ["Young", "Adult", "Elder", "Young"]
    assert result.to_pylist() == expected

    # Test case with numeric outputs
    expr = (
        when(col("score") >= 90, lit(1))
        .when(col("score") >= 80, lit(2))
        .otherwise(lit(3))
    )
    result = eval_expr(expr, table)
    expected = [2, 1, 3, 1]  # Based on scores [85, 92, 78, 95]
    assert result.to_pylist() == expected


def test_case_expression_edge_cases():
    """Test edge cases for case expression evaluation."""
    # Test with single row
    single_df = pd.DataFrame({"age": [25]})
    expr = when(col("age") > 30, lit("Adult")).otherwise(lit("Young"))
    result = eval_expr(expr, single_df)
    assert list(result) == ["Young"]

    # Test with empty DataFrame
    empty_df = pd.DataFrame({"age": []})
    expr = when(col("age") > 30, lit("Adult")).otherwise(lit("Young"))
    result = eval_expr(expr, empty_df)
    assert len(result) == 0

    # Test with all conditions false
    data = {"age": [15, 20, 25]}
    df = pd.DataFrame(data)
    expr = when(col("age") > 30, lit("Adult")).otherwise(lit("Young"))
    result = eval_expr(expr, df)
    assert all(r == "Young" for r in result)


def test_when_expression_direct_evaluation_error():
    """Test that WhenExpr cannot be evaluated directly."""
    from ray.data.expressions import WhenExpr

    # Create test data
    df = pd.DataFrame({"age": [25]})

    # Create a WhenExpr (not completed with .otherwise())
    when_expr = when(col("age") > 30, lit("Adult"))
    assert isinstance(when_expr, WhenExpr)

    # Test that WhenExpr cannot be evaluated directly
    with pytest.raises(TypeError, match="WhenExpr cannot be evaluated directly"):
        eval_expr(when_expr, df)


def test_case_expression_complex_conditions():
    """Test case expressions with complex boolean conditions."""
    # Create test data with multiple columns
    data = {
        "age": [25, 35, 55, 15],
        "income": [40000, 60000, 120000, 20000],
        "is_student": [True, False, False, True],
    }
    df = pd.DataFrame(data)

    # Test complex boolean conditions
    expr = (
        when((col("age") > 50) & (col("income") > 100000), lit("Rich Elder"))
        .when(col("age") > 30, lit("Adult"))
        .when(col("is_student"), lit("Student"))
        .otherwise(lit("Other"))
    )

    result = eval_expr(expr, df)
    # Expected: [Student, Adult, Rich Elder, Student]
    expected = ["Student", "Adult", "Rich Elder", "Student"]
    assert list(result) == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
