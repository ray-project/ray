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


def test_new_operators_pandas():
    """Test new operators with pandas DataFrames."""
    # Create test data
    data = {
        "a": [10, 7, 15],
        "b": [3, 2, 4],
        "status": ["active", "pending", "inactive"],
    }
    df = pd.DataFrame(data)

    # Test != operator
    expr = col("a") != 10
    result = eval_expr(expr, df)
    expected = [False, True, True]
    assert list(result) == expected

    # Test // operator (floor division)
    expr = col("a") // col("b")
    result = eval_expr(expr, df)
    expected = [3, 3, 3]  # 10//3=3, 7//2=3, 15//4=3
    assert list(result) == expected

    # Test is_null()
    data_with_nulls = {"a": [10, None, 15], "b": [1, 2, 3]}
    df_nulls = pd.DataFrame(data_with_nulls)
    expr = col("a").is_null()
    result = eval_expr(expr, df_nulls)
    expected = [False, True, False]
    assert list(result) == expected

    # Test is_not_null()
    expr = col("a").is_not_null()
    result = eval_expr(expr, df_nulls)
    expected = [True, False, True]
    assert list(result) == expected

    # Test is_in()
    expr = col("status").is_in(["active", "approved"])
    result = eval_expr(expr, df)
    expected = [True, False, False]
    assert list(result) == expected

    # Test not_in()
    expr = col("status").not_in(["inactive", "rejected"])
    result = eval_expr(expr, df)
    expected = [True, True, False]
    assert list(result) == expected


def test_new_operators_arrow():
    """Test new operators with PyArrow Tables."""
    # Create test data
    data = {
        "a": pa.array([10, 7, 15]),
        "b": pa.array([3, 2, 4]),
        "status": pa.array(["active", "pending", "inactive"]),
    }
    table = pa.table(data)

    # Test != operator
    expr = col("a") != 10
    result = eval_expr(expr, table)
    expected = [False, True, True]
    assert result.to_pylist() == expected

    # Test // operator (floor division)
    expr = col("a") // col("b")
    result = eval_expr(expr, table)
    expected = [3, 3, 3]  # 10//3=3, 7//2=3, 15//4=3
    assert result.to_pylist() == expected

    # Test is_null()
    data_with_nulls = {"a": pa.array([10, None, 15]), "b": pa.array([1, 2, 3])}
    table_nulls = pa.table(data_with_nulls)
    expr = col("a").is_null()
    result = eval_expr(expr, table_nulls)
    expected = [False, True, False]
    assert result.to_pylist() == expected

    # Test is_not_null()
    expr = col("a").is_not_null()
    result = eval_expr(expr, table_nulls)
    expected = [True, False, True]
    assert result.to_pylist() == expected

    # Test is_in()
    expr = col("status").is_in(["active", "approved"])
    result = eval_expr(expr, table)
    expected = [True, False, False]
    assert result.to_pylist() == expected

    # Test not_in()
    expr = col("status").not_in(["inactive", "rejected"])
    result = eval_expr(expr, table)
    expected = [True, True, False]
    assert result.to_pylist() == expected


def test_string_concatenation_arrow():
    """Test string concatenation with ADD operator for PyArrow."""
    data = {
        "first_name": pa.array(["John", "Jane", "Bob"]),
        "last_name": pa.array(["Doe", "Smith", "Johnson"]),
    }
    table = pa.table(data)

    # Test string concatenation
    expr = col("first_name") + lit(" ") + col("last_name")
    result = eval_expr(expr, table)
    expected = ["John Doe", "Jane Smith", "Bob Johnson"]
    assert result.to_pylist() == expected


def test_boolean_null_handling_arrow():
    """Test AND/OR operations with null values using Kleene logic for PyArrow."""
    data = {
        "a": pa.array([True, True, False, None]),
        "b": pa.array([True, None, False, True]),
    }
    table = pa.table(data)

    # Test AND with nulls (Kleene logic)
    expr = col("a") & col("b")
    result = eval_expr(expr, table)
    # True AND True = True
    # True AND None = None
    # False AND False = False
    # None AND True = None
    assert result.to_pylist() == [True, None, False, None]

    # Test OR with nulls (Kleene logic)
    expr = col("a") | col("b")
    result = eval_expr(expr, table)
    # True OR True = True
    # True OR None = True
    # False OR False = False
    # None OR True = True
    assert result.to_pylist() == [True, True, False, True]


def test_case_with_nested_expressions_pandas():
    """Test nested case expressions with pandas."""
    data = {"score": [95, 82, 70, 88], "extra_credit": [5, 0, 10, 2]}
    df = pd.DataFrame(data)

    # Nested case expression
    expr = (
        when(
            col("score") >= 90,
            when(col("extra_credit") > 0, lit("A+")).otherwise(lit("A")),
        )
        .when(
            col("score") >= 80,
            when(col("extra_credit") >= 5, lit("B+")).otherwise(lit("B")),
        )
        .otherwise(when(col("extra_credit") >= 10, lit("C+")).otherwise(lit("C")))
    )

    result = eval_expr(expr, df)
    expected = ["A+", "B", "C+", "B"]
    assert list(result) == expected


def test_case_with_nested_expressions_arrow():
    """Test nested case expressions with PyArrow."""
    data = {
        "score": pa.array([95, 82, 70, 88]),
        "extra_credit": pa.array([5, 0, 10, 2]),
    }
    table = pa.table(data)

    # Nested case expression
    expr = (
        when(
            col("score") >= 90,
            when(col("extra_credit") > 0, lit("A+")).otherwise(lit("A")),
        )
        .when(
            col("score") >= 80,
            when(col("extra_credit") >= 5, lit("B+")).otherwise(lit("B")),
        )
        .otherwise(when(col("extra_credit") >= 10, lit("C+")).otherwise(lit("C")))
    )

    result = eval_expr(expr, table)
    expected = ["A+", "B", "C+", "B"]
    assert result.to_pylist() == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
