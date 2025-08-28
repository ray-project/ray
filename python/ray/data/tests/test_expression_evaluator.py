import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._expression_evaluator import eval_expr
from ray.data._internal.planner.plan_expression.expression_evaluator import (
    ExpressionEvaluator,
)
from ray.data.expressions import case, col, lit, when


@pytest.fixture(scope="module")
def sample_data(tmpdir_factory):
    """Create and yield sample Parquet data for testing.

    This fixture creates sample data with various data types and edge cases
    to test expression evaluation functionality.

    Args:
        tmpdir_factory: Pytest fixture for creating temporary directories

    Yields:
        Tuple of (parquet_file_path, schema) for testing
    """
    # Sample data for testing purposes
    data = {
        "age": [
            25,
            32,
            45,
            29,
            40,
            np.nan,
        ],  # List of ages, including a None value for testing
        "city": [
            "New York",
            "San Francisco",
            "Los Angeles",
            "Los Angeles",
            "San Francisco",
            "San Jose",
        ],
        "is_student": [False, True, False, False, True, None],  # Including a None value
    }

    # Define the schema explicitly
    schema = pa.schema(
        [("age", pa.float64()), ("city", pa.string()), ("is_student", pa.bool_())]
    )

    # Create a PyArrow table from the sample data
    table = pa.table(data, schema=schema)

    # Use tmpdir_factory to create a temporary directory
    temp_dir = tmpdir_factory.mktemp("data")
    parquet_file = temp_dir.join("sample_data.parquet")

    # Write the table to a Parquet file in the temporary directory
    pq.write_table(table, str(parquet_file))

    # Yield the path to the Parquet file for testing
    yield str(parquet_file), schema


expressions_and_expected_data = [
    # Parameterized test cases with expressions and their expected results
    # Comparison Ops
    (
        "40 > age",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "40 >= age",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "30 < age",
        [
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age >= 30",
        [
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age == 40",
        [{"age": 40, "city": "San Francisco", "is_student": True}],
    ),
    (
        "is_student != True",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "age < 0",
        [],
    ),
    (
        "is_student == True",
        [
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "is_student == False",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Op 'in'
    (
        "city in ['Los Angeles', 'New York']",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "city in ['Los Angeles']",
        [
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "city in ['New York', 'San Francisco']",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age in []",
        [],
    ),
    (
        "age in [25, 32, 45, 29, 40]",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age in [25, 32, None]",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
        ],
    ),
    # Op 'not in'
    (
        "is_student not in [None]",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "is_student not in [True, None]",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Logical Ops 'and'
    (
        "age > 30 and is_student == True",
        [
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "city == 'Los Angeles' and age < 40",
        [
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    (
        "age < 40 and city in ['New York', 'Los Angeles']",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Logical Ops 'or'
    (
        "age < 30 or is_student == True",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "city == 'New York' or city == 'San Francisco'",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "age < 30 or age > 40",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Logical Ops combination 'and' and 'or'
    (
        "(age < 30 or age > 40) and is_student != True",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
        ],
    ),
    # Op 'is_null'
    (
        "is_null(is_student)",
        [
            {"age": None, "city": "San Jose", "is_student": None},
        ],
    ),
    (
        "age < 40 or is_null(is_student)",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": None, "city": "San Jose", "is_student": None},
        ],
    ),
    # Op 'is_nan'
    (
        "is_nan(age)",
        [
            {"age": None, "city": "San Jose", "is_student": None},
        ],
    ),
    (
        "city in ['San Jose', 'Los Angeles'] and is_nan(age)",
        [
            {"age": None, "city": "San Jose", "is_student": None},
        ],
    ),
    # Op 'is_valid'
    (
        "is_valid(is_student)",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
    (
        "is_valid(is_student) and is_valid(age)",
        [
            {"age": 25, "city": "New York", "is_student": False},
            {"age": 32, "city": "San Francisco", "is_student": True},
            {"age": 45, "city": "Los Angeles", "is_student": False},
            {"age": 29, "city": "Los Angeles", "is_student": False},
            {"age": 40, "city": "San Francisco", "is_student": True},
        ],
    ),
]


@pytest.mark.parametrize("expression, expected_data", expressions_and_expected_data)
def test_filter(sample_data, expression, expected_data):
    """Test the filter functionality of the ExpressionEvaluator."""

    # Instantiate the ExpressionEvaluator with valid column names
    sample_data_path, _ = sample_data
    filters = ExpressionEvaluator.get_filters(expression=expression)

    # Read the table from the Parquet file with the applied filters
    filtered_table = pq.read_table(sample_data_path, filters=filters)

    # Convert the filtered table back to a list of dictionaries for comparison
    result = filtered_table.to_pandas().to_dict(orient="records")

    def convert_nan_to_none(data):
        return [
            {k: (None if pd.isna(v) else v) for k, v in record.items()}
            for record in data
        ]

    # Convert NaN to None for comparison
    result_converted = convert_nan_to_none(result)

    assert result_converted == expected_data


def test_filter_equal_negative_number():
    df = pd.DataFrame.from_dict(
        {"A": [-1, -1, 1, 2, -1, 3, 4, 5], "B": [-1, -1, 1, 2, -1, 3, 4, 5]}
    )
    expression = ExpressionEvaluator.get_filters(expression="A == -1")
    result = pa.table(df).filter(expression)
    result_df = result.to_pandas().to_dict(orient="records")
    expected = df[df["A"] == -1].to_dict(orient="records")
    assert result_df == expected


def test_filter_bad_expression(sample_data):
    with pytest.raises(ValueError, match="Invalid syntax in the expression"):
        ExpressionEvaluator.get_filters(expression="bad filter")

    filters = ExpressionEvaluator.get_filters(expression="hi > 3")

    sample_data_path, _ = sample_data
    with pytest.raises(pa.ArrowInvalid):
        pq.read_table(sample_data_path, filters=filters)


def test_case_expression_evaluation():
    """Test that case expressions are properly evaluated.

    This test verifies that case expressions can be evaluated correctly
    against pandas DataFrames and produce the expected results.
    """
    # Create test data
    data = {"age": [25, 35, 55, 15], "city": ["NYC", "LA", "SF", "CHI"]}
    df = pd.DataFrame(data)

    # Test simple case statement
    expr = case(
        [(col("age") > 50, lit("Elder")), (col("age") > 30, lit("Adult"))],
        default=lit("Young"),
    )
    result = eval_expr(expr, df)

    # Expected results: [Young, Adult, Elder, Young]
    expected = ["Young", "Adult", "Elder", "Young"]
    assert list(result) == expected

    # Test case with boolean conditions
    expr = case(
        [(col("age") > 40, lit("Over 40")), (col("age") > 20, lit("20-40"))],
        default=lit("Under 20"),
    )
    result = eval_expr(expr, df)

    # Expected results: [20-40, 20-40, Over 40, Under 20]
    expected = ["20-40", "20-40", "Over 40", "Under 20"]
    assert list(result) == expected


def test_case_expression_with_complex_conditions():
    """Test case expressions with more complex boolean conditions."""
    data = {"age": [25, 35, 55, 15], "is_student": [True, False, False, True]}
    df = pd.DataFrame(data)

    # Test case with compound conditions
    expr = case(
        [
            (
                (col("age") > 50) & (~col("is_student")),
                lit("Elder Non-Student"),
            ),
            (col("age") > 30, lit("Adult")),
            (col("is_student"), lit("Student")),
        ],
        default=lit("Other"),
    )

    result = eval_expr(expr, df)

    # Expected results: [Student, Adult, Elder Non-Student, Student]
    expected = ["Student", "Adult", "Elder Non-Student", "Student"]
    assert list(result) == expected


def test_when_method_chaining_evaluation():
    """Test that when() method chaining produces the same results as case()."""
    data = {"age": [25, 35, 55, 15], "income": [30000, 60000, 120000, 10000]}
    df = pd.DataFrame(data)

    # Create equivalent expressions using both APIs
    case_expr = case(
        [(col("age") > 50, lit("Elder")), (col("age") > 30, lit("Adult"))],
        default=lit("Young"),
    )

    when_expr = (
        when(col("age") > 50, lit("Elder"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )

    # Both should produce identical results
    case_result = eval_expr(case_expr, df)
    when_result = eval_expr(when_expr, df)

    assert list(case_result) == list(when_result)
    expected = ["Young", "Adult", "Elder", "Young"]
    assert list(case_result) == expected


def test_case_expression_with_numeric_values():
    """Test case expressions that return numeric values."""
    data = {"score": [85, 92, 78, 95, 88]}
    df = pd.DataFrame(data)

    # Test case with numeric outputs
    expr = case(
        [
            (col("score") >= 95, lit(100)),
            (col("score") >= 90, lit(95)),
            (col("score") >= 85, lit(90)),
            (col("score") >= 80, lit(85)),
        ],
        default=lit(80),
    )

    result = eval_expr(expr, df)

    # Expected results: [90, 95, 80, 100, 90]
    expected = [90, 95, 80, 100, 90]
    assert list(result) == expected


def test_case_expression_with_boolean_conditions():
    """Test case expressions with boolean column conditions."""
    data = {
        "is_active": [True, False, True, False],
        "is_premium": [False, True, True, False],
    }
    df = pd.DataFrame(data)

    # Test case with boolean conditions
    expr = case(
        [
            (col("is_active") & col("is_premium"), lit("Active Premium")),
            (col("is_active"), lit("Active")),
            (col("is_premium"), lit("Premium")),
        ],
        default=lit("Inactive"),
    )

    result = eval_expr(expr, df)

    # Expected results: [Active, Premium, Active Premium, Inactive]
    expected = ["Active", "Premium", "Active Premium", "Inactive"]
    assert list(result) == expected


def test_case_expression_evaluation_order():
    """Test that case expressions evaluate conditions in the correct order."""
    data = {"age": [25, 35, 55, 15]}
    df = pd.DataFrame(data)

    # Test that first matching condition is used
    expr = case(
        [
            (
                col("age") > 20,
                lit("Over 20"),
            ),  # This will match first for age 25, 35, 55
            (
                col("age") > 30,
                lit("Over 30"),
            ),  # This would match for 35, 55 but won't be reached
            (
                col("age") > 50,
                lit("Over 50"),
            ),  # This would match for 55 but won't be reached
        ],
        default=lit("Under 20"),
    )

    result = eval_expr(expr, df)

    # Expected results: [Over 20, Over 20, Over 20, Under 20]
    # Note: age 35 and 55 match the first condition (> 20) before reaching > 30 or > 50
    expected = ["Over 20", "Over 20", "Over 20", "Under 20"]
    assert list(result) == expected


def test_case_expression_with_none_values():
    """Test case expressions that handle None/NaN values."""
    data = {"status": ["active", None, "pending", "inactive"]}
    df = pd.DataFrame(data)

    # Test case with None handling
    expr = case(
        [
            (col("status") == "active", lit("Active")),
            (col("status") == "pending", lit("Pending")),
            (col("status") == "inactive", lit("Inactive")),
        ],
        default=lit("Unknown"),
    )

    result = eval_expr(expr, df)

    # Expected results: [Active, Unknown, Pending, Inactive]
    expected = ["Active", "Unknown", "Pending", "Inactive"]
    assert list(result) == expected


def test_case_expression_performance_with_large_data():
    """Test case expression performance with larger datasets."""
    # Create larger test data
    data = {
        "category": ["A"] * 100 + ["B"] * 100 + ["C"] * 100,
        "value": list(range(300)),
    }
    df = pd.DataFrame(data)

    # Test case with multiple conditions
    expr = case(
        [
            (col("category") == "A", lit("Alpha")),
            (col("category") == "B", lit("Beta")),
            (col("category") == "C", lit("Gamma")),
        ],
        default=lit("Unknown"),
    )

    result = eval_expr(expr, df)

    # Verify results
    assert len(result) == 300
    assert result.iloc[0] == "Alpha"  # First 100 should be Alpha
    assert result.iloc[100] == "Beta"  # Next 100 should be Beta
    assert result.iloc[200] == "Gamma"  # Last 100 should be Gamma


def test_case_expression_edge_cases():
    """Test edge cases and boundary conditions for case expressions."""
    # Test with empty DataFrame
    empty_df = pd.DataFrame({"col": []})
    expr = case([(col("col") > 0, lit("Positive"))], default=lit("Zero"))
    result = eval_expr(expr, empty_df)
    assert len(result) == 0

    # Test with single row
    single_df = pd.DataFrame({"age": [25]})
    expr = case([(col("age") > 30, lit("Adult"))], default=lit("Young"))
    result = eval_expr(expr, single_df)
    assert list(result) == ["Young"]

    # Test with all conditions false
    data = {"age": [15, 20, 25]}
    df = pd.DataFrame(data)
    expr = case([(col("age") > 30, lit("Adult"))], default=lit("Young"))
    result = eval_expr(expr, df)
    assert all(r == "Young" for r in result)


def test_when_expression_error_handling():
    """Test error handling for incomplete when expressions."""
    # Test that WhenExpr cannot be evaluated directly
    data = {"age": [25, 35]}
    df = pd.DataFrame(data)

    # Create incomplete when expression
    incomplete_expr = when(col("age") > 30, lit("Adult"))

    # This should raise an error because .otherwise() was not called
    with pytest.raises(TypeError, match="WhenExpr cannot be evaluated directly"):
        eval_expr(incomplete_expr, df)


def test_case_expression_with_mixed_data_types():
    """Test case expressions that return mixed data types."""
    data = {"category": ["A", "B", "C", "D"]}
    df = pd.DataFrame(data)

    # Test case with mixed return types
    expr = case(
        [
            (col("category") == "A", lit(100)),  # Integer
            (col("category") == "B", lit("High")),  # String
            (col("category") == "C", lit(True)),  # Boolean
            (col("category") == "D", lit(None)),  # None
        ],
        default=lit("Unknown"),
    )

    result = eval_expr(expr, df)

    # Expected results: [100, "High", True, None]
    expected = [100, "High", True, None]
    assert list(result) == expected


def test_case_expression_complex_nested_conditions():
    """Test case expressions with deeply nested boolean logic."""
    data = {
        "age": [25, 35, 55, 15],
        "income": [30000, 60000, 120000, 10000],
        "is_student": [True, False, False, True],
        "has_credit": [False, True, True, False],
    }
    df = pd.DataFrame(data)

    # Test complex nested conditions
    expr = case(
        [
            (
                ((col("age") > 50) & (col("income") > 100000))
                | (col("is_student") & col("has_credit")),
                lit("Premium"),
            ),
            (
                ((col("age") > 30) & (col("income") > 50000)) | col("is_student"),
                lit("Standard"),
            ),
            (col("age") > 18, lit("Basic")),
        ],
        default=lit("Limited"),
    )

    result = eval_expr(expr, df)

    # Expected results based on complex logic
    # Age 25, student=True, has_credit=False: Standard (student=True)
    # Age 35, income=60000: Standard (age>30 & income>50000)
    # Age 55, income=120000: Premium (age>50 & income>100000)
    # Age 15: Limited (age<=18)
    expected = ["Standard", "Standard", "Premium", "Limited"]
    assert list(result) == expected
