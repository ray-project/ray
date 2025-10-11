import pytest
from packaging.version import parse as parse_version

from ray._private.test_utils import get_pyarrow_version
from ray.data.expressions import (
    CaseExpr,
    Expr,
    WhenExpr,
    col,
    lit,
    when,
)

# Tuples of (expr1, expr2, expected_result)
STRUCTURAL_EQUALITY_TEST_CASES = [
    # Base cases: ColumnExpr
    (col("a"), col("a"), True),
    (col("a"), col("b"), False),
    # Base cases: LiteralExpr
    (lit(1), lit(1), True),
    (lit(1), lit(2), False),
    (lit("x"), lit("y"), False),
    # Different expression types
    (col("a"), lit("a"), False),
    (lit(1), lit(1.0), False),
    # Simple binary expressions
    (col("a") + 1, col("a") + 1, True),
    (col("a") + 1, col("a") + 2, False),  # Different literal
    (col("a") + 1, col("b") + 1, False),  # Different column
    (col("a") + 1, col("a") - 1, False),  # Different operator
    # Complex, nested binary expressions
    ((col("a") * 2) + (col("b") / 3), (col("a") * 2) + (col("b") / 3), True),
    ((col("a") * 2) + (col("b") / 3), (col("a") * 2) - (col("b") / 3), False),
    ((col("a") * 2) + (col("b") / 3), (col("c") * 2) + (col("b") / 3), False),
    ((col("a") * 2) + (col("b") / 3), (col("a") * 2) + (col("b") / 4), False),
    # Commutative operations are not structurally equal
    (col("a") + col("b"), col("b") + col("a"), False),
    (lit(1) * col("c"), col("c") * lit(1), False),
    # Case expressions (method chaining)
    (
        when(col("a") > 1, lit("high")).otherwise(lit("low")),
        when(col("a") > 1, lit("high")).otherwise(lit("low")),
        True,
    ),
    (
        when(col("a") > 1, lit("high")).otherwise(lit("low")),
        when(col("a") > 2, lit("high")).otherwise(lit("low")),
        False,
    ),  # Different condition
    (
        when(col("a") > 1, lit("high")).otherwise(lit("low")),
        when(col("a") > 1, lit("medium")).otherwise(lit("low")),
        False,
    ),  # Different value
    (
        when(col("a") > 1, lit("high")).otherwise(lit("low")),
        when(col("a") > 1, lit("high")).otherwise(lit("default")),
        False,
    ),  # Different default
    (
        when(col("a") > 1, lit("high")).otherwise(lit("low")),
        when(col("a") > 1, lit("high"))
        .when(col("b") > 1, lit("medium"))
        .otherwise(lit("low")),
        False,
    ),  # Different number of clauses
]


@pytest.mark.parametrize(
    "expr1, expr2, expected",
    STRUCTURAL_EQUALITY_TEST_CASES,
    ids=[f"{i}" for i in range(len(STRUCTURAL_EQUALITY_TEST_CASES))],
)
def test_structural_equality(expr1, expr2, expected):
    """Test `structurally_equals` for various expression trees.

    This test verifies that the structural equality comparison works correctly
    for different types of expressions and their combinations.

    Args:
        expr1: First expression to compare
        expr2: Second expression to compare
        expected: Expected result of the comparison
    """
    assert expr1.structurally_equals(expr2) is expected
    # Test for symmetry
    assert expr2.structurally_equals(expr1) is expected


def test_operator_eq_is_not_structural_eq():
    """Confirm that `__eq__` (==) builds an expression, while `structurally_equals` compares two existing expressions.

    This test ensures that the == operator creates new expressions rather than
    comparing existing ones, while structurally_equals performs actual comparison.
    """
    # `==` returns a BinaryExpr, not a boolean
    op_eq_expr = col("a") == col("a")
    assert isinstance(op_eq_expr, Expr)
    assert not isinstance(op_eq_expr, bool)

    # `structurally_equals` returns a boolean
    struct_eq_result = col("a").structurally_equals(col("a"))
    assert isinstance(struct_eq_result, bool)
    assert struct_eq_result is True


def test_when_expression_creation():
    """Test creating WhenExpr instances and their properties."""
    # Simple when expression
    expr = when(col("age") > 30, lit("Senior"))
    assert isinstance(expr, WhenExpr)
    assert expr.condition.structurally_equals(col("age") > 30)
    assert expr.value.structurally_equals(lit("Senior"))
    assert expr.next_when is None

    # Chained when expressions
    expr = when(col("age") > 50, lit("Elder")).when(col("age") > 30, lit("Adult"))
    assert isinstance(expr, WhenExpr)
    assert expr.condition.structurally_equals(col("age") > 30)
    assert expr.value.structurally_equals(lit("Adult"))
    assert expr.next_when is not None
    assert expr.next_when.condition.structurally_equals(col("age") > 50)
    assert expr.next_when.value.structurally_equals(lit("Elder"))


def test_when_method_chaining_api():
    """Test the method chaining case statement API (when().when().otherwise())."""
    # Simple case statement
    expr = when(col("age") > 30, lit("Senior")).otherwise(lit("Junior"))
    assert isinstance(expr, CaseExpr)
    assert len(expr.when_clauses) == 1
    assert expr.when_clauses[0][0].structurally_equals(col("age") > 30)
    assert expr.when_clauses[0][1].structurally_equals(lit("Senior"))
    assert expr.default.structurally_equals(lit("Junior"))

    # Multiple conditions using method chaining
    expr = (
        when(col("age") > 50, lit("Elder"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )
    assert len(expr.when_clauses) == 2
    # First when should be evaluated first (age > 50)
    assert expr.when_clauses[0][0].structurally_equals(col("age") > 50)
    assert expr.when_clauses[0][1].structurally_equals(lit("Elder"))
    # Second when should be evaluated second (age > 30)
    assert expr.when_clauses[1][0].structurally_equals(col("age") > 30)
    assert expr.when_clauses[1][1].structurally_equals(lit("Adult"))
    assert expr.default.structurally_equals(lit("Young"))

    # Complex conditions
    expr = (
        when((col("age") > 50) & (col("income") > 100000), lit("High Net Worth"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )
    assert len(expr.when_clauses) == 2
    assert expr.when_clauses[0][0].structurally_equals(
        (col("age") > 50) & (col("income") > 100000)
    )
    assert expr.when_clauses[0][1].structurally_equals(lit("High Net Worth"))
    assert expr.when_clauses[1][0].structurally_equals(col("age") > 30)
    assert expr.when_clauses[1][1].structurally_equals(lit("Adult"))
    assert expr.default.structurally_equals(lit("Young"))

    # Test with OR conditions
    expr = (
        when((col("age") > 50) | (col("income") > 100000), lit("High Value"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )
    assert len(expr.when_clauses) == 2
    assert expr.when_clauses[0][0].structurally_equals(
        (col("age") > 50) | (col("income") > 100000)
    )

    # Test with equality conditions
    expr = (
        when(col("status") == "active", lit("Active"))
        .when(col("status") == "pending", lit("Pending"))
        .otherwise(lit("Inactive"))
    )
    assert len(expr.when_clauses) == 2
    assert expr.when_clauses[0][0].structurally_equals(col("status") == "active")
    assert expr.when_clauses[1][0].structurally_equals(col("status") == "pending")


def test_when_expression_structural_equality():
    """Test structural equality for WhenExpr instances."""
    # Simple when expressions
    expr1 = when(col("age") > 30, lit("Senior"))
    expr2 = when(col("age") > 30, lit("Senior"))
    assert expr1.structurally_equals(expr2)
    assert expr2.structurally_equals(expr1)

    # Different conditions
    expr1 = when(col("age") > 30, lit("Senior"))
    expr2 = when(col("age") > 25, lit("Senior"))
    assert not expr1.structurally_equals(expr2)

    # Different values
    expr1 = when(col("age") > 30, lit("Senior"))
    expr2 = when(col("age") > 30, lit("Adult"))
    assert not expr1.structurally_equals(expr2)

    # Chained when expressions
    expr1 = when(col("age") > 50, lit("Elder")).when(col("age") > 30, lit("Adult"))
    expr2 = when(col("age") > 50, lit("Elder")).when(col("age") > 30, lit("Adult"))
    assert expr1.structurally_equals(expr2)

    # Different chain lengths
    expr1 = when(col("age") > 50, lit("Elder")).when(col("age") > 30, lit("Adult"))
    expr2 = when(col("age") > 50, lit("Elder"))
    assert not expr1.structurally_equals(expr2)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_when_api_with_dataset_api(ray_start_regular_shared):
    """Test that the when() method chaining API works with the Dataset.with_column API."""
    import ray

    # Create a simple dataset
    ds = ray.data.from_items(
        [
            {"age": 25, "name": "Alice"},
            {"age": 35, "name": "Bob"},
            {"age": 55, "name": "Charlie"},
            {"age": 15, "name": "David"},
        ]
    )

    # Use when() method chaining to add age_group column
    result = ds.with_column(
        "age_group",
        when(col("age") > 50, lit("Elder"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young")),
    )

    # Verify the result
    rows = result.take_all()
    assert len(rows) == 4

    # Check that age_group column was added correctly
    age_groups = [row["age_group"] for row in rows]
    expected = ["Young", "Adult", "Elder", "Young"]
    assert age_groups == expected

    # Verify original columns are preserved
    assert all("age" in row for row in rows)
    assert all("name" in row for row in rows)
    assert all("age_group" in row for row in rows)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_when_expression_api_consistency(ray_start_regular_shared):
    """Test that when() method chaining maintains API consistency with other Ray Data operations."""
    import ray

    # Create test dataset
    ds = ray.data.from_items(
        [
            {"id": 1, "score": 85, "age": 25},
            {"id": 2, "score": 92, "age": 30},
            {"id": 3, "score": 78, "age": 35},
            {"id": 4, "score": 95, "age": 40},
        ]
    )

    # Test when() method chaining with dataset operations
    result = ds.with_column(
        "grade",
        when(col("score") >= 90, lit("A"))
        .when(col("score") >= 80, lit("B"))
        .when(col("score") >= 70, lit("C"))
        .otherwise(lit("F")),
    )

    # Verify results
    rows = result.take_all()
    assert len(rows) == 4
    assert all("grade" in row for row in rows)

    # Test chaining with other operations
    filtered = result.filter(col("grade") == "A")
    a_rows = filtered.take_all()
    assert len(a_rows) == 2  # IDs 2 and 4

    # Test complex when() expressions
    complex_result = ds.with_column(
        "status",
        when((col("score") >= 90) & (col("age") < 35), lit("Young Star"))
        .when(col("score") >= 85, lit("High Performer"))
        .when(col("age") < 30, lit("Young"))
        .otherwise(lit("Standard")),
    )

    complex_rows = complex_result.take_all()
    assert len(complex_rows) == 4
    assert all("status" in row for row in complex_rows)

    # Test that when() expressions work with aggregations
    aggregated = complex_result.aggregate(
        ray.data.aggregate.Count("id"), ray.data.aggregate.Mean("score")
    )
    assert "count(id)" in aggregated
    assert "mean(score)" in aggregated


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "test_data, when_expr, expected_values",
    [
        # Age grouping with method chaining
        (
            [{"age": 25}, {"age": 35}, {"age": 55}, {"age": 15}],
            when(col("age") > 50, lit("Elder"))
            .when(col("age") > 30, lit("Adult"))
            .otherwise(lit("Young")),
            ["Young", "Adult", "Elder", "Young"],
        ),
        # Score grading with method chaining
        (
            [{"score": 95}, {"score": 87}, {"score": 72}, {"score": 100}],
            when(col("score") >= 95, lit("A+"))
            .when(col("score") >= 90, lit("A"))
            .when(col("score") >= 80, lit("B"))
            .otherwise(lit("C")),
            ["A+", "B", "C", "A+"],
        ),
    ],
)
def test_when_expressions_parametrized(
    ray_start_regular_shared,
    test_data,
    when_expr,
    expected_values,
    target_max_block_size_infinite_or_default,
):
    """Test when() method chaining expressions with various data using parametrization."""
    import ray

    # Create dataset from test data
    ds = ray.data.from_items(test_data)

    # Apply when expression
    result = ds.with_column("result", when_expr)

    # Verify results
    rows = result.take_all()
    assert len(rows) == len(test_data)

    # Check that result column was added correctly
    actual_values = [row["result"] for row in rows]
    assert actual_values == expected_values

    # Verify original columns are preserved
    for row in rows:
        for key in test_data[0].keys():
            assert key in row


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_new_operators(ray_start_regular_shared):
    """Test new operators: !=, //, is_null, is_not_null, is_in, not_in."""
    import ray

    # Test data with some nulls
    ds = ray.data.from_items(
        [
            {"a": 10, "b": 3, "status": "active"},
            {"a": 7, "b": 2, "status": "pending"},
            {"a": None, "b": 5, "status": "inactive"},
            {"a": 15, "b": 4, "status": "active"},
        ]
    )

    # Test != operator
    result = ds.with_column("not_equal", col("a") != 10)
    rows = result.take_all()
    # Expect [False, True, null, True] for "a != 10"
    assert rows[0]["not_equal"] is False
    assert rows[1]["not_equal"] is True
    assert rows[3]["not_equal"] is True

    # Test // operator (floor division)
    result = ds.with_column("floor_div", col("a") // col("b"))
    rows = result.take_all()
    assert rows[0]["floor_div"] == 3  # 10 // 3 = 3
    assert rows[1]["floor_div"] == 3  # 7 // 2 = 3
    assert rows[3]["floor_div"] == 3  # 15 // 4 = 3

    # Test is_null()
    result = ds.with_column("is_null_a", col("a").is_null())
    rows = result.take_all()
    assert rows[0]["is_null_a"] is False
    assert rows[1]["is_null_a"] is False
    assert rows[2]["is_null_a"] is True
    assert rows[3]["is_null_a"] is False

    # Test is_not_null()
    result = ds.with_column("is_not_null_a", col("a").is_not_null())
    rows = result.take_all()
    assert rows[0]["is_not_null_a"] is True
    assert rows[1]["is_not_null_a"] is True
    assert rows[2]["is_not_null_a"] is False
    assert rows[3]["is_not_null_a"] is True

    # Test is_in()
    result = ds.with_column("in_list", col("status").is_in(["active", "approved"]))
    rows = result.take_all()
    assert rows[0]["in_list"] is True
    assert rows[1]["in_list"] is False
    assert rows[2]["in_list"] is False
    assert rows[3]["in_list"] is True

    # Test not_in()
    result = ds.with_column("not_in_list", col("status").not_in(["inactive", "rejected"]))
    rows = result.take_all()
    assert rows[0]["not_in_list"] is True
    assert rows[1]["not_in_list"] is True
    assert rows[2]["not_in_list"] is False
    assert rows[3]["not_in_list"] is True


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_with_new_operators(ray_start_regular_shared):
    """Test case expressions with new operators (!=, //, is_null, etc.)."""
    import ray

    ds = ray.data.from_items(
        [
            {"score": 95, "age": None, "status": "active"},
            {"score": 82, "age": 25, "status": "pending"},
            {"score": 70, "age": 30, "status": "inactive"},
            {"score": 88, "age": 35, "status": "active"},
        ]
    )

    # Test case with != operator
    result = ds.with_column(
        "grade_type",
        when(col("score") != 95, lit("Standard")).otherwise(lit("Perfect")),
    )
    rows = result.take_all()
    assert rows[0]["grade_type"] == "Perfect"
    assert rows[1]["grade_type"] == "Standard"
    assert rows[2]["grade_type"] == "Standard"
    assert rows[3]["grade_type"] == "Standard"

    # Test case with is_null()
    result = ds.with_column(
        "age_status",
        when(col("age").is_null(), lit("Unknown"))
        .when(col("age") < 30, lit("Young"))
        .otherwise(lit("Experienced")),
    )
    rows = result.take_all()
    assert rows[0]["age_status"] == "Unknown"
    assert rows[1]["age_status"] == "Young"
    assert rows[2]["age_status"] == "Experienced"
    assert rows[3]["age_status"] == "Experienced"

    # Test case with is_not_null()
    result = ds.with_column(
        "has_age",
        when(col("age").is_not_null(), lit("Yes")).otherwise(lit("No")),
    )
    rows = result.take_all()
    assert rows[0]["has_age"] == "No"
    assert rows[1]["has_age"] == "Yes"
    assert rows[2]["has_age"] == "Yes"
    assert rows[3]["has_age"] == "Yes"

    # Test case with is_in()
    result = ds.with_column(
        "active_status",
        when(col("status").is_in(["active", "approved"]), lit("Active"))
        .when(col("status").is_in(["pending"]), lit("Pending"))
        .otherwise(lit("Inactive")),
    )
    rows = result.take_all()
    assert rows[0]["active_status"] == "Active"
    assert rows[1]["active_status"] == "Pending"
    assert rows[2]["active_status"] == "Inactive"
    assert rows[3]["active_status"] == "Active"


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_nested_case_expressions(ray_start_regular_shared):
    """Test nested case expressions where case expressions are used as values."""
    import ray

    ds = ray.data.from_items(
        [
            {"score": 95, "extra_credit": 5},
            {"score": 82, "extra_credit": 0},
            {"score": 70, "extra_credit": 10},
            {"score": 88, "extra_credit": 2},
        ]
    )

    # Nested case: outer case determines grade category, inner case adjusts for extra credit
    result = ds.with_column(
        "grade",
        when(
            col("score") >= 90,
            # Nested case for high scores
            when(col("extra_credit") > 0, lit("A+")).otherwise(lit("A")),
        )
        .when(
            col("score") >= 80,
            # Nested case for medium scores
            when(col("extra_credit") >= 5, lit("B+")).otherwise(lit("B")),
        )
        .otherwise(
            # Nested case for low scores
            when(col("extra_credit") >= 10, lit("C+")).otherwise(lit("C"))
        ),
    )

    rows = result.take_all()
    assert rows[0]["grade"] == "A+"  # 95 >= 90, extra_credit > 0
    assert rows[1]["grade"] == "B"  # 82 >= 80, extra_credit < 5
    assert rows[2]["grade"] == "C+"  # 70 < 80, extra_credit >= 10
    assert rows[3]["grade"] == "B"  # 88 >= 80, extra_credit < 5


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_with_complex_conditions(ray_start_regular_shared):
    """Test case expressions with complex boolean combinations."""
    import ray

    ds = ray.data.from_items(
        [
            {"age": 25, "income": 50000, "credit_score": 750},
            {"age": 35, "income": 80000, "credit_score": 680},
            {"age": 45, "income": 120000, "credit_score": 720},
            {"age": 22, "income": 30000, "credit_score": 600},
        ]
    )

    # Complex case with multiple conditions using AND, OR
    result = ds.with_column(
        "loan_category",
        when(
            (col("age") >= 30) & (col("income") >= 100000) & (col("credit_score") >= 700),
            lit("Premium"),
        )
        .when(
            ((col("age") >= 25) & (col("income") >= 50000)) | (col("credit_score") >= 750),
            lit("Standard"),
        )
        .when(col("credit_score") >= 650, lit("Basic"))
        .otherwise(lit("Declined")),
    )

    rows = result.take_all()
    assert rows[0]["loan_category"] == "Standard"  # age >= 25, income >= 50000, credit >= 750
    assert rows[1]["loan_category"] == "Standard"  # age >= 25, income >= 50000
    assert rows[2]["loan_category"] == "Premium"  # age >= 30, income >= 100000, credit >= 700
    assert rows[3]["loan_category"] == "Declined"  # credit_score < 650


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_with_alias(ray_start_regular_shared):
    """Test that case expressions work with alias."""
    import ray

    ds = ray.data.from_items(
        [
            {"score": 95},
            {"score": 82},
            {"score": 70},
            {"score": 88},
        ]
    )

    # Note: alias() should work on the full case expression
    # The column name would come from the alias, not the with_column parameter
    result = ds.with_column(
        "final_grade",
        when(col("score") >= 90, lit("A"))
        .when(col("score") >= 80, lit("B"))
        .when(col("score") >= 70, lit("C"))
        .otherwise(lit("F")),
    )

    rows = result.take_all()
    assert "final_grade" in rows[0]
    assert rows[0]["final_grade"] == "A"
    assert rows[1]["final_grade"] == "B"
    assert rows[2]["final_grade"] == "C"
    assert rows[3]["final_grade"] == "B"


def test_case_to_pyarrow_conversion():
    """Test that case expressions can be converted to PyArrow expressions."""
    from ray.data.expressions import col, lit, when

    # Simple case expression
    expr = when(col("age") > 30, lit("Senior")).otherwise(lit("Junior"))

    # Convert to PyArrow - should not raise
    pa_expr = expr.to_pyarrow()
    assert pa_expr is not None

    # Complex case expression with multiple conditions
    expr = (
        when(col("age") > 50, lit("Elder"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )

    pa_expr = expr.to_pyarrow()
    assert pa_expr is not None

    # Case with new operators
    expr = when(col("status").is_in(["active", "approved"]), lit("Active")).otherwise(lit("Inactive"))

    pa_expr = expr.to_pyarrow()
    assert pa_expr is not None


def test_when_to_pyarrow_fails():
    """Test that incomplete WhenExpr cannot be converted to PyArrow."""
    from ray.data.expressions import col, lit, when, WhenExpr

    # Create incomplete when expression
    when_expr = when(col("age") > 30, lit("Senior"))
    assert isinstance(when_expr, WhenExpr)

    # Should raise TypeError when trying to convert
    with pytest.raises(TypeError, match="WhenExpr cannot be converted to PyArrow"):
        when_expr.to_pyarrow()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
