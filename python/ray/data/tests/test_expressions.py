import pytest

from ray.data.expressions import CaseExpr, Expr, WhenExpr, case, col, lit, when

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
    # Case expressions (function-based)
    (
        case([(col("a") > 1, lit("high"))], default=lit("low")),
        case([(col("a") > 1, lit("high"))], default=lit("low")),
        True,
    ),
    (
        case([(col("a") > 1, lit("high"))], default=lit("low")),
        case([(col("a") > 2, lit("high"))], default=lit("low")),
        False,
    ),  # Different condition
    (
        case([(col("a") > 1, lit("high"))], default=lit("low")),
        case([(col("a") > 1, lit("medium"))], default=lit("low")),
        False,
    ),  # Different value
    (
        case([(col("a") > 1, lit("high"))], default=lit("low")),
        case([(col("a") > 1, lit("high"))], default=lit("default")),
        False,
    ),  # Different default
    (
        case([(col("a") > 1, lit("high"))], default=lit("low")),
        case(
            [(col("a") > 1, lit("high")), (col("b") > 1, lit("medium"))],
            default=lit("low"),
        ),
        False,
    ),  # Different number of clauses
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


def test_case_expression_creation():
    """Test creating case expressions with various configurations.

    This test verifies that case expressions can be created correctly with
    different numbers of conditions and various data types.
    """
    # Simple case with one condition
    expr = case([(col("age") > 30, lit("Senior"))], default=lit("Junior"))
    assert isinstance(expr, CaseExpr)
    assert len(expr.when_clauses) == 1
    assert expr.when_clauses[0][0].structurally_equals(col("age") > 30)
    assert expr.when_clauses[0][1].structurally_equals(lit("Senior"))
    assert expr.default.structurally_equals(lit("Junior"))

    # Multiple conditions
    expr = case(
        [
            (col("age") > 50, lit("Elder")),
            (col("age") > 30, lit("Adult")),
            (col("age") > 18, lit("Young")),
        ],
        default=lit("Child"),
    )
    assert len(expr.when_clauses) == 3
    assert expr.when_clauses[0][0].structurally_equals(col("age") > 50)
    assert expr.when_clauses[1][0].structurally_equals(col("age") > 30)
    assert expr.when_clauses[2][0].structurally_equals(col("age") > 18)

    # Test with different data types
    expr = case(
        [(col("is_active") == True, lit(1)), (col("is_active") == False, lit(0))],
        default=lit(-1),
    )
    assert len(expr.when_clauses) == 2
    assert expr.when_clauses[0][1].structurally_equals(lit(1))
    assert expr.when_clauses[1][1].structurally_equals(lit(0))
    assert expr.default.structurally_equals(lit(-1))


def test_case_expression_validation():
    """Test case expression validation and error handling.

    This test ensures that case expressions properly validate their inputs
    and raise appropriate errors for invalid configurations.
    """
    # Empty when_clauses should raise error
    with pytest.raises(
        ValueError, match="case\\(\\) must have at least one when clause"
    ):
        case([], default=lit("default"))

    # Test with None default (should work)
    expr = case([(col("age") > 30, lit("Senior"))], default=lit(None))
    assert expr.default.structurally_equals(lit(None))

    # Test with invalid when_clauses structure - this will fail during evaluation
    # because the string "not a tuple" doesn't have the required Expr methods
    invalid_expr = case([("not a tuple", lit("value"))], default=lit("default"))
    # The expression is created successfully, but will fail during evaluation
    assert isinstance(invalid_expr, CaseExpr)

    # Test with incomplete tuple (missing value)
    with pytest.raises(ValueError):
        case([(col("age") > 30,)], default=lit("default"))  # Missing value

    # Test with None conditions (should work as expressions)
    expr = case(
        [(lit(True), lit("Always True")), (lit(False), lit("Always False"))],
        default=lit("Default"),
    )
    assert len(expr.when_clauses) == 2


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
    """Test the new method chaining case statement API (when().when().otherwise())."""
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


def test_when_api_equivalence():
    """Test that the when() API produces equivalent results to case() API."""
    # Test that both APIs produce equivalent expressions
    case_expr = case(
        [(col("age") > 50, lit("Elder")), (col("age") > 30, lit("Adult"))],
        default=lit("Young"),
    )

    when_expr = (
        when(col("age") > 50, lit("Elder"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )

    # Both should be structurally equal
    assert case_expr.structurally_equals(when_expr)
    assert when_expr.structurally_equals(case_expr)

    # Test with more complex conditions
    case_expr = case(
        [
            ((col("age") > 50) & (col("income") > 100000), lit("High Net Worth")),
            (col("age") > 30, lit("Adult")),
            (col("is_student") == True, lit("Student")),
        ],
        default=lit("Young"),
    )

    when_expr = (
        when((col("age") > 50) & (col("income") > 100000), lit("High Net Worth"))
        .when(col("age") > 30, lit("Adult"))
        .when(col("is_student") == True, lit("Student"))
        .otherwise(lit("Young"))
    )

    assert case_expr.structurally_equals(when_expr)
    assert when_expr.structurally_equals(case_expr)


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


def test_case_expression_edge_cases():
    """Test edge cases and boundary conditions for case expressions."""
    # Single condition with complex boolean logic
    expr = case(
        [
            (
                (col("age") > 18) & (col("age") < 65) & (col("is_employed") == True),
                lit("Working Age"),
            )
        ],
        default=lit("Other"),
    )
    assert len(expr.when_clauses) == 1
    assert isinstance(expr.when_clauses[0][0], Expr)

    # Multiple conditions with different data types
    expr = case(
        [
            (col("score") > 90, lit(100)),
            (col("score") > 80, lit(90)),
            (col("score") > 70, lit(80)),
        ],
        default=lit(0),
    )
    assert len(expr.when_clauses) == 3
    assert all(isinstance(clause[0], Expr) for clause in expr.when_clauses)
    assert all(isinstance(clause[1], Expr) for clause in expr.when_clauses)

    # Test with None values
    expr = case(
        [
            (col("status") == "active", lit("Active")),
            (col("status") == "inactive", lit(None)),
        ],
        default=lit("Unknown"),
    )
    assert len(expr.when_clauses) == 2


def test_when_expression_edge_cases():
    """Test edge cases and boundary conditions for when expressions."""
    # Deep chaining
    expr = (
        when(col("level") > 10, lit("Expert"))
        .when(col("level") > 7, lit("Advanced"))
        .when(col("level") > 4, lit("Intermediate"))
        .when(col("level") > 1, lit("Beginner"))
        .otherwise(lit("Novice"))
    )

    assert isinstance(expr, CaseExpr)
    assert len(expr.when_clauses) == 4

    # Test with empty conditions (should work)
    expr = when(col("always_true") == True, lit("Always")).otherwise(lit("Never"))
    assert len(expr.when_clauses) == 1

    # Test with complex nested conditions
    expr = when(
        ((col("age") > 18) & (col("income") > 50000)) | (col("is_student") == True),
        lit("Eligible"),
    ).otherwise(lit("Not Eligible"))
    assert len(expr.when_clauses) == 1


def test_case_expression_with_dataset_api():
    """Test that case expressions work with the Dataset.with_column API."""
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

    # Use case statement to add age_group column
    result = ds.with_column(
        "age_group",
        case(
            [
                (col("age") > 50, lit("Elder")),
                (col("age") > 30, lit("Adult")),
                (col("age") > 18, lit("Young")),
            ],
            default=lit("Child"),
        ),
    )

    # Verify the result
    rows = result.take_all()
    assert len(rows) == 4

    # Check that age_group column was added correctly
    age_groups = [row["age_group"] for row in rows]
    expected = ["Young", "Adult", "Elder", "Child"]
    assert age_groups == expected

    # Verify original columns are preserved
    assert all("age" in row for row in rows)
    assert all("name" in row for row in rows)
    assert all("age_group" in row for row in rows)


def test_when_api_with_dataset_api():
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


def test_case_expression_performance_characteristics():
    """Test performance characteristics and optimization features."""
    # Test that expressions are properly structured for evaluation
    expr = (
        when(col("age") > 50, lit("Elder"))
        .when(col("age") > 30, lit("Adult"))
        .otherwise(lit("Young"))
    )

    # Verify evaluation order is correct (most specific to least specific)
    assert expr.when_clauses[0][0].structurally_equals(
        col("age") > 50
    )  # Most specific first
    assert expr.when_clauses[1][0].structurally_equals(
        col("age") > 30
    )  # Less specific second

    # Test with many conditions
    expr = (
        when(col("score") >= 95, lit("A+"))
        .when(col("score") >= 90, lit("A"))
        .when(col("score") >= 85, lit("B+"))
        .when(col("score") >= 80, lit("B"))
        .when(col("score") >= 75, lit("C+"))
        .when(col("score") >= 70, lit("C"))
        .otherwise(lit("F"))
    )

    assert len(expr.when_clauses) == 6
    # Verify conditions are ordered from highest to lowest
    assert expr.when_clauses[0][0].structurally_equals(col("score") >= 95)
    assert expr.when_clauses[5][0].structurally_equals(col("score") >= 70)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
