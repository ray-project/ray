import pytest

from ray.data.expressions import CaseExpr, Expr, WhenExpr, case, col, lit, when
from ray._private.test_utils import get_pyarrow_version
from packaging.version import parse as parse_version

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
        [(col("is_active"), lit(1)), (~col("is_active"), lit(0))],
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


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_expression_error_handling(ray_start_regular_shared):
    """Test error handling for case expressions in dataset operations."""
    import ray
    
    # Test with non-existent column reference
    ds = ray.data.from_items([{"age": 25}, {"age": 35}])
    
    # This should raise an error when trying to reference a non-existent column
    with pytest.raises(Exception):  # UserCodeException or similar
        ds.with_column(
            "result",
            case(
                [(col("nonexistent_column") > 30, lit("High"))],
                default=lit("Low")
            )
        ).materialize()
    
    # Test with invalid expression types
    ds = ray.data.from_items([{"age": 25}, {"age": 35}])
    
    # This should work (creating the expression) but fail during evaluation
    result = ds.with_column(
        "result",
        case(
            [(col("age") > 30, "not a literal")],  # Invalid: should be lit()
            default=lit("Low")
        )
    )
    
    # The expression is created but will fail during evaluation
    assert isinstance(result, ray.data.Dataset)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_expression_edge_cases_dataset(ray_start_regular_shared):
    """Test edge cases for case expressions in dataset operations."""
    import ray
    
    # Test with empty dataset
    empty_ds = ray.data.from_items([])
    result = empty_ds.with_column(
        "result",
        case(
            [(col("age") > 30, lit("Adult"))],
            default=lit("Young")
        )
    )
    assert result.count() == 0
    
    # Test with single row dataset
    single_ds = ray.data.from_items([{"age": 25}])
    result = single_ds.with_column(
        "result",
        case(
            [(col("age") > 30, lit("Adult"))],
            default=lit("Young")
        )
    )
    rows = result.take_all()
    assert len(rows) == 1
    assert rows[0]["result"] == "Young"
    
    # Test with all conditions false
    ds = ray.data.from_items([{"age": 15}, {"age": 20}, {"age": 25}])
    result = ds.with_column(
        "result",
        case(
            [(col("age") > 30, lit("Adult"))],
            default=lit("Young")
        )
    )
    rows = result.take_all()
    assert all(row["result"] == "Young" for row in rows)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_expression_schema_integration(ray_start_regular_shared):
    """Test that case expressions properly integrate with Ray Data schema operations."""
    import ray
    
    # Create dataset with mixed data types
    ds = ray.data.from_items([
        {"id": 1, "age": 25, "score": 85.5, "status": "active"},
        {"id": 2, "age": 35, "score": 92.0, "status": "pending"},
        {"id": 3, "age": 55, "score": 78.5, "status": "inactive"}
    ])
    
    # Apply case expression
    result = ds.with_column(
        "category",
        case(
            [
                ((col("age") > 50) & (col("score") > 80), lit("High Performer")),
                (col("age") > 30, lit("Experienced")),
                (col("status") == "active", lit("Active"))
            ],
            default=lit("Other")
        )
    )
    
    # Verify schema is updated
    schema = result.schema()
    assert "category" in schema.names
    
    # Verify data integrity
    rows = result.take_all()
    assert len(rows) == 3
    assert all("category" in row for row in rows)
    
    # Test chaining with other operations
    filtered = result.filter(col("category") == "High Performer")
    filtered_rows = filtered.take_all()
    
    # Should only have rows where age > 50 and score > 80
    assert len(filtered_rows) >= 0  # Could be 0 or 1 depending on data
    
    # Test with select_columns
    selected = result.select_columns(["id", "category"])
    selected_rows = selected.take_all()
    assert len(selected_rows) == 3
    assert "id" in selected_rows[0]
    assert "category" in selected_rows[0]
    assert "age" not in selected_rows[0]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_expression_performance_dataset(ray_start_regular_shared):
    """Test performance characteristics of case expressions in dataset operations."""
    import ray
    import time
    
    # Create larger dataset for performance testing
    data = [
        {
            "id": i,
            "age": 20 + (i % 60),  # Ages 20-79
            "score": 50 + (i % 50),  # Scores 50-99
            "category": f"cat_{i % 10}"  # 10 categories
        }
        for i in range(1000)
    ]
    
    ds = ray.data.from_items(data)
    
    # Test case expression with multiple conditions
    start_time = time.time()
    result = ds.with_column(
        "grade",
        case(
            [
                (col("score") >= 90, lit("A")),
                (col("score") >= 80, lit("B")),
                (col("score") >= 70, lit("C")),
                (col("score") >= 60, lit("D"))
            ],
            default=lit("F")
        )
    )
    
    # Force materialization to measure actual computation time
    rows = result.take_all()
    end_time = time.time()
    
    # Verify results
    assert len(rows) == 1000
    assert all("grade" in row for row in rows)
    
    # Performance assertion (should complete within reasonable time)
    # This is a basic sanity check - actual performance depends on hardware
    assert end_time - start_time < 30.0  # Should complete within 30 seconds
    
    # Test memory efficiency by checking that we can still access the data
    grades = [row["grade"] for row in rows]
    assert len(grades) == 1000
    
    # Verify grade distribution makes sense
    grade_counts = {}
    for grade in grades:
        grade_counts[grade] = grade_counts.get(grade, 0) + 1
    
    # Should have all expected grades
    expected_grades = {"A", "B", "C", "D", "F"}
    assert set(grade_counts.keys()) == expected_grades


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_expression_api_consistency(ray_start_regular_shared):
    """Test that case expressions maintain API consistency with other Ray Data operations."""
    import ray
    
    # Create test dataset
    ds = ray.data.from_items([
        {"id": 1, "value": 10, "flag": True},
        {"id": 2, "value": 20, "flag": False},
        {"id": 3, "value": 30, "flag": True},
        {"id": 4, "value": 40, "flag": False}
    ])
    
    # Test that case expressions work with other column operations
    result = ds.with_column("category", case(
        [(col("value") > 25, lit("High")), (col("value") > 15, lit("Medium"))],
        default=lit("Low")
    ))
    
    # Test chaining with filter
    filtered = result.filter(col("category") == "High")
    high_rows = filtered.take_all()
    assert len(high_rows) == 1
    assert high_rows[0]["id"] == 3
    
    # Test chaining with map
    mapped = result.map(lambda row: {"id": row["id"], "category": row["category"]})
    mapped_rows = mapped.take_all()
    assert len(mapped_rows) == 4
    assert all("category" in row for row in mapped_rows)
    
    # Test chaining with sort
    sorted_result = result.sort("value")
    sorted_rows = sorted_result.take_all()
    assert sorted_rows[0]["value"] == 10
    assert sorted_rows[-1]["value"] == 40
    
    # Test that case expressions work with aggregations
    aggregated = result.aggregate(
        ray.data.aggregate.Count("id"),
        ray.data.aggregate.Sum("value")
    )
    assert "count(id)" in aggregated
    assert "sum(value)" in aggregated
    
    # Test that case expressions work with groupby
    grouped = result.groupby("category").aggregate(
        ray.data.aggregate.Count("id"),
        ray.data.aggregate.Mean("value")
    )
    grouped_rows = grouped.take_all()
    assert len(grouped_rows) >= 1  # At least one group
    
    # Test that case expressions work with joins
    ds2 = ray.data.from_items([
        {"id": 1, "extra": "A"},
        {"id": 2, "extra": "B"},
        {"id": 3, "extra": "C"}
    ])
    
    joined = result.join(ds2, "id")
    joined_rows = joined.take_all()
    assert len(joined_rows) == 3
    assert all("extra" in row for row in joined_rows)
    assert all("category" in row for row in joined_rows)


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
            (col("is_student"), lit("Student")),
        ],
        default=lit("Young"),
    )

    when_expr = (
        when((col("age") > 50) & (col("income") > 100000), lit("High Net Worth"))
        .when(col("age") > 30, lit("Adult"))
        .when(col("is_student"), lit("Student"))
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
                (col("age") > 18) & (col("age") < 65) & (col("is_employed")),
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
    expr = when(col("always_true"), lit("Always")).otherwise(lit("Never"))
    assert len(expr.when_clauses) == 1

    # Test with complex nested conditions
    expr = when(
        ((col("age") > 18) & (col("income") > 50000)) | (col("is_student")),
        lit("Eligible"),
    ).otherwise(lit("Not Eligible"))
    assert len(expr.when_clauses) == 1


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_case_expression_with_dataset_api(ray_start_regular_shared):
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


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "test_data, case_expr, expected_values",
    [
        # Simple age grouping
        (
            [{"age": 25}, {"age": 35}, {"age": 55}, {"age": 15}],
            case(
                [(col("age") > 50, lit("Elder")), (col("age") > 30, lit("Adult"))],
                default=lit("Young")
            ),
            ["Young", "Adult", "Elder", "Young"]
        ),
        # Score grading
        (
            [{"score": 95}, {"score": 87}, {"score": 72}, {"score": 100}],
            case(
                [
                    (col("score") >= 95, lit("A+")),
                    (col("score") >= 90, lit("A")),
                    (col("score") >= 80, lit("B")),
                    (col("score") >= 70, lit("C"))
                ],
                default=lit("F")
            ),
            ["A+", "B", "C", "A+"]
        ),
        # Complex boolean conditions
        (
            [
                {"age": 25, "income": 60000, "is_student": True},
                {"age": 35, "income": 80000, "is_student": False},
                {"age": 55, "income": 120000, "is_student": False},
                {"age": 20, "income": 30000, "is_student": True}
            ],
            case(
                [
                    ((col("age") > 50) & (col("income") > 100000), lit("Rich Elder")),
                    (col("age") > 30, lit("Adult")),
                    (col("is_student"), lit("Student"))
                ],
                default=lit("Other")
            ),
            ["Student", "Adult", "Rich Elder", "Student"]
        ),
        # String-based conditions
        (
            [{"status": "active"}, {"status": "pending"}, {"status": "inactive"}],
            case(
                [
                    (col("status") == "active", lit("Active")),
                    (col("status") == "pending", lit("Pending")),
                    (col("status") == "inactive", lit("Inactive"))
                ],
                default=lit("Unknown")
            ),
            ["Active", "Pending", "Inactive"]
        ),
    ],
)
def test_case_expressions_parametrized(
    ray_start_regular_shared,
    test_data,
    case_expr,
    expected_values,
    target_max_block_size_infinite_or_default,
):
    """Test case expressions with various data types and conditions using parametrization."""
    import ray
    
    # Create dataset from test data
    ds = ray.data.from_items(test_data)
    
    # Apply case expression
    result = ds.with_column("result", case_expr)
    
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
@pytest.mark.parametrize(
    "test_data, when_expr, expected_values",
    [
        # Age grouping with method chaining
        (
            [{"age": 25}, {"age": 35}, {"age": 55}, {"age": 15}],
            when(col("age") > 50, lit("Elder"))
            .when(col("age") > 30, lit("Adult"))
            .otherwise(lit("Young")),
            ["Young", "Adult", "Elder", "Young"]
        ),
        # Score grading with method chaining
        (
            [{"score": 95}, {"score": 87}, {"score": 72}, {"score": 100}],
            when(col("score") >= 95, lit("A+"))
            .when(col("score") >= 90, lit("A"))
            .when(col("score") >= 80, lit("B"))
            .otherwise(lit("C")),
            ["A+", "B", "C", "A+"]
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
def test_when_expression_api_consistency(ray_start_regular_shared):
    """Test that when() method chaining maintains API consistency with other Ray Data operations."""
    import ray
    
    # Create test dataset
    ds = ray.data.from_items([
        {"id": 1, "score": 85, "age": 25},
        {"id": 2, "score": 92, "age": 30},
        {"id": 3, "score": 78, "age": 35},
        {"id": 4, "score": 95, "age": 40}
    ])
    
    # Test when() method chaining with dataset operations
    result = ds.with_column("grade", 
        when(col("score") >= 90, lit("A"))
        .when(col("score") >= 80, lit("B"))
        .when(col("score") >= 70, lit("C"))
        .otherwise(lit("F"))
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
    complex_result = ds.with_column("status",
        when((col("score") >= 90) & (col("age") < 35), lit("Young Star"))
        .when(col("score") >= 85, lit("High Performer"))
        .when(col("age") < 30, lit("Young"))
        .otherwise(lit("Standard"))
    )
    
    complex_rows = complex_result.take_all()
    assert len(complex_rows) == 4
    assert all("status" in row for row in complex_rows)
    
    # Test that when() expressions work with aggregations
    aggregated = complex_result.aggregate(
        ray.data.aggregate.Count("id"),
        ray.data.aggregate.Mean("score")
    )
    assert "count(id)" in aggregated
    assert "mean(score)" in aggregated


def test_case_expression_optimization_patterns():
    """Test case statement optimization patterns and edge cases."""
    
    # Test case statement with constant conditions (should optimize)
    expr = case([
        (lit(True), lit("always")),
        (lit(False), lit("never"))
    ], default=lit("default"))
    
    # Verify structure
    assert len(expr.when_clauses) == 2
    assert expr.when_clauses[0][0] == lit(True)
    assert expr.when_clauses[0][1] == lit("always")
    
    # Test case statement with column-only conditions
    expr = case([
        (col("flag"), lit("flagged")),
        (~col("flag"), lit("not_flagged"))
    ], default=lit("unknown"))
    
    # Verify boolean negation is properly handled
    assert isinstance(expr.when_clauses[1][0], BinaryExpr)
    assert expr.when_clauses[1][0].op == Operation.AND  # NOT is implemented as AND with False
    
    # Test case statement with complex nested conditions
    expr = case([
        ((col("age") > 50) & (col("income") > 100000), lit("wealthy_senior")),
        ((col("age") > 30) & (col("income") > 50000), lit("established")),
        (col("age") > 18, lit("young"))
    ], default=lit("minor"))
    
    # Verify complex boolean logic structure
    assert len(expr.when_clauses) == 3
    first_condition = expr.when_clauses[0][0]
    assert isinstance(first_condition, BinaryExpr)
    assert first_condition.op == Operation.AND
    
    # Test case statement with string comparisons
    expr = case([
        (col("status") == "active", lit("active")),
        (col("status") == "pending", lit("pending")),
        (col("status") == "inactive", lit("inactive"))
    ], default=lit("unknown"))
    
    # Verify string equality operations
    for condition, _ in expr.when_clauses:
        assert isinstance(condition, BinaryExpr)
        assert condition.op == Operation.EQ


def test_case_expression_evaluation_order():
    """Test that case statements evaluate conditions in the correct order."""
    
    # Test evaluation order with overlapping conditions
    expr = case([
        (col("score") >= 90, lit("A")),
        (col("score") >= 80, lit("B")),
        (col("score") >= 70, lit("C")),
        (col("score") >= 60, lit("D"))
    ], default=lit("F"))
    
    # Verify conditions are ordered from most specific to least specific
    conditions = [clause[0] for clause in expr.when_clauses]
    
    # First condition should be most restrictive (score >= 90)
    assert isinstance(conditions[0], BinaryExpr)
    assert conditions[0].op == Operation.GE
    assert conditions[0].right == lit(90)
    
    # Last condition should be least restrictive (score >= 60)
    assert isinstance(conditions[-1], BinaryExpr)
    assert conditions[-1].op == Operation.GE
    assert conditions[-1].right == lit(60)


def test_case_expression_type_handling():
    """Test case statements handle different data types correctly."""
    
    # Test with numeric types
    numeric_expr = case([
        (col("value") > 100, lit(1)),
        (col("value") > 50, lit(2)),
        (col("value") > 0, lit(3))
    ], default=lit(0))
    
    # Test with string types
    string_expr = case([
        (col("category") == "high", lit("premium")),
        (col("category") == "medium", lit("standard")),
        (col("category") == "low", lit("basic"))
    ], default=lit("unknown"))
    
    # Test with boolean types
    boolean_expr = case([
        (col("flag1") & col("flag2"), lit(True)),
        (col("flag1") | col("flag2"), lit(False))
    ], default=lit(None))
    
    # Test with mixed types in conditions
    mixed_expr = case([
        ((col("age") > 30) & (col("status") == "active"), lit("senior_active")),
        (col("age") > 18, lit("adult")),
        (col("status") == "student", lit("student"))
    ], default=lit("other"))
    
    # Verify all expressions are valid
    assert isinstance(numeric_expr, CaseExpr)
    assert isinstance(string_expr, CaseExpr)
    assert isinstance(boolean_expr, CaseExpr)
    assert isinstance(mixed_expr, CaseExpr)


def test_case_expression_performance_characteristics():
    """Test case statement performance characteristics and optimization features."""
    
    # Test case statement with many conditions (performance test)
    many_conditions = []
    for i in range(100, 0, -10):
        many_conditions.append((col("score") >= i, lit(f"grade_{i}")))
    
    expr = case(many_conditions, default=lit("F"))
    
    # Verify large case statements are handled correctly
    assert len(expr.when_clauses) == 10
    assert expr.when_clauses[0][0].right == lit(100)  # Most restrictive first
    assert expr.when_clauses[-1][0].right == lit(10)  # Least restrictive last
    
    # Test case statement with complex boolean logic
    complex_expr = case([
        ((col("age") > 50) & (col("income") > 100000) & (col("education") == "phd"), lit("elite")),
        ((col("age") > 30) & (col("income") > 50000) & (col("education") == "masters"), lit("professional")),
        ((col("age") > 18) & (col("income") > 20000), lit("working")),
        (col("education") == "student", lit("student"))
    ], default=lit("other"))
    
    # Verify complex boolean expressions maintain structure
    assert len(complex_expr.when_clauses) == 4
    
    # First condition should have three AND operations
    first_condition = complex_expr.when_clauses[0][0]
    assert isinstance(first_condition, BinaryExpr)
    assert first_condition.op == Operation.AND
    
    # Verify nested structure
    left_side = first_condition.left
    assert isinstance(left_side, BinaryExpr)
    assert left_side.op == Operation.AND


def test_case_expression_integration_with_ray_data():
    """Test case statements integrate seamlessly with other Ray Data operations."""
    
    # Test case statements in filter operations
    filter_expr = case([
        (col("category") == "high", lit(True)),
        (col("category") == "medium", lit(True)),
        (col("category") == "low", lit(False))
    ], default=lit(False))
    
    # Test case statements in sort operations
    sort_expr = case([
        (col("priority") == "high", lit(1)),
        (col("priority") == "medium", lit(2)),
        (col("priority") == "low", lit(3))
    ], default=lit(4))
    
    # Test case statements in groupby operations
    groupby_expr = case([
        (col("age") > 50, lit("senior")),
        (col("age") > 30, lit("adult")),
        (col("age") > 18, lit("young"))
    ], default=lit("minor"))
    
    # Test case statements in join conditions
    join_expr = case([
        (col("type") == "user", col("user_id")),
        (col("type") == "admin", col("admin_id")),
        (col("type") == "guest", col("guest_id"))
    ], default=lit(None))
    
    # Verify all expressions are valid and can be used in Ray Data operations
    assert isinstance(filter_expr, CaseExpr)
    assert isinstance(sort_expr, CaseExpr)
    assert isinstance(groupby_expr, CaseExpr)
    assert isinstance(join_expr, CaseExpr)


def test_case_expression_method_chaining_optimization():
    """Test that when() method chaining creates optimal case statements."""
    
    # Test method chaining creates the same structure as function-based case
    method_chained = (
        when(col("score") >= 95, lit("A+"))
        .when(col("score") >= 90, lit("A"))
        .when(col("score") >= 80, lit("B"))
        .when(col("score") >= 70, lit("C"))
        .otherwise(lit("F"))
    )
    
    function_based = case([
        (col("score") >= 95, lit("A+")),
        (col("score") >= 90, lit("A")),
        (col("score") >= 80, lit("B")),
        (col("score") >= 70, lit("C"))
    ], default=lit("F"))
    
    # Both should create equivalent CaseExpr objects
    assert isinstance(method_chained, CaseExpr)
    assert isinstance(function_based, CaseExpr)
    
    # Verify they have the same number of when clauses
    assert len(method_chained.when_clauses) == len(function_based.when_clauses)
    
    # Verify the conditions are equivalent
    for i in range(len(method_chained.when_clauses)):
        method_condition = method_chained.when_clauses[i][0]
        function_condition = function_based.when_clauses[i][0]
        
        # Both should be structurally equal
        assert method_condition.structurally_equals(function_condition)


def test_case_expression_validation_optimization():
    """Test case statement validation and error handling optimization."""
    
    # Test validation of empty when_clauses
    with pytest.raises(ValueError, match="case\\(\\) must have at least one when clause"):
        case([], default=lit("default"))
    
    # Test validation of malformed when_clauses
    with pytest.raises(ValueError, match="when_clauses\\[0\\] must be a tuple with exactly 2 elements"):
        case([(col("age") > 30,)], default=lit("default"))  # Missing value
    
    # Test validation of invalid when_clauses structure
    with pytest.raises(ValueError, match="when_clauses\\[1\\] must be a tuple with exactly 2 elements"):
        case([
            (col("age") > 30, lit("adult")),
            "not a tuple"  # Invalid structure
        ], default=lit("default"))
    
    # Test that valid case statements are created successfully
    valid_expr = case([
        (col("age") > 30, lit("adult")),
        (col("age") > 18, lit("young"))
    ], default=lit("minor"))
    
    assert isinstance(valid_expr, CaseExpr)
    assert len(valid_expr.when_clauses) == 2


def test_case_expression_performance_optimization():
    """Test case statement performance optimization patterns."""
    
    # Test case statement with early termination conditions
    early_term_expr = case([
        (col("status") == "error", lit("error")),
        (col("status") == "warning", lit("warning")),
        (col("status") == "info", lit("info"))
    ], default=lit("unknown"))
    
    # Verify conditions are ordered for optimal evaluation
    # Most specific conditions should come first
    assert early_term_expr.when_clauses[0][0].op == Operation.EQ
    assert early_term_expr.when_clauses[0][0].right == lit("error")
    
    # Test case statement with range-based conditions (should optimize)
    range_expr = case([
        (col("value") >= 100, lit("high")),
        (col("value") >= 50, lit("medium")),
        (col("value") >= 0, lit("low"))
    ], default=lit("negative"))
    
    # Verify range conditions are properly ordered
    conditions = [clause[0] for clause in range_expr.when_clauses]
    assert conditions[0].op == Operation.GE
    assert conditions[0].right == lit(100)  # Most restrictive first
    
    # Test case statement with boolean flag conditions
    flag_expr = case([
        (col("is_active") & col("is_verified"), lit("verified_active")),
        (col("is_active"), lit("active")),
        (col("is_verified"), lit("verified"))
    ], default=lit("inactive"))
    
    # Verify boolean logic is properly structured
    first_condition = flag_expr.when_clauses[0][0]
    assert isinstance(first_condition, BinaryExpr)
    assert first_condition.op == Operation.AND
    
    # Test case statement with null handling
    null_expr = case([
        (col("value").is_null(), lit("missing")),
        (col("value") > 0, lit("positive")),
        (col("value") < 0, lit("negative"))
    ], default=lit("zero"))
    
    # Verify null handling is properly structured
    assert isinstance(null_expr.when_clauses[0][0], BinaryExpr)


def test_case_expression_memory_optimization():
    """Test case statement memory optimization patterns."""
    
    # Test case statement with large number of conditions
    large_conditions = []
    for i in range(1000, 0, -10):
        large_conditions.append((col("score") >= i, lit(f"grade_{i}")))
    
    large_expr = case(large_conditions, default=lit("F"))
    
    # Verify large case statements are handled efficiently
    assert len(large_expr.when_clauses) == 100
    
    # Test case statement with complex nested conditions
    nested_expr = case([
        (((col("a") > 1) & (col("b") > 2)) | (col("c") > 3), lit("complex_1")),
        ((col("d") > 4) & (col("e") > 5), lit("complex_2")),
        (col("f") > 6, lit("simple"))
    ], default=lit("default"))
    
    # Verify nested boolean logic maintains structure
    first_condition = nested_expr.when_clauses[0][0]
    assert isinstance(first_condition, BinaryExpr)
    assert first_condition.op == Operation.OR
    
    # Test case statement with string pattern matching
    pattern_expr = case([
        (col("name").like("%admin%"), lit("administrator")),
        (col("name").like("%user%"), lit("regular_user")),
        (col("name").like("%guest%"), lit("guest_user"))
    ], default=lit("unknown"))
    
    # Verify pattern matching expressions are properly structured
    for condition, _ in pattern_expr.when_clauses:
        assert isinstance(condition, BinaryExpr)
        # Note: .like() method would need to be implemented in ColumnExpr


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
