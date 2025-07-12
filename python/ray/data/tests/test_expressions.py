import pytest

from ray.data.expressions import Expr, col, lit

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
]


@pytest.mark.parametrize(
    "expr1, expr2, expected",
    STRUCTURAL_EQUALITY_TEST_CASES,
    ids=[f"{i}" for i in range(len(STRUCTURAL_EQUALITY_TEST_CASES))],
)
def test_structural_equality(expr1, expr2, expected):
    """Tests `structurally_equals` for various expression trees."""
    assert expr1.structurally_equals(expr2) is expected
    # Test for symmetry
    assert expr2.structurally_equals(expr1) is expected


def test_operator_eq_is_not_structural_eq():
    """
    Confirms that `__eq__` (==) builds an expression, while
    `structurally_equals` compares two existing expressions.
    """
    # `==` returns a BinaryExpr, not a boolean
    op_eq_expr = col("a") == col("a")
    assert isinstance(op_eq_expr, Expr)
    assert not isinstance(op_eq_expr, bool)

    # `structurally_equals` returns a boolean
    struct_eq_result = col("a").structurally_equals(col("a"))
    assert isinstance(struct_eq_result, bool)
    assert struct_eq_result is True


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
