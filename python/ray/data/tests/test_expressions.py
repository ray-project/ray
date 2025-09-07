import pytest

from ray.data.expressions import (
    Expr,
    col,
    lit,
    where,
    PredicateExpr,
    UnaryExpr,
    BinaryExpr,
    Operation,
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


class TestUnaryExpressions:
    """Test unary expression functionality."""

    @pytest.mark.parametrize(
        "expr, expected_op",
        [
            (col("age").is_null(), Operation.IS_NULL),
            (col("name").is_not_null(), Operation.IS_NOT_NULL),
            (~col("active"), Operation.NOT),
        ],
        ids=["is_null", "is_not_null", "not"],
    )
    def test_unary_operations(self, expr, expected_op):
        """Test that unary operations create correct UnaryExpr."""
        assert isinstance(expr, UnaryExpr)
        assert expr.op == expected_op
        assert isinstance(expr.operand, Expr)

    def test_unary_structural_equality(self):
        """Test structural equality for unary expressions."""
        # Same expressions should be equal
        assert col("age").is_null().structurally_equals(col("age").is_null())
        assert (
            col("active").is_not_null().structurally_equals(col("active").is_not_null())
        )
        assert (~col("flag")).structurally_equals(~col("flag"))

        # Different operations should not be equal
        assert not col("age").is_null().structurally_equals(col("age").is_not_null())

        # Different operands should not be equal
        assert not col("age").is_null().structurally_equals(col("name").is_null())


class TestBinaryExpressions:
    """Test enhanced binary expression functionality."""

    @pytest.mark.parametrize(
        "expr, expected_op",
        [
            (col("age") != lit(25), Operation.NE),
            (col("status").isin(["active", "pending"]), Operation.IN),
            (col("status").not_in(["inactive", "deleted"]), Operation.NOT_IN),
        ],
        ids=["not_equal", "isin", "not_in"],
    )
    def test_new_binary_operations(self, expr, expected_op):
        """Test new binary operations."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == expected_op

    def test_isin_with_list(self):
        """Test isin with list of values."""
        expr = col("status").isin(["active", "pending", "completed"])
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.IN
        # The right operand should be a LiteralExpr containing the list
        assert expr.right.value == ["active", "pending", "completed"]

    def test_isin_with_expr(self):
        """Test isin with expression."""
        values_expr = lit(["a", "b", "c"])
        expr = col("category").isin(values_expr)
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.IN
        assert expr.right == values_expr


class TestFilterFunction:
    """Test the where() function for creating predicates."""

    @pytest.mark.parametrize(
        "condition",
        [
            col("age") > lit(18),
            col("status") == lit("active"),
            col("name").is_not_null(),
            (col("age") >= lit(21)) & (col("country") == lit("USA")),
        ],
        ids=["simple_gt", "simple_eq", "is_not_null", "complex_and"],
    )
    def test_filter_creates_predicate_expr(self, condition):
        """Test that where() creates PredicateExpr with correct condition."""
        predicate = where(condition)
        assert isinstance(predicate, PredicateExpr)
        assert predicate.condition.structurally_equals(condition)

    def test_predicate_combination(self):
        """Test combining predicates with logical operators."""
        pred1 = where(col("age") > 18)
        pred2 = where(col("status") == "active")

        # Test AND combination
        combined_and = pred1 & pred2
        assert isinstance(combined_and, PredicateExpr)

        # Test OR combination
        combined_or = pred1 | pred2
        assert isinstance(combined_or, PredicateExpr)

        # Test NOT operation
        negated = ~pred1
        assert isinstance(negated, PredicateExpr)

    def test_predicate_structural_equality(self):
        """Test structural equality for predicates."""
        pred1 = where(col("age") > 18)
        pred2 = where(col("age") > 18)
        pred3 = where(col("age") > 21)

        assert pred1.structurally_equals(pred2)
        assert not pred1.structurally_equals(pred3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
