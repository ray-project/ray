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
    # Alias expression tests
    (col("a").alias("b"), col("a").alias("b"), True),
    (col("a").alias("b"), col("a").alias("c"), False),  # Different alias
    (col("a").alias("b"), col("b").alias("b"), False),  # Different column
    ((col("a") + 1).alias("result"), (col("a") + 1).alias("result"), True),
    (
        (col("a") + 1).alias("result"),
        (col("a") + 2).alias("result"),
        False,
    ),  # Different expr
    (col("a").alias("b"), col("a"), False),  # Alias vs non-alias
]


# Parametrized test cases for alias functionality
ALIAS_TEST_CASES = [
    # (expression, alias_name, expected_alias, should_match_original)
    (col("price"), "product_price", "product_price", True),
    (lit(42), "answer", "answer", True),
    (col("a") + col("b"), "sum", "sum", True),
    ((col("price") * col("qty")) + lit(5), "total_with_fee", "total_with_fee", True),
    (col("age") >= lit(18), "is_adult", "is_adult", True),
]


@pytest.mark.parametrize(
    "expr, alias_name, expected_alias, should_match_original",
    ALIAS_TEST_CASES,
    ids=["col_alias", "lit_alias", "binary_alias", "complex_alias", "comparison_alias"],
)
def test_alias_functionality(expr, alias_name, expected_alias, should_match_original):
    """Test alias functionality with various expression types."""
    import pandas as pd

    from ray.data._expression_evaluator import eval_expr
    from ray.data.expressions import AliasExpr

    # Test alias creation
    aliased_expr = expr.alias(alias_name)
    assert isinstance(aliased_expr, AliasExpr)
    assert aliased_expr.alias == expected_alias
    assert aliased_expr.expr.structurally_equals(expr)

    # Test data type preservation
    assert aliased_expr.data_type == expr.data_type

    # Test evaluation equivalence (if we can create test data)
    if should_match_original:
        test_data = pd.DataFrame(
            {
                "price": [10, 20],
                "qty": [2, 3],
                "a": [1, 2],
                "b": [3, 4],
                "age": [17, 25],
            }
        )
        try:
            original_result = eval_expr(expr, test_data)
            aliased_result = eval_expr(aliased_expr, test_data)
            if hasattr(original_result, "equals"):  # For pandas Series
                assert original_result.equals(aliased_result)
            else:  # For scalars
                assert original_result == aliased_result
        except (KeyError, TypeError):
            # Skip evaluation test if columns don't exist in test data
            pass


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
