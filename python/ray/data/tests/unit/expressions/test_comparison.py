"""Tests for comparison expression operations.

This module tests:
- Comparison operators: GT (>), LT (<), GE (>=), LE (<=), EQ (==), NE (!=)
- Comparison with columns and literals
- Reverse comparisons (literal compared to column)
"""

import pandas as pd
import pytest

from ray.data._internal.planner.plan_expression.expression_evaluator import eval_expr
from ray.data.expressions import BinaryExpr, Operation, col, lit

# ──────────────────────────────────────
# Basic Comparison Operations
# ──────────────────────────────────────


class TestComparisonOperators:
    """Tests for comparison operators (>, <, >=, <=, ==, !=)."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for comparison tests."""
        return pd.DataFrame(
            {
                "age": [18, 21, 25, 30, 16],
                "score": [50, 75, 100, 60, 85],
                "status": ["active", "inactive", "active", "pending", "active"],
            }
        )

    # ── Greater Than ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("age") > 21, [False, False, True, True, False]),
            (col("age") > col("score") / 10, [True, True, True, True, True]),
        ],
        ids=["col_gt_literal", "col_gt_col_expr"],
    )
    def test_greater_than(self, sample_data, expr, expected_values):
        """Test greater than (>) comparisons."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.GT
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values),
            check_names=False,
        )

    def test_greater_than_reverse(self, sample_data):
        """Test reverse greater than (literal > col)."""
        expr = 22 > col("age")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.LT  # Reverse: 22 > age becomes age < 22
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, True, False, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    # ── Less Than ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("age") < 21, [True, False, False, False, True]),
            (col("score") < 70, [True, False, False, True, False]),
        ],
        ids=["col_lt_literal", "score_lt_70"],
    )
    def test_less_than(self, sample_data, expr, expected_values):
        """Test less than (<) comparisons."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.LT
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values),
            check_names=False,
        )

    def test_less_than_reverse(self, sample_data):
        """Test reverse less than (literal < col)."""
        expr = 20 < col("age")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.GT  # Reverse: 20 < age becomes age > 20
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    # ── Greater Than or Equal ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("age") >= 21, [False, True, True, True, False]),
            (col("score") >= 75, [False, True, True, False, True]),
        ],
        ids=["col_ge_21", "score_ge_75"],
    )
    def test_greater_equal(self, sample_data, expr, expected_values):
        """Test greater than or equal (>=) comparisons."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.GE
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values),
            check_names=False,
        )

    def test_greater_equal_reverse(self, sample_data):
        """Test reverse greater equal (literal >= col)."""
        expr = 21 >= col("age")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.LE  # Reverse
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, True, False, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    # ── Less Than or Equal ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("age") <= 21, [True, True, False, False, True]),
            (col("score") <= 60, [True, False, False, True, False]),
        ],
        ids=["col_le_21", "score_le_60"],
    )
    def test_less_equal(self, sample_data, expr, expected_values):
        """Test less than or equal (<=) comparisons."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.LE
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values),
            check_names=False,
        )

    def test_less_equal_reverse(self, sample_data):
        """Test reverse less equal (literal <= col)."""
        expr = 25 <= col("age")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.GE  # Reverse
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, False, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    # ── Equality ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("age") == 21, [False, True, False, False, False]),
            (col("status") == "active", [True, False, True, False, True]),
            (col("score") == lit(100), [False, False, True, False, False]),
        ],
        ids=["age_eq_21", "status_eq_active", "score_eq_100"],
    )
    def test_equality(self, sample_data, expr, expected_values):
        """Test equality (==) comparisons."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.EQ
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values),
            check_names=False,
        )

    # ── Not Equal ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("age") != 21, [True, False, True, True, True]),
            (col("status") != "active", [False, True, False, True, False]),
        ],
        ids=["age_ne_21", "status_ne_active"],
    )
    def test_not_equal(self, sample_data, expr, expected_values):
        """Test not equal (!=) comparisons."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.NE
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values),
            check_names=False,
        )


# ──────────────────────────────────────
# Column vs Column Comparisons
# ──────────────────────────────────────


class TestColumnToColumnComparison:
    """Tests for comparing columns against other columns."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with comparable columns."""
        return pd.DataFrame(
            {
                "value_a": [10, 20, 30, 40],
                "value_b": [15, 20, 25, 45],
                "threshold": [12, 18, 35, 35],
            }
        )

    @pytest.mark.parametrize(
        "expr_fn,expected_values",
        [
            (lambda: col("value_a") > col("value_b"), [False, False, True, False]),
            (lambda: col("value_a") < col("threshold"), [True, False, True, False]),
            (lambda: col("value_a") == col("value_b"), [False, True, False, False]),
            (lambda: col("value_a") >= col("threshold"), [False, True, False, True]),
        ],
        ids=["a_gt_b", "a_lt_threshold", "a_eq_b", "a_ge_threshold"],
    )
    def test_column_to_column_comparisons(self, sample_data, expr_fn, expected_values):
        """Test various column-to-column comparisons."""
        expr = expr_fn()
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values),
            check_names=False,
        )


# ──────────────────────────────────────
# Comparison with Expressions
# ──────────────────────────────────────


class TestComparisonWithExpressions:
    """Tests for comparing expressions against other expressions."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for expression comparison tests."""
        return pd.DataFrame(
            {
                "price": [100, 200, 150],
                "discount": [10, 50, 30],
                "min_price": [80, 160, 130],
            }
        )

    def test_compare_computed_values(self, sample_data):
        """Test comparing computed expression results."""
        # (price - discount) > min_price
        expr = (col("price") - col("discount")) > col("min_price")
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, False])  # 90>80, 150>160, 120>130
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_compare_scaled_values(self, sample_data):
        """Test comparing scaled column values."""
        # price * 0.9 >= min_price (check if 10% discount still meets minimum)
        expr = col("price") * 0.9 >= col("min_price")
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, True, True])  # 90>=80, 180>=160, 135>=130
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


# ──────────────────────────────────────
# String Comparisons
# ──────────────────────────────────────


class TestStringComparison:
    """Tests for string equality and inequality."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with string columns."""
        return pd.DataFrame(
            {
                "name": ["Alice", "Bob", "Charlie", "Alice"],
                "city": ["NYC", "LA", "NYC", "SF"],
            }
        )

    def test_string_equality(self, sample_data):
        """Test string equality comparison."""
        expr = col("name") == "Alice"
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_string_inequality(self, sample_data):
        """Test string inequality comparison."""
        expr = col("city") != "NYC"
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


# ──────────────────────────────────────
# Boolean Comparisons
# ──────────────────────────────────────


class TestBooleanComparison:
    """Tests for boolean value comparisons."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with boolean columns."""
        return pd.DataFrame(
            {
                "is_active": [True, False, True, False],
                "is_verified": [True, True, False, False],
            }
        )

    def test_boolean_equality_true(self, sample_data):
        """Test boolean equality with True."""
        expr = col("is_active") == lit(True)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_boolean_equality_false(self, sample_data):
        """Test boolean equality with False."""
        expr = col("is_active") == lit(False)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_boolean_column_to_column(self, sample_data):
        """Test comparing two boolean columns."""
        expr = col("is_active") == col("is_verified")
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
