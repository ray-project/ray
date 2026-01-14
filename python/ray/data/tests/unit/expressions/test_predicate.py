"""Tests for predicate expression operations.

This module tests:
- Null predicates: is_null(), is_not_null()
- Membership predicates: is_in(), not_in()
"""

import pandas as pd
import pytest

from ray.data._internal.planner.plan_expression.expression_evaluator import eval_expr
from ray.data.expressions import BinaryExpr, Operation, UnaryExpr, col, lit

# ──────────────────────────────────────
# Null Predicate Operations
# ──────────────────────────────────────


class TestIsNull:
    """Tests for is_null() predicate."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with null values for null predicate tests."""
        return pd.DataFrame(
            {
                "value": [1.0, None, 3.0, None, 5.0],
                "name": ["Alice", None, "Charlie", "Diana", None],
            }
        )

    def test_is_null_numeric(self, sample_data):
        """Test is_null on numeric column."""
        expr = col("value").is_null()
        assert isinstance(expr, UnaryExpr)
        assert expr.op == Operation.IS_NULL
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, False, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_null_string(self, sample_data):
        """Test is_null on string column."""
        expr = col("name").is_null()
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, False, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_null_structural_equality(self):
        """Test structural equality for is_null expressions."""
        expr1 = col("value").is_null()
        expr2 = col("value").is_null()
        expr3 = col("other").is_null()

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)


class TestIsNotNull:
    """Tests for is_not_null() predicate."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with null values."""
        return pd.DataFrame(
            {
                "value": [1.0, None, 3.0, None, 5.0],
                "name": ["Alice", None, "Charlie", "Diana", None],
            }
        )

    def test_is_not_null_numeric(self, sample_data):
        """Test is_not_null on numeric column."""
        expr = col("value").is_not_null()
        assert isinstance(expr, UnaryExpr)
        assert expr.op == Operation.IS_NOT_NULL
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_not_null_string(self, sample_data):
        """Test is_not_null on string column."""
        expr = col("name").is_not_null()
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_not_null_structural_equality(self):
        """Test structural equality for is_not_null expressions."""
        expr1 = col("value").is_not_null()
        expr2 = col("value").is_not_null()
        expr3 = col("other").is_not_null()

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)


class TestNullPredicateCombinations:
    """Tests for null predicates combined with other operations."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with null values and other columns."""
        return pd.DataFrame(
            {
                "value": [10.0, None, 30.0, None, 50.0],
                "threshold": [5.0, 20.0, 25.0, 10.0, 40.0],
            }
        )

    def test_null_aware_comparison(self, sample_data):
        """Test null-aware comparison (is_not_null AND comparison)."""
        # Filter: value is not null AND value > threshold
        expr = col("value").is_not_null() & (col("value") > col("threshold"))
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_null_or_condition(self, sample_data):
        """Test is_null combined with OR."""
        # value is null OR value > 40
        expr = col("value").is_null() | (col("value") > 40)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, False, True, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


# ──────────────────────────────────────
# Membership Predicate Operations
# ──────────────────────────────────────


class TestIsIn:
    """Tests for is_in() predicate."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for membership tests."""
        return pd.DataFrame(
            {
                "status": ["active", "inactive", "pending", "active", "deleted"],
                "category": ["A", "B", "C", "A", "D"],
                "value": [1, 2, 3, 4, 5],
            }
        )

    def test_is_in_string_list(self, sample_data):
        """Test is_in with string list."""
        expr = col("status").is_in(["active", "pending"])
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.IN
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_in_single_value_list(self, sample_data):
        """Test is_in with single-value list."""
        expr = col("status").is_in(["active"])
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, False, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_in_numeric_list(self, sample_data):
        """Test is_in with numeric list."""
        expr = col("value").is_in([1, 3, 5])
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_in_empty_list(self, sample_data):
        """Test is_in with empty list (should return all False)."""
        expr = col("status").is_in([])
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, False, False, False, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_in_with_literal_expr(self, sample_data):
        """Test is_in with LiteralExpr containing list."""
        values_expr = lit(["A", "C"])
        expr = col("category").is_in(values_expr)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_in_structural_equality(self):
        """Test structural equality for is_in expressions."""
        expr1 = col("status").is_in(["active", "pending"])
        expr2 = col("status").is_in(["active", "pending"])
        expr3 = col("status").is_in(["active"])

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)


class TestNotIn:
    """Tests for not_in() predicate."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for membership tests."""
        return pd.DataFrame(
            {
                "status": ["active", "inactive", "pending", "active", "deleted"],
                "value": [1, 2, 3, 4, 5],
            }
        )

    def test_not_in_string_list(self, sample_data):
        """Test not_in with string list."""
        expr = col("status").not_in(["inactive", "deleted"])
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.NOT_IN
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_not_in_numeric_list(self, sample_data):
        """Test not_in with numeric list."""
        expr = col("value").not_in([2, 4])
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_not_in_empty_list(self, sample_data):
        """Test not_in with empty list (should return all True)."""
        expr = col("status").not_in([])
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, True, True, True, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_not_in_structural_equality(self):
        """Test structural equality for not_in expressions."""
        expr1 = col("status").not_in(["deleted"])
        expr2 = col("status").not_in(["deleted"])
        expr3 = col("status").not_in(["deleted", "inactive"])

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)


class TestMembershipWithNulls:
    """Tests for membership predicates with null values."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with null values for membership tests."""
        return pd.DataFrame(
            {
                "status": ["active", None, "pending", None, "deleted"],
                "value": [1, None, 3, None, 5],
            }
        )

    def test_is_in_with_nulls_in_data(self, sample_data):
        """Test is_in when data contains nulls."""
        expr = col("status").is_in(["active", "pending"])
        result = eval_expr(expr, sample_data)
        # Nulls should return False (null is not in any list)
        expected = pd.Series([True, False, True, False, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_not_in_with_nulls_in_data(self, sample_data):
        """Test not_in when data contains nulls."""
        expr = col("status").not_in(["active"])
        result = eval_expr(expr, sample_data)
        # Nulls should return True (null is not in the exclusion list)
        expected = pd.Series([False, True, True, True, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


class TestMembershipCombinations:
    """Tests for membership predicates combined with other operations."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for combination tests."""
        return pd.DataFrame(
            {
                "status": ["active", "inactive", "pending", "active", "deleted"],
                "priority": ["high", "low", "high", "medium", "low"],
                "value": [100, 50, 75, 200, 25],
            }
        )

    def test_is_in_and_comparison(self, sample_data):
        """Test is_in combined with comparison."""
        # status in ["active", "pending"] AND value > 50
        expr = col("status").is_in(["active", "pending"]) & (col("value") > 50)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_multiple_is_in(self, sample_data):
        """Test multiple is_in predicates."""
        # status in ["active"] AND priority in ["high", "medium"]
        expr = col("status").is_in(["active"]) & col("priority").is_in(
            ["high", "medium"]
        )
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, False, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_is_in_or_not_in(self, sample_data):
        """Test is_in combined with not_in."""
        # status in ["active"] OR priority not_in ["low"]
        expr = col("status").is_in(["active"]) | col("priority").not_in(["low"])
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
