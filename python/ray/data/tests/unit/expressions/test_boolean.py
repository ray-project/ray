"""Tests for boolean/logical expression operations.

This module tests:
- Logical operators: AND (&), OR (|), NOT (~)
- Boolean expression combinations
- Complex nested boolean expressions
"""

import pandas as pd
import pytest

from ray.data._internal.planner.plan_expression.expression_evaluator import eval_expr
from ray.data.expressions import BinaryExpr, Operation, UnaryExpr, col, lit

# ──────────────────────────────────────
# Logical AND Operations
# ──────────────────────────────────────


class TestLogicalAnd:
    """Tests for logical AND (&) operations."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for logical AND tests."""
        return pd.DataFrame(
            {
                "is_active": [True, True, False, False],
                "is_verified": [True, False, True, False],
                "age": [25, 17, 30, 15],
            }
        )

    def test_and_two_booleans(self, sample_data):
        """Test AND of two boolean columns."""
        expr = col("is_active") & col("is_verified")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.AND
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, False, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_and_two_comparisons(self, sample_data):
        """Test AND of two comparison expressions."""
        expr = (col("is_active") == lit(True)) & (col("age") >= 18)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, False, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_and_chained(self, sample_data):
        """Test chained AND operations."""
        expr = (col("is_active")) & (col("is_verified")) & (col("age") >= 18)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, False, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


# ──────────────────────────────────────
# Logical OR Operations
# ──────────────────────────────────────


class TestLogicalOr:
    """Tests for logical OR (|) operations."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for logical OR tests."""
        return pd.DataFrame(
            {
                "is_admin": [True, False, False, False],
                "is_moderator": [False, True, False, False],
                "age": [25, 17, 30, 15],
            }
        )

    def test_or_two_booleans(self, sample_data):
        """Test OR of two boolean columns."""
        expr = col("is_admin") | col("is_moderator")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.OR
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, True, False, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_or_two_comparisons(self, sample_data):
        """Test OR of two comparison expressions."""
        expr = (col("is_admin") == lit(True)) | (col("age") >= 18)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_or_chained(self, sample_data):
        """Test chained OR operations."""
        expr = (col("is_admin")) | (col("is_moderator")) | (col("age") >= 21)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, True, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


# ──────────────────────────────────────
# Logical NOT Operations
# ──────────────────────────────────────


class TestLogicalNot:
    """Tests for logical NOT (~) operations."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for logical NOT tests."""
        return pd.DataFrame(
            {
                "is_active": [True, False, True, False],
                "is_banned": [False, False, True, True],
                "age": [25, 17, 30, 15],
            }
        )

    def test_not_boolean_column(self, sample_data):
        """Test NOT of a boolean column."""
        expr = ~col("is_active")
        assert isinstance(expr, UnaryExpr)
        assert expr.op == Operation.NOT
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_not_comparison(self, sample_data):
        """Test NOT of a comparison expression."""
        expr = ~(col("age") >= 18)
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_double_negation(self, sample_data):
        """Test double negation (~~)."""
        expr = ~~col("is_active")
        result = eval_expr(expr, sample_data)
        expected = pd.Series([True, False, True, False])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


# ──────────────────────────────────────
# Complex Boolean Combinations
# ──────────────────────────────────────


class TestComplexBooleanExpressions:
    """Tests for complex boolean expression combinations."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for complex boolean tests."""
        return pd.DataFrame(
            {
                "age": [17, 21, 25, 30, 65],
                "is_student": [True, True, False, False, False],
                "is_member": [False, True, True, False, True],
                "country": ["USA", "UK", "USA", "Canada", "USA"],
            }
        )

    def test_and_or_combination(self, sample_data):
        """Test combination of AND and OR."""
        # (age >= 21) AND (is_student OR is_member)
        expr = (col("age") >= 21) & (col("is_student") | col("is_member"))
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_not_with_and_or(self, sample_data):
        """Test NOT combined with AND and OR."""
        # NOT(age < 18) AND (is_member OR is_student)
        expr = ~(col("age") < 18) & (col("is_member") | col("is_student"))
        result = eval_expr(expr, sample_data)
        expected = pd.Series([False, True, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_demorgans_law_and(self, sample_data):
        """Test De Morgan's law: ~(A & B) == (~A) | (~B)."""
        # ~(is_student & is_member)
        expr1 = ~(col("is_student") & col("is_member"))
        # (~is_student) | (~is_member)
        expr2 = (~col("is_student")) | (~col("is_member"))

        result1 = eval_expr(expr1, sample_data)
        result2 = eval_expr(expr2, sample_data)

        pd.testing.assert_series_equal(
            result1.reset_index(drop=True),
            result2.reset_index(drop=True),
            check_names=False,
        )

    def test_demorgans_law_or(self, sample_data):
        """Test De Morgan's law: ~(A | B) == (~A) & (~B)."""
        # ~(is_student | is_member)
        expr1 = ~(col("is_student") | col("is_member"))
        # (~is_student) & (~is_member)
        expr2 = (~col("is_student")) & (~col("is_member"))

        result1 = eval_expr(expr1, sample_data)
        result2 = eval_expr(expr2, sample_data)

        pd.testing.assert_series_equal(
            result1.reset_index(drop=True),
            result2.reset_index(drop=True),
            check_names=False,
        )

    def test_deeply_nested_boolean(self, sample_data):
        """Test deeply nested boolean expression."""
        # ((age >= 21) & (country == "USA")) | ((is_student) & (is_member))
        expr = ((col("age") >= 21) & (col("country") == "USA")) | (
            (col("is_student")) & (col("is_member"))
        )
        result = eval_expr(expr, sample_data)
        # Row 0: (17>=21 & USA) | (True & False) = False | False = False
        # Row 1: (21>=21 & UK) | (True & True) = False | True = True
        # Row 2: (25>=21 & USA) | (False & True) = True | False = True
        # Row 3: (30>=21 & Canada) | (False & False) = False | False = False
        # Row 4: (65>=21 & USA) | (False & True) = True | False = True
        expected = pd.Series([False, True, True, False, True])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


# ──────────────────────────────────────
# Boolean Expression Structural Equality
# ──────────────────────────────────────


class TestBooleanStructuralEquality:
    """Tests for structural equality of boolean expressions."""

    def test_and_structural_equality(self):
        """Test structural equality for AND expressions."""
        expr1 = col("a") & col("b")
        expr2 = col("a") & col("b")
        expr3 = col("b") & col("a")  # Order matters

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)

    def test_or_structural_equality(self):
        """Test structural equality for OR expressions."""
        expr1 = col("a") | col("b")
        expr2 = col("a") | col("b")
        expr3 = col("a") | col("c")

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)

    def test_not_structural_equality(self):
        """Test structural equality for NOT expressions."""
        expr1 = ~col("a")
        expr2 = ~col("a")
        expr3 = ~col("b")

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)

    def test_complex_boolean_structural_equality(self):
        """Test structural equality for complex boolean expressions."""
        expr1 = (col("a") > 10) & ((col("b") < 5) | ~col("c"))
        expr2 = (col("a") > 10) & ((col("b") < 5) | ~col("c"))
        expr3 = (col("a") > 10) & ((col("b") < 6) | ~col("c"))

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
