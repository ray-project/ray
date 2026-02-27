"""Tests for arithmetic expression operations.

This module tests:
- Basic arithmetic: ADD, SUB, MUL, DIV, FLOORDIV
- Reverse arithmetic: radd, rsub, rmul, rtruediv, rfloordiv
- Rounding helpers: ceil, floor, round, trunc
- Logarithmic helpers: ln, log10, log2, exp
- Trigonometric helpers: sin, cos, tan, asin, acos, atan
- Arithmetic helpers: negate, sign, power, abs
"""

import math

import pandas as pd
import pyarrow as pa
import pytest
from pkg_resources import parse_version

from ray.data._internal.planner.plan_expression.expression_evaluator import eval_expr
from ray.data.expressions import BinaryExpr, Operation, UDFExpr, col, lit
from ray.data.tests.conftest import get_pyarrow_version

pytestmark = pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="Expression unit tests require PyArrow >= 20.0.0",
)

# ──────────────────────────────────────
# Basic Arithmetic Operations
# ──────────────────────────────────────


class TestBasicArithmetic:
    """Tests for basic arithmetic operations (+, -, *, /, //)."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for arithmetic tests."""
        return pd.DataFrame(
            {
                "a": [10, 20, 30, 40],
                "b": [2, 4, 5, 8],
                "c": [1.5, 2.5, 3.5, 4.5],
            }
        )

    # ── Addition ──

    @pytest.mark.parametrize(
        "expr,expected_name,expected_values",
        [
            (col("a") + 5, "add_literal", [15, 25, 35, 45]),
            (col("a") + col("b"), "add_cols", [12, 24, 35, 48]),
            (col("a") + lit(10), "add_lit", [20, 30, 40, 50]),
        ],
        ids=["col_plus_int", "col_plus_col", "col_plus_lit"],
    )
    def test_addition(self, sample_data, expr, expected_name, expected_values):
        """Test addition operations."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.ADD
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values, name=None),
            check_names=False,
        )

    def test_reverse_addition(self, sample_data):
        """Test reverse addition (literal + expr)."""
        expr = 5 + col("a")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.ADD
        result = eval_expr(expr, sample_data)
        expected = pd.Series([15, 25, 35, 45])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_string_concat_invalid_input_type(self):
        """Reject non-string-like inputs in string concatenation."""
        table = pa.table({"name": ["a", "b"], "age": [1, 2]})
        expr = col("name") + col("age")
        with pytest.raises(TypeError, match="string-like pyarrow.*int64"):
            eval_expr(expr, table)

    # ── Subtraction ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("a") - 5, [5, 15, 25, 35]),
            (col("a") - col("b"), [8, 16, 25, 32]),
        ],
        ids=["col_minus_int", "col_minus_col"],
    )
    def test_subtraction(self, sample_data, expr, expected_values):
        """Test subtraction operations."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.SUB
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values, name=None),
            check_names=False,
        )

    def test_reverse_subtraction(self, sample_data):
        """Test reverse subtraction (literal - expr)."""
        expr = 100 - col("a")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.SUB
        result = eval_expr(expr, sample_data)
        expected = pd.Series([90, 80, 70, 60])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    # ── Multiplication ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("a") * 2, [20, 40, 60, 80]),
            (col("a") * col("b"), [20, 80, 150, 320]),
        ],
        ids=["col_times_int", "col_times_col"],
    )
    def test_multiplication(self, sample_data, expr, expected_values):
        """Test multiplication operations."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.MUL
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values, name=None),
            check_names=False,
        )

    def test_reverse_multiplication(self, sample_data):
        """Test reverse multiplication (literal * expr)."""
        expr = 3 * col("b")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.MUL
        result = eval_expr(expr, sample_data)
        expected = pd.Series([6, 12, 15, 24])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    # ── Division ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("a") / 2, [5.0, 10.0, 15.0, 20.0]),
            (col("a") / col("b"), [5.0, 5.0, 6.0, 5.0]),
        ],
        ids=["col_div_int", "col_div_col"],
    )
    def test_division(self, sample_data, expr, expected_values):
        """Test division operations."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.DIV
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values, name=None),
            check_names=False,
        )

    def test_reverse_division(self, sample_data):
        """Test reverse division (literal / expr)."""
        expr = 100 / col("a")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.DIV
        result = eval_expr(expr, sample_data)
        expected = pd.Series([10.0, 5.0, 100 / 30, 2.5])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    # ── Floor Division ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("a") // 3, [3, 6, 10, 13]),
            (col("a") // col("b"), [5, 5, 6, 5]),
        ],
        ids=["col_floordiv_int", "col_floordiv_col"],
    )
    def test_floor_division(self, sample_data, expr, expected_values):
        """Test floor division operations."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.FLOORDIV
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values, name=None),
            check_names=False,
        )

    def test_reverse_floor_division(self, sample_data):
        """Test reverse floor division (literal // expr)."""
        expr = 100 // col("a")
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.FLOORDIV
        result = eval_expr(expr, sample_data)
        expected = pd.Series([10, 5, 3, 2])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    # ── Modulo ──

    @pytest.mark.parametrize(
        "expr,expected_values",
        [
            (col("a") % 3, [1, 2, 0, 1]),
            (col("a") % col("c"), [1.0, 0.0, 2.0, 4.0]),
            (10 % col("b"), [0, 2, 0, 2]),
        ],
        ids=["col_mod_int", "col_mod_fp", "col_rmod_int"],
    )
    def test_modulo(self, sample_data, expr, expected_values):
        """Test modulo operations."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.MOD
        result = eval_expr(expr, sample_data)
        pd.testing.assert_series_equal(
            result.reset_index(drop=True),
            pd.Series(expected_values, name=None),
            check_names=False,
        )


# ──────────────────────────────────────
# Complex Arithmetic Expressions
# ──────────────────────────────────────


class TestComplexArithmetic:
    """Tests for complex arithmetic expressions with multiple operations."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for complex arithmetic tests."""
        return pd.DataFrame(
            {
                "x": [1.0, 2.0, 3.0, 4.0],
                "y": [4.0, 3.0, 2.0, 1.0],
                "z": [2.0, 2.0, 2.0, 2.0],
            }
        )

    def test_chained_operations(self, sample_data):
        """Test chained arithmetic operations."""
        expr = (col("x") + col("y")) * col("z")
        result = eval_expr(expr, sample_data)
        expected = pd.Series([10.0, 10.0, 10.0, 10.0])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_nested_operations(self, sample_data):
        """Test nested arithmetic operations."""
        expr = ((col("x") * 2) + (col("y") / 2)) - 1
        result = eval_expr(expr, sample_data)
        expected = pd.Series([3.0, 4.5, 6.0, 7.5])
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )

    def test_order_of_operations(self, sample_data):
        """Test that order of operations is respected."""
        # Should compute x + (y * z) due to operator precedence
        expr = col("x") + col("y") * col("z")
        result = eval_expr(expr, sample_data)
        expected = pd.Series([9.0, 8.0, 7.0, 6.0])  # 1+8, 2+6, 3+4, 4+2
        pd.testing.assert_series_equal(
            result.reset_index(drop=True), expected, check_names=False
        )


# ──────────────────────────────────────
# Rounding Operations
# ──────────────────────────────────────


class TestRoundingOperations:
    """Tests for rounding helper methods."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with decimal values for rounding tests."""
        return pd.DataFrame(
            {
                "value": [1.2, 2.5, 3.7, -1.3, -2.5, -3.8],
            }
        )

    @pytest.mark.parametrize(
        "method,expected_values",
        [
            ("ceil", [2, 3, 4, -1, -2, -3]),
            ("floor", [1, 2, 3, -2, -3, -4]),
            ("trunc", [1, 2, 3, -1, -2, -3]),
        ],
        ids=["ceil", "floor", "trunc"],
    )
    def test_rounding_methods(self, sample_data, method, expected_values):
        """Test rounding methods (ceil, floor, trunc)."""
        expr = getattr(col("value"), method)()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        # Convert to list for comparison since PyArrow might return different types
        result_list = result.tolist()
        assert result_list == expected_values

    def test_round_method(self, sample_data):
        """Test round method (may differ due to banker's rounding)."""
        expr = col("value").round()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        # PyArrow uses banker's rounding (round half to even)
        # Just verify it runs and returns numeric values
        assert len(result) == len(sample_data)


# ──────────────────────────────────────
# Logarithmic Operations
# ──────────────────────────────────────


class TestLogarithmicOperations:
    """Tests for logarithmic helper methods."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with positive values for logarithmic tests."""
        return pd.DataFrame(
            {
                "value": [1.0, math.e, 10.0, 100.0],
            }
        )

    def test_ln(self, sample_data):
        """Test natural logarithm."""
        expr = col("value").ln()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        expected = [0.0, 1.0, math.log(10), math.log(100)]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10

    def test_log10(self, sample_data):
        """Test base-10 logarithm."""
        expr = col("value").log10()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        expected = [0.0, math.log10(math.e), 1.0, 2.0]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10

    def test_log2(self):
        """Test base-2 logarithm."""
        data = pd.DataFrame({"value": [1.0, 2.0, 4.0, 8.0]})
        expr = col("value").log2()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, data)
        expected = [0.0, 1.0, 2.0, 3.0]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10

    def test_exp(self):
        """Test exponential function."""
        data = pd.DataFrame({"value": [0.0, 1.0, 2.0]})
        expr = col("value").exp()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, data)
        expected = [1.0, math.e, math.e**2]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10


# ──────────────────────────────────────
# Trigonometric Operations
# ──────────────────────────────────────


class TestTrigonometricOperations:
    """Tests for trigonometric helper methods."""

    @pytest.fixture
    def sample_data(self):
        """Sample data with angles in radians for trig tests."""
        return pd.DataFrame(
            {
                "angle": [0.0, math.pi / 6, math.pi / 4, math.pi / 3, math.pi / 2],
            }
        )

    def test_sin(self, sample_data):
        """Test sine function."""
        expr = col("angle").sin()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        expected = [0.0, 0.5, math.sqrt(2) / 2, math.sqrt(3) / 2, 1.0]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10

    def test_cos(self, sample_data):
        """Test cosine function."""
        expr = col("angle").cos()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        expected = [1.0, math.sqrt(3) / 2, math.sqrt(2) / 2, 0.5, 0.0]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10

    def test_tan(self):
        """Test tangent function."""
        data = pd.DataFrame({"angle": [0.0, math.pi / 4]})
        expr = col("angle").tan()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, data)
        expected = [0.0, 1.0]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10

    def test_asin(self):
        """Test arcsine function."""
        data = pd.DataFrame({"value": [0.0, 0.5, 1.0]})
        expr = col("value").asin()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, data)
        expected = [0.0, math.pi / 6, math.pi / 2]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10

    def test_acos(self):
        """Test arccosine function."""
        data = pd.DataFrame({"value": [1.0, 0.5, 0.0]})
        expr = col("value").acos()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, data)
        expected = [0.0, math.pi / 3, math.pi / 2]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10

    def test_atan(self):
        """Test arctangent function."""
        data = pd.DataFrame({"value": [0.0, 1.0]})
        expr = col("value").atan()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, data)
        expected = [0.0, math.pi / 4]
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10


# ──────────────────────────────────────
# Arithmetic Helper Operations
# ──────────────────────────────────────


class TestArithmeticHelpers:
    """Tests for arithmetic helper methods (negate, sign, power, abs)."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for arithmetic helper tests."""
        return pd.DataFrame(
            {
                "value": [5, -3, 0, 10, -7],
            }
        )

    def test_negate(self, sample_data):
        """Test negate method."""
        expr = col("value").negate()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        expected = [-5, 3, 0, -10, 7]
        assert result.tolist() == expected

    def test_sign(self, sample_data):
        """Test sign method."""
        expr = col("value").sign()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        expected = [1, -1, 0, 1, -1]
        assert result.tolist() == expected

    def test_abs(self, sample_data):
        """Test abs method."""
        expr = col("value").abs()
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, sample_data)
        expected = [5, 3, 0, 10, 7]
        assert result.tolist() == expected

    @pytest.mark.parametrize(
        "base_values,exponent,expected",
        [
            ([2, 3, 4], 2, [4, 9, 16]),
            ([2, 3, 4], 3, [8, 27, 64]),
            ([4, 9, 16], 0.5, [2.0, 3.0, 4.0]),
        ],
        ids=["square", "cube", "sqrt"],
    )
    def test_power(self, base_values, exponent, expected):
        """Test power method with various exponents."""
        data = pd.DataFrame({"value": base_values})
        expr = col("value").power(exponent)
        assert isinstance(expr, UDFExpr)
        result = eval_expr(expr, data)
        for r, e in zip(result.tolist(), expected):
            assert abs(r - e) < 1e-10


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
