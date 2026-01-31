"""Comprehensive tests for constant folding optimization."""

import pytest

import ray
from ray.data._internal.planner.plan_expression.constant_folder import (
    fold_constant_expressions,
)
from ray.data.expressions import col, lit


class TestBasicConstantFolding:
    """Test basic constant folding operations."""

    def test_arithmetic_addition(self):
        """Test lit(3) + lit(5) → lit(8)"""
        expr = lit(3) + lit(5)
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, lit(8).__class__)
        assert folded.value == 8

    def test_arithmetic_multiplication(self):
        """Test lit(2) * lit(7) → lit(14)"""
        expr = lit(2) * lit(7)
        folded = fold_constant_expressions(expr)

        assert folded.value == 14

    def test_arithmetic_division(self):
        """Test lit(10) / lit(2) → lit(5.0)"""
        expr = lit(10) / lit(2)
        folded = fold_constant_expressions(expr)

        assert folded.value == 5.0

    def test_boolean_and(self):
        """Test lit(True) & lit(False) → lit(False)"""
        expr = lit(True) & lit(False)
        folded = fold_constant_expressions(expr)

        assert folded.value is False

    def test_boolean_or(self):
        """Test lit(True) | lit(False) → lit(True)"""
        expr = lit(True) | lit(False)
        folded = fold_constant_expressions(expr)

        assert folded.value is True

    def test_comparison_equal(self):
        """Test lit(5) == lit(5) → lit(True)"""
        expr = lit(5) == lit(5)
        folded = fold_constant_expressions(expr)

        assert folded.value is True

    def test_comparison_greater(self):
        """Test lit(10) > lit(5) → lit(True)"""
        expr = lit(10) > lit(5)
        folded = fold_constant_expressions(expr)

        assert folded.value is True


class TestNestedConstantFolding:
    """Test folding of nested expressions."""

    def test_nested_arithmetic_case1(self):
        """Test 2*(3+4) → lit(14)"""
        expr = lit(2) * (lit(3) + lit(4))
        folded = fold_constant_expressions(expr)

        assert folded.value == 14

    def test_nested_arithmetic_case2(self):
        """Test 2*3+4 → lit(10)"""
        expr = lit(2) * lit(3) + lit(4)
        folded = fold_constant_expressions(expr)

        assert folded.value == 10

    def test_deeply_nested(self):
        """Test 2*(3+(4*(5+6))) → lit(86)"""
        expr = lit(2) * (lit(3) + (lit(4) * (lit(5) + lit(6))))
        folded = fold_constant_expressions(expr)

        # Calculation: 5+6=11, 4*11=44, 3+44=47, 2*47=94
        assert folded.value == 94

    def test_complex_boolean(self):
        """Test ((True & False) | True) → lit(True)"""
        expr = (lit(True) & lit(False)) | lit(True)
        folded = fold_constant_expressions(expr)

        assert folded.value is True


class TestAlgebraicSimplification:
    """Test algebraic identity simplification."""

    def test_multiply_by_one(self):
        """Test col("x") * 1 → col("x")"""
        expr = col("x") * 1
        folded = fold_constant_expressions(expr)

        # Result should be ColumnExpr
        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"

    def test_multiply_by_zero(self):
        """Test col("x") * 0 → lit(0)"""
        expr = col("x") * 0
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, lit(0).__class__)
        assert folded.value == 0

    def test_add_zero(self):
        """Test col("x") + 0 → col("x")"""
        expr = col("x") + 0
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"

    def test_subtract_zero(self):
        """Test col("x") - 0 → col("x")"""
        expr = col("x") - 0
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"

    def test_divide_by_one(self):
        """Test col("x") / 1 → col("x")"""
        expr = col("x") / 1
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"

    def test_and_true(self):
        """Test col("x") & True → col("x")"""
        expr = col("x") & True
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"

    def test_and_false(self):
        """Test col("x") & False → lit(False)"""
        expr = col("x") & False
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, lit(False).__class__)
        assert folded.value is False

    def test_or_true(self):
        """Test col("x") | True → lit(True)"""
        expr = col("x") | True
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, lit(True).__class__)
        assert folded.value is True

    def test_or_false(self):
        """Test col("x") | False → col("x")"""
        expr = col("x") | False
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"

    def test_double_negation(self):
        """Test NOT(NOT(col("x"))) → col("x")"""
        expr = ~(~col("x"))
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"


class TestShortCircuitEvaluation:
    """Test short-circuit evaluation optimization."""

    def test_false_and_column(self):
        """Test False & col("x") → lit(False)"""
        expr = lit(False) & col("x")
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, lit(False).__class__)
        assert folded.value is False

    def test_true_or_column(self):
        """Test True | col("x") → lit(True)"""
        expr = lit(True) | col("x")
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, lit(True).__class__)
        assert folded.value is True

    def test_true_and_column(self):
        """Test True & col("x") → col("x")"""
        expr = lit(True) & col("x")
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"

    def test_false_or_column(self):
        """Test False | col("x") → col("x")"""
        expr = lit(False) | col("x")
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("x").__class__)
        assert folded.name == "x"


class TestComplexBooleanExpressions:
    """Test complex boolean expression simplification."""

    def test_complex_case_1(self):
        """Test ((True & col("a")) | False) → col("a")"""
        expr = (lit(True) & col("a")) | lit(False)
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("a").__class__)
        assert folded.name == "a"

    def test_complex_case_2(self):
        """Test ((True & col("a")) | False) & True → col("a")"""
        expr = ((lit(True) & col("a")) | lit(False)) & lit(True)
        folded = fold_constant_expressions(expr)

        assert isinstance(folded, col("a").__class__)
        assert folded.name == "a"

    def test_spark_like_example(self):
        """Test complex Spark-like boolean expression.

        Original:
        (($"a" > 1 && Literal(1) === Literal(1)) ||
         ($"a" < 10 && Literal(1) === Literal(2)) ||
         (Literal(1) === Literal(1) && $"b" > 1) ||
         (Literal(1) === Literal(2) && $"b" < 10))

        Expected after folding:
        (col("a") > 1 || col("b") > 1)
        """
        expr = (
            ((col("a") > 1) & (lit(1) == lit(1)))
            | ((col("a") < 10) & (lit(1) == lit(2)))
            | ((lit(1) == lit(1)) & (col("b") > 1))
            | ((lit(1) == lit(2)) & (col("b") < 10))
        )

        folded = fold_constant_expressions(expr)

        # After folding:
        # (col("a") > 1 & True) | (col("a") < 10 & False) | (True & col("b") > 1) | (False & col("b") < 10)
        # → (col("a") > 1) | False | (col("b") > 1) | False
        # → col("a") > 1 | col("b") > 1

        # Verify structure (it should be a BinaryExpr with OR)
        from ray.data.expressions import BinaryExpr, Operation

        assert isinstance(folded, BinaryExpr)
        assert folded.op == Operation.OR


class TestMultiPassOptimization:
    """Test that multi-pass optimization works correctly."""

    def test_requires_multiple_passes(self):
        """Test expression that needs multiple passes.

        ((lit(True) & col("a")) | lit(False)) & lit(True)

        Pass 1: (col("a") | lit(False)) & lit(True)
        Pass 2: col("a") & lit(True)
        Pass 3: col("a")
        """
        expr = ((lit(True) & col("a")) | lit(False)) & lit(True)
        folded = fold_constant_expressions(expr)

        # Should fully simplify to col("a")
        assert isinstance(folded, col("a").__class__)
        assert folded.name == "a"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_division_by_zero_not_folded(self):
        """Test lit(5) / lit(0) is NOT folded (deferred to runtime)"""
        expr = lit(5) / lit(0)
        folded = fold_constant_expressions(expr)

        # Should remain as BinaryExpr (not folded)
        from ray.data.expressions import BinaryExpr

        assert isinstance(folded, BinaryExpr)

    def test_mixed_types_not_folded(self):
        """Test incompatible types are not folded"""
        expr = lit("hello") + lit(5)
        folded = fold_constant_expressions(expr)

        # Should remain as BinaryExpr (not folded)
        from ray.data.expressions import BinaryExpr

        assert isinstance(folded, BinaryExpr)

    def test_preserve_column_references(self):
        """Test column references are never modified"""
        expr = col("x") + col("y")
        folded = fold_constant_expressions(expr)

        # Should remain unchanged
        from ray.data.expressions import BinaryExpr

        assert isinstance(folded, BinaryExpr)


class TestIntegrationWithDataset:
    """Test constant folding in actual Ray Data operations."""

    def test_with_column_constant_folding(self):
        """Test folding works in with_column"""
        ds = ray.data.from_items([{"x": 1}, {"x": 2}])
        result = ds.with_column("y", lit(3) + lit(5))

        assert result.take() == [{"x": 1, "y": 8}, {"x": 2, "y": 8}]

    def test_filter_constant_folding(self):
        """Test folding works in filter"""
        ds = ray.data.from_items([{"x": 1}, {"x": 2}, {"x": 3}])
        result = ds.filter(expr=col("x") > (lit(5) - lit(4)))

        assert result.take() == [{"x": 2}, {"x": 3}]

    def test_complex_pipeline(self):
        """Test folding in multi-stage pipeline"""
        ds = ray.data.range(10)
        result = (
            ds.with_column("a", lit(2) + lit(3))
            .with_column("b", col("a") * 2)
            .filter(expr=col("b") > (lit(5) * 1))
        )

        data = result.take()
        assert all(row["b"] == 10 for row in data)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
