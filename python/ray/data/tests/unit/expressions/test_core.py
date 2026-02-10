"""Tests for core expression types and basic functionality.

This module tests:
- ColumnExpr, LiteralExpr, BinaryExpr, UnaryExpr, AliasExpr, StarExpr
- Structural equality for all expression types
- Expression tree repr (string representation)
- UDFExpr structural equality
"""

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from ray.data._internal.planner.plan_expression.expression_visitors import (
    _InlineExprReprVisitor,
)
from ray.data.datatype import DataType
from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    Expr,
    LiteralExpr,
    Operation,
    StarExpr,
    UDFExpr,
    UnaryExpr,
    col,
    download,
    lit,
    star,
    udf,
)

# ──────────────────────────────────────
# Column Expression Tests
# ──────────────────────────────────────


class TestColumnExpr:
    """Tests for ColumnExpr functionality."""

    def test_column_creation(self):
        """Test that col() creates a ColumnExpr with correct name."""
        expr = col("age")
        assert isinstance(expr, ColumnExpr)
        assert expr.name == "age"

    def test_column_name_property(self):
        """Test that name property returns the column name."""
        expr = col("my_column")
        assert expr.name == "my_column"

    @pytest.mark.parametrize(
        "name1,name2,expected",
        [
            ("a", "a", True),
            ("a", "b", False),
            ("column_name", "column_name", True),
            ("COL", "col", False),  # Case sensitive
        ],
        ids=["same_name", "different_name", "long_name", "case_sensitive"],
    )
    def test_column_structural_equality(self, name1, name2, expected):
        """Test structural equality for column expressions."""
        assert col(name1).structurally_equals(col(name2)) is expected


# ──────────────────────────────────────
# Literal Expression Tests
# ──────────────────────────────────────


class TestLiteralExpr:
    """Tests for LiteralExpr functionality."""

    @pytest.mark.parametrize(
        "value",
        [42, 3.14, "hello", True, False, None, [1, 2, 3]],
        ids=["int", "float", "string", "bool_true", "bool_false", "none", "list"],
    )
    def test_literal_creation(self, value):
        """Test that lit() creates a LiteralExpr with correct value."""
        expr = lit(value)
        assert isinstance(expr, LiteralExpr)
        assert expr.value == value

    @pytest.mark.parametrize(
        "val1,val2,expected",
        [
            (1, 1, True),
            (1, 2, False),
            ("x", "y", False),
            ("x", "x", True),
            (1, 1.0, False),  # Different types
            (True, True, True),
            (True, False, False),
            ([1, 2], [1, 2], True),
            ([1, 2], [1, 3], False),
        ],
        ids=[
            "same_int",
            "different_int",
            "different_str",
            "same_str",
            "int_vs_float",
            "same_bool",
            "different_bool",
            "same_list",
            "different_list",
        ],
    )
    def test_literal_structural_equality(self, val1, val2, expected):
        """Test structural equality for literal expressions."""
        assert lit(val1).structurally_equals(lit(val2)) is expected


# ──────────────────────────────────────
# Binary Expression Tests
# ──────────────────────────────────────


class TestBinaryExpr:
    """Tests for BinaryExpr structure (not operation semantics)."""

    def test_binary_expression_structure(self):
        """Test that binary expressions have correct structure."""
        expr = col("a") + lit(1)
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.ADD
        assert isinstance(expr.left, ColumnExpr)
        assert isinstance(expr.right, LiteralExpr)

    @pytest.mark.parametrize(
        "expr1,expr2,expected",
        [
            (col("a") + 1, col("a") + 1, True),
            (col("a") + 1, col("a") + 2, False),  # Different literal
            (col("a") + 1, col("b") + 1, False),  # Different column
            (col("a") + 1, col("a") - 1, False),  # Different operator
            # Nested binary expressions
            ((col("a") * 2) + (col("b") / 3), (col("a") * 2) + (col("b") / 3), True),
            ((col("a") * 2) + (col("b") / 3), (col("a") * 2) - (col("b") / 3), False),
            ((col("a") * 2) + (col("b") / 3), (col("c") * 2) + (col("b") / 3), False),
            ((col("a") * 2) + (col("b") / 3), (col("a") * 2) + (col("b") / 4), False),
            # Commutative operations are not structurally equal
            (col("a") + col("b"), col("b") + col("a"), False),
            (lit(1) * col("c"), col("c") * lit(1), False),
        ],
        ids=[
            "same_simple",
            "different_literal",
            "different_column",
            "different_operator",
            "same_nested",
            "nested_diff_op",
            "nested_diff_col",
            "nested_diff_lit",
            "commutative_add",
            "commutative_mul",
        ],
    )
    def test_binary_structural_equality(self, expr1, expr2, expected):
        """Test structural equality for binary expressions."""
        assert expr1.structurally_equals(expr2) is expected
        # Test symmetry
        assert expr2.structurally_equals(expr1) is expected


# ──────────────────────────────────────
# Unary Expression Tests
# ──────────────────────────────────────


class TestUnaryExpr:
    """Tests for UnaryExpr structure."""

    @pytest.mark.parametrize(
        "expr,expected_op",
        [
            (col("age").is_null(), Operation.IS_NULL),
            (col("name").is_not_null(), Operation.IS_NOT_NULL),
            (~col("active"), Operation.NOT),
        ],
        ids=["is_null", "is_not_null", "not"],
    )
    def test_unary_expression_structure(self, expr, expected_op):
        """Test that unary expressions have correct structure."""
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


# ──────────────────────────────────────
# Alias Expression Tests
# ──────────────────────────────────────


class TestAliasExpr:
    """Tests for AliasExpr functionality."""

    @pytest.mark.parametrize(
        "expr,alias_name,expected_alias",
        [
            (col("price"), "product_price", "product_price"),
            (lit(42), "answer", "answer"),
            (col("a") + col("b"), "sum", "sum"),
            ((col("price") * col("qty")) + lit(5), "total_with_fee", "total_with_fee"),
            (col("age") >= lit(18), "is_adult", "is_adult"),
        ],
        ids=["col_alias", "lit_alias", "binary_alias", "complex_alias", "comparison"],
    )
    def test_alias_functionality(self, expr, alias_name, expected_alias):
        """Test alias creation and properties."""
        aliased_expr = expr.alias(alias_name)
        assert aliased_expr.name == expected_alias
        assert aliased_expr.expr.structurally_equals(expr)
        # Data type should be preserved
        assert aliased_expr.data_type == expr.data_type

    @pytest.mark.parametrize(
        "expr1,expr2,expected",
        [
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
        ],
        ids=[
            "same_alias",
            "different_alias_name",
            "different_column",
            "same_complex",
            "different_expr",
            "alias_vs_non_alias",
        ],
    )
    def test_alias_structural_equality(self, expr1, expr2, expected):
        """Test structural equality for alias expressions."""
        assert expr1.structurally_equals(expr2) is expected

    def test_alias_structural_equality_respects_rename_flag(self):
        expr = col("a")
        aliased = expr.alias("b")
        renamed = expr._rename("b")

        assert aliased.structurally_equals(aliased)
        assert renamed.structurally_equals(renamed)
        assert not aliased.structurally_equals(renamed)
        assert not aliased.structurally_equals(expr.alias("c"))

    def test_alias_evaluation_equivalence(self):
        """Test that alias evaluation produces same result as original."""
        import pandas as pd

        from ray.data._internal.planner.plan_expression.expression_evaluator import (
            eval_expr,
        )

        test_data = pd.DataFrame({"price": [10, 20], "qty": [2, 3]})
        expr = col("price") * col("qty")
        aliased = expr.alias("total")

        original_result = eval_expr(expr, test_data)
        aliased_result = eval_expr(aliased, test_data)

        assert original_result.equals(aliased_result)


# ──────────────────────────────────────
# Star Expression Tests
# ──────────────────────────────────────


class TestStarExpr:
    """Tests for StarExpr functionality."""

    def test_star_creation(self):
        """Test that star() creates a StarExpr."""
        expr = star()
        assert isinstance(expr, StarExpr)

    def test_star_structural_equality(self):
        """Test structural equality for star expressions."""
        assert star().structurally_equals(star())
        assert not star().structurally_equals(col("a"))


# ──────────────────────────────────────
# UDF Expression Tests
# ──────────────────────────────────────


class TestUDFExpr:
    """Tests for UDFExpr structural equality."""

    def test_regular_function_udf_structural_equality(self):
        """Test that regular function UDFs compare fn correctly."""

        @udf(return_dtype=DataType.int32())
        def add_one(x: pa.Array) -> pa.Array:
            return pc.add(x, 1)

        @udf(return_dtype=DataType.int32())
        def add_two(x: pa.Array) -> pa.Array:
            return pc.add(x, 2)

        expr1 = add_one(col("value"))
        expr2 = add_one(col("value"))
        expr3 = add_two(col("value"))

        # Same function should be equal
        assert expr1.structurally_equals(expr2)

        # Different functions should not be equal
        assert not expr1.structurally_equals(expr3)

    def test_callable_class_udf_structural_equality(self):
        """Test that callable class UDFs with same spec are structurally equal."""

        @udf(return_dtype=DataType.int32())
        class AddOffset:
            def __init__(self, offset):
                self.offset = offset

            def __call__(self, x: pa.Array) -> pa.Array:
                return pc.add(x, self.offset)

        # Create the same callable class instance
        add_five = AddOffset(5)

        # Each call creates a new _placeholder function internally,
        # but the callable_class_spec should be the same
        expr1 = add_five(col("value"))
        expr2 = add_five(col("value"))

        # These should be structurally equal
        assert expr1.structurally_equals(expr2)
        assert expr2.structurally_equals(expr1)

        # Different constructor args should not be equal
        add_ten = AddOffset(10)
        expr3 = add_ten(col("value"))
        assert not expr1.structurally_equals(expr3)

        # Different column args should not be equal
        expr4 = add_five(col("other"))
        assert not expr1.structurally_equals(expr4)

    def test_callable_class_vs_regular_function_udf(self):
        """Test that callable class UDFs are not equal to regular function UDFs."""

        @udf(return_dtype=DataType.int32())
        class AddOne:
            def __call__(self, x: pa.Array) -> pa.Array:
                return pc.add(x, 1)

        @udf(return_dtype=DataType.int32())
        def add_one(x: pa.Array) -> pa.Array:
            return pc.add(x, 1)

        class_expr = AddOne()(col("value"))
        func_expr = add_one(col("value"))

        # Different types of UDFs should not be equal
        assert not class_expr.structurally_equals(func_expr)
        assert not func_expr.structurally_equals(class_expr)


# ──────────────────────────────────────
# Cross-type Equality Tests
# ──────────────────────────────────────


class TestCrossTypeEquality:
    """Test that different expression types are not structurally equal."""

    @pytest.mark.parametrize(
        "expr1,expr2",
        [
            (col("a"), lit("a")),
            (col("a"), col("a") + 0),
            (lit(1), lit(1) + 0),
            (col("a"), col("a").alias("a")),
            (col("a"), star()),
        ],
        ids=[
            "col_vs_lit",
            "col_vs_binary",
            "lit_vs_binary",
            "col_vs_alias",
            "col_vs_star",
        ],
    )
    def test_different_types_not_equal(self, expr1, expr2):
        """Test that different expression types are not structurally equal."""
        assert not expr1.structurally_equals(expr2)
        assert not expr2.structurally_equals(expr1)

    def test_operator_eq_is_not_structural_eq(self):
        """Confirms that == builds an expression, while structurally_equals compares."""
        # `==` returns a BinaryExpr, not a boolean
        op_eq_expr = col("a") == col("a")
        assert isinstance(op_eq_expr, Expr)
        assert not isinstance(op_eq_expr, bool)

        # `structurally_equals` returns a boolean
        struct_eq_result = col("a").structurally_equals(col("a"))
        assert isinstance(struct_eq_result, bool)
        assert struct_eq_result is True


# ──────────────────────────────────────
# Expression Repr Tests
# ──────────────────────────────────────


def _build_complex_expr():
    """Build a convoluted expression that exercises all visitor code paths."""

    def custom_udf(x, y):
        return x + y

    # Create UDF expression
    udf_expr = UDFExpr(
        fn=custom_udf,
        args=[col("value"), lit(10)],
        kwargs={"z": col("multiplier")},
        data_type=DataType(int),
    )

    # Build the mega-complex expression
    inner_expr = (
        ((col("age") + lit(10)) * col("rate") / lit(2.5) >= lit(100))
        & (
            col("name").is_not_null()
            | (col("status").is_in(["active", "pending"]) & col("verified"))
        )
        & ((col("count") - lit(5)) // lit(2) <= col("limit"))
        & ~(col("deleted").is_null() | (col("score") != lit(0)))
        & (download("uri") < star())
        & (udf_expr.alias("udf_result") > lit(50))
    ).alias("complex_filter")

    return ~inner_expr


class TestExpressionRepr:
    """Test expression string representations."""

    def test_tree_repr(self):
        """Test tree representation of expressions."""
        expr = _build_complex_expr()
        expected = """NOT
    └── operand: ALIAS('complex_filter')
        └── AND
            ├── left: AND
            │   ├── left: AND
            │   │   ├── left: AND
            │   │   │   ├── left: AND
            │   │   │   │   ├── left: GE
            │   │   │   │   │   ├── left: DIV
            │   │   │   │   │   │   ├── left: MUL
            │   │   │   │   │   │   │   ├── left: ADD
            │   │   │   │   │   │   │   │   ├── left: COL('age')
            │   │   │   │   │   │   │   │   └── right: LIT(10)
            │   │   │   │   │   │   │   └── right: COL('rate')
            │   │   │   │   │   │   └── right: LIT(2.5)
            │   │   │   │   │   └── right: LIT(100)
            │   │   │   │   └── right: OR
            │   │   │   │       ├── left: IS_NOT_NULL
            │   │   │   │       │   └── operand: COL('name')
            │   │   │   │       └── right: AND
            │   │   │   │           ├── left: IN
            │   │   │   │           │   ├── left: COL('status')
            │   │   │   │           │   └── right: LIT(['active', 'pending'])
            │   │   │   │           └── right: COL('verified')
            │   │   │   └── right: LE
            │   │   │       ├── left: FLOORDIV
            │   │   │       │   ├── left: SUB
            │   │   │       │   │   ├── left: COL('count')
            │   │   │       │   │   └── right: LIT(5)
            │   │   │       │   └── right: LIT(2)
            │   │   │       └── right: COL('limit')
            │   │   └── right: NOT
            │   │       └── operand: OR
            │   │           ├── left: IS_NULL
            │   │           │   └── operand: COL('deleted')
            │   │           └── right: NE
            │   │               ├── left: COL('score')
            │   │               └── right: LIT(0)
            │   └── right: LT
            │       ├── left: DOWNLOAD('uri')
            │       └── right: COL(*)
            └── right: GT
                ├── left: ALIAS('udf_result')
                │   └── UDF(custom_udf)
                │       ├── arg[0]: COL('value')
                │       ├── arg[1]: LIT(10)
                │       └── kwarg['z']: COL('multiplier')
                └── right: LIT(50)"""
        assert repr(expr) == expected

    def test_inline_repr_prefix(self):
        """Test that inline representation starts correctly."""
        expr = _build_complex_expr()
        visitor = _InlineExprReprVisitor()
        inline_repr = visitor.visit(expr)
        expected_prefix = "~((((((((col('age') + 10) * col('rate')) / 2.5) >= 100) & (col('name').is_not_null() | ((col('status')"
        assert inline_repr.startswith(expected_prefix)
        assert inline_repr.endswith(".alias('complex_filter')")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
