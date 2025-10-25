import pyarrow as pa
import pyarrow.compute as pc
import pytest

from ray.data.expressions import (
    BinaryExpr,
    Expr,
    Operation,
    UnaryExpr,
    col,
    lit,
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


@pytest.mark.parametrize(
    "expr, alias_name, expected_alias",
    [
        # (expression, alias_name, expected_alias)
        (col("price"), "product_price", "product_price"),
        (lit(42), "answer", "answer"),
        (col("a") + col("b"), "sum", "sum"),
        ((col("price") * col("qty")) + lit(5), "total_with_fee", "total_with_fee"),
        (col("age") >= lit(18), "is_adult", "is_adult"),
    ],
    ids=["col_alias", "lit_alias", "binary_alias", "complex_alias", "comparison_alias"],
)
def test_alias_functionality(expr, alias_name, expected_alias):
    """Test alias functionality with various expression types."""
    import pandas as pd

    from ray.data._internal.planner.plan_expression.expression_evaluator import (
        eval_expr,
    )

    # Test alias creation
    aliased_expr = expr.alias(alias_name)
    assert aliased_expr.name == expected_alias
    assert aliased_expr.expr.structurally_equals(expr)

    # Test data type preservation
    assert aliased_expr.data_type == expr.data_type

    # Test evaluation equivalence
    test_data = pd.DataFrame(
        {
            "price": [10, 20],
            "qty": [2, 3],
            "a": [1, 2],
            "b": [3, 4],
            "age": [17, 25],
        }
    )
    original_result = eval_expr(expr, test_data)
    aliased_result = eval_expr(aliased_expr, test_data)
    if hasattr(original_result, "equals"):  # For pandas Series
        assert original_result.equals(aliased_result)
    else:  # For scalars
        assert original_result == aliased_result


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
            (col("status").is_in(["active", "pending"]), Operation.IN),
            (col("status").not_in(["inactive", "deleted"]), Operation.NOT_IN),
            (col("a").is_in(col("b")), Operation.IN),
        ],
        ids=["not_equal", "is_in", "not_in", "is_in_amongst_cols"],
    )
    def test_new_binary_operations(self, expr, expected_op):
        """Test new binary operations."""
        assert isinstance(expr, BinaryExpr)
        assert expr.op == expected_op

    def test_is_in_with_list(self):
        """Test is_in with list of values."""
        expr = col("status").is_in(["active", "pending", "completed"])
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.IN
        # The right operand should be a LiteralExpr containing the list
        assert expr.right.value == ["active", "pending", "completed"]

    def test_is_in_with_expr(self):
        """Test is_in with expression."""
        values_expr = lit(["a", "b", "c"])
        expr = col("category").is_in(values_expr)
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.IN
        assert expr.right == values_expr

    def test_is_in_amongst_cols(self):
        """Test is_in with expression."""
        expr = col("a").is_in(col("b"))
        assert isinstance(expr, BinaryExpr)
        assert expr.op == Operation.IN
        assert expr.right == col("b")


class TestBooleanExpressions:
    """Test boolean expression functionality."""

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
    def test_boolean_expressions_directly(self, condition):
        """Test that boolean expressions work directly."""
        assert isinstance(condition, Expr)
        # Verify the expression structure based on type
        if condition.op in [Operation.GT, Operation.EQ]:
            assert isinstance(condition, BinaryExpr)
        elif condition.op == Operation.IS_NOT_NULL:
            assert isinstance(condition, UnaryExpr)
        elif condition.op == Operation.AND:
            assert isinstance(condition, BinaryExpr)

    def test_boolean_combination(self):
        """Test combining boolean expressions with logical operators."""
        expr1 = col("age") > 18
        expr2 = col("status") == "active"

        # Test AND combination
        combined_and = expr1 & expr2
        assert isinstance(combined_and, BinaryExpr)
        assert combined_and.op == Operation.AND

        # Test OR combination
        combined_or = expr1 | expr2
        assert isinstance(combined_or, BinaryExpr)
        assert combined_or.op == Operation.OR

        # Test NOT operation
        negated = ~expr1
        assert isinstance(negated, UnaryExpr)
        assert negated.op == Operation.NOT

    def test_boolean_structural_equality(self):
        """Test structural equality for boolean expressions."""
        expr1 = col("age") > 18
        expr2 = col("age") > 18
        expr3 = col("age") > 21

        assert expr1.structurally_equals(expr2)
        assert not expr1.structurally_equals(expr3)

    def test_complex_boolean_expressions(self):
        """Test complex boolean expressions work correctly."""
        # Complex boolean expression
        complex_expr = (col("age") >= 21) & (col("country") == "USA")
        assert isinstance(complex_expr, BinaryExpr)
        assert complex_expr.op == Operation.AND

        # Even more complex with OR and NOT
        very_complex = ((col("age") > 21) | (col("status") == "VIP")) & ~col("banned")
        assert isinstance(very_complex, BinaryExpr)
        assert very_complex.op == Operation.AND


class TestToPyArrow:
    """Test conversion of Ray Data expressions to PyArrow compute expressions."""

    @pytest.mark.parametrize(
        "ray_expr, equivalent_pyarrow_expr, description",
        [
            # Basic expressions
            (col("age"), lambda: pc.field("age"), "column reference"),
            (lit(42), lambda: pc.scalar(42), "integer literal"),
            (lit("hello"), lambda: pc.scalar("hello"), "string literal"),
            # Arithmetic operations
            (
                col("x") + 5,
                lambda: pc.add(pc.field("x"), pc.scalar(5)),
                "addition",
            ),
            (
                col("x") * 2,
                lambda: pc.multiply(pc.field("x"), pc.scalar(2)),
                "multiplication",
            ),
            # Comparison operations
            (
                col("age") > 18,
                lambda: pc.greater(pc.field("age"), pc.scalar(18)),
                "greater than",
            ),
            (
                col("status") == "active",
                lambda: pc.equal(pc.field("status"), pc.scalar("active")),
                "equality",
            ),
            # Boolean operations
            (
                (col("age") > 18) & (col("age") < 65),
                lambda: pc.and_kleene(
                    pc.greater(pc.field("age"), pc.scalar(18)),
                    pc.less(pc.field("age"), pc.scalar(65)),
                ),
                "logical AND",
            ),
            (
                ~(col("active")),
                lambda: pc.invert(pc.field("active")),
                "logical NOT",
            ),
            # Unary operations
            (
                col("value").is_null(),
                lambda: pc.is_null(pc.field("value")),
                "is_null check",
            ),
            # In operations
            (
                col("status").is_in(["active", "pending"]),
                lambda: pc.is_in(pc.field("status"), pa.array(["active", "pending"])),
                "is_in with list",
            ),
            # Complex nested expressions
            (
                (col("price") * col("quantity")) + col("tax"),
                lambda: pc.add(
                    pc.multiply(pc.field("price"), pc.field("quantity")),
                    pc.field("tax"),
                ),
                "nested arithmetic",
            ),
            # Alias expressions (should unwrap to inner expression)
            (
                (col("x") + 5).alias("result"),
                lambda: pc.add(pc.field("x"), pc.scalar(5)),
                "aliased expression",
            ),
        ],
        ids=[
            "col",
            "int_lit",
            "str_lit",
            "add",
            "mul",
            "gt",
            "eq",
            "and",
            "not",
            "is_null",
            "is_in",
            "nested",
            "alias",
        ],
    )
    def test_to_pyarrow_equivalence(
        self, ray_expr, equivalent_pyarrow_expr, description
    ):
        """Test that Ray Data expressions convert to equivalent PyArrow expressions.

        This test documents the expected PyArrow expression for each Ray Data expression
        and verifies correctness by comparing results on sample data.
        """
        import pyarrow.dataset as ds

        # Convert Ray expression to PyArrow
        converted = ray_expr.to_pyarrow()
        expected = equivalent_pyarrow_expr()

        # Both should be PyArrow expressions
        assert isinstance(converted, pc.Expression)
        assert isinstance(expected, pc.Expression)

        # Verify they produce the same results on sample data
        test_data = pa.table(
            {
                "age": [15, 25, 45, 70],
                "x": [1, 2, 3, 4],
                "price": [10.0, 20.0, 30.0, 40.0],
                "quantity": [2, 3, 1, 5],
                "tax": [1.0, 2.0, 3.0, 4.0],
                "status": ["active", "pending", "inactive", "active"],
                "value": [1, None, 3, None],
                "active": [True, False, True, False],
            }
        )

        dataset = ds.dataset(test_data)

        try:
            # For boolean expressions, compare filter results
            result_converted = dataset.scanner(filter=converted).to_table()
            result_expected = dataset.scanner(filter=expected).to_table()
            assert result_converted.equals(
                result_expected
            ), f"Expressions produce different results for {description}"
        except (TypeError, pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError):
            # For non-boolean expressions, just verify both are valid
            pass

    def test_to_pyarrow_unsupported_expressions(self):
        """Test that unsupported expression types raise appropriate errors."""
        from ray.data.datatype import DataType
        from ray.data.expressions import UDFExpr

        def dummy_fn(x):
            return x

        udf_expr = UDFExpr(
            fn=dummy_fn,
            args=[col("x")],
            kwargs={},
            data_type=DataType(int),
        )

        with pytest.raises(TypeError, match="UDF expressions cannot be converted"):
            udf_expr.to_pyarrow()


class TestExpressionRepr:
    """Test tree representation of expressions."""

    def test_column_repr(self):
        """Test repr for simple column expression."""
        expr = col("age")
        expected = "COL('age')"
        assert repr(expr) == expected

    def test_literal_repr(self):
        """Test repr for literal expressions."""
        # Integer literal
        expr = lit(42)
        expected = "LIT(42)"
        assert repr(expr) == expected

        # String literal
        expr = lit("hello")
        expected = "LIT('hello')"
        assert repr(expr) == expected

        # Boolean literal
        expr = lit(True)
        expected = "LIT(True)"
        assert repr(expr) == expected

        # Float literal
        expr = lit(3.14)
        expected = "LIT(3.14)"
        assert repr(expr) == expected

        # List literal
        expr = lit([1, 2, 3])
        expected = "LIT([1, 2, 3])"
        assert repr(expr) == expected

    def test_literal_repr_truncation(self):
        """Test that long values are truncated."""
        long_value = "x" * 100
        expr = lit(long_value)
        repr_str = repr(expr)
        # Should be truncated to 50 chars (47 + "...")
        assert len(repr_str) <= len("LIT('')") + 50
        assert "..." in repr_str

    def test_simple_binary_expr_repr(self):
        """Test repr for simple binary expressions."""
        expr = col("x") + lit(5)
        expected = """ADD
    ├── left: COL('x')
    └── right: LIT(5)"""
        assert repr(expr) == expected

    def test_binary_expr_multiplication_repr(self):
        """Test repr for multiplication."""
        expr = col("price") * lit(2)
        expected = """MUL
    ├── left: COL('price')
    └── right: LIT(2)"""
        assert repr(expr) == expected

    def test_nested_binary_expr_repr(self):
        """Test repr for nested binary expressions."""
        # (col("x") + 5) * col("y")
        expr = (col("x") + lit(5)) * col("y")
        expected = """MUL
    ├── left: ADD
    │   ├── left: COL('x')
    │   └── right: LIT(5)
    └── right: COL('y')"""
        assert repr(expr) == expected

    def test_deeply_nested_binary_expr_repr(self):
        """Test repr for deeply nested expressions."""
        # ((col("a") + 5) * col("b")) > 100
        expr = ((col("a") + lit(5)) * col("b")) > lit(100)
        expected = """GT
    ├── left: MUL
    │   ├── left: ADD
    │   │   ├── left: COL('a')
    │   │   └── right: LIT(5)
    │   └── right: COL('b')
    └── right: LIT(100)"""
        assert repr(expr) == expected

    def test_unary_expr_not_repr(self):
        """Test repr for NOT operation."""
        expr = ~col("active")
        expected = """NOT
    └── operand: COL('active')"""
        assert repr(expr) == expected

    def test_unary_expr_is_null_repr(self):
        """Test repr for IS_NULL operation."""
        expr = col("value").is_null()
        expected = """IS_NULL
    └── operand: COL('value')"""
        assert repr(expr) == expected

    def test_unary_expr_is_not_null_repr(self):
        """Test repr for IS_NOT_NULL operation."""
        expr = col("value").is_not_null()
        expected = """IS_NOT_NULL
    └── operand: COL('value')"""
        assert repr(expr) == expected

    def test_alias_simple_repr(self):
        """Test repr for simple alias expressions."""
        expr = col("price").alias("product_price")
        expected = """ALIAS('product_price')
    └── COL('price')"""
        assert repr(expr) == expected

    def test_alias_complex_repr(self):
        """Test repr for complex aliased expression."""
        expr = (col("x") + lit(5)).alias("result")
        expected = """ALIAS('result')
    └── ADD
        ├── left: COL('x')
        └── right: LIT(5)"""
        assert repr(expr) == expected

    def test_udf_expr_with_args_repr(self):
        """Test repr for UDF expressions with positional args."""
        from ray.data.datatype import DataType
        from ray.data.expressions import UDFExpr

        def my_function(x, y):
            return x + y

        expr = UDFExpr(
            fn=my_function,
            args=[col("value"), lit(10)],
            kwargs={},
            data_type=DataType(int),
        )
        expected = """UDF(my_function)
    ├── arg[0]: COL('value')
    └── arg[1]: LIT(10)"""
        assert repr(expr) == expected

    def test_udf_expr_with_kwargs_repr(self):
        """Test repr for UDF expressions with keyword args."""
        from ray.data.datatype import DataType
        from ray.data.expressions import UDFExpr

        def my_function(x, y):
            return x + y

        expr = UDFExpr(
            fn=my_function,
            args=[],
            kwargs={"x": col("a"), "y": lit(5)},
            data_type=DataType(int),
        )
        expected = """UDF(my_function)
    ├── kwarg['x']: COL('a')
    └── kwarg['y']: LIT(5)"""
        assert repr(expr) == expected

    def test_udf_expr_mixed_args_repr(self):
        """Test repr for UDF with both positional and keyword args."""
        from ray.data.datatype import DataType
        from ray.data.expressions import UDFExpr

        def mixed_fn(a, b, c):
            return a + b + c

        expr = UDFExpr(
            fn=mixed_fn,
            args=[col("x")],
            kwargs={"y": lit(10), "z": col("w")},
            data_type=DataType(int),
        )
        expected = """UDF(mixed_fn)
    ├── arg[0]: COL('x')
    ├── kwarg['y']: LIT(10)
    └── kwarg['z']: COL('w')"""
        assert repr(expr) == expected

    def test_star_expr_repr(self):
        """Test repr for star expressions."""
        from ray.data.expressions import star

        expr = star()
        expected = "COL(*)"
        assert repr(expr) == expected

    def test_download_expr_repr(self):
        """Test repr for download expressions."""
        from ray.data.expressions import download

        expr = download("uri_column")
        expected = "DOWNLOAD('uri_column')"
        assert repr(expr) == expected

    def test_boolean_and_repr(self):
        """Test repr for AND operation."""
        expr = (col("age") > lit(18)) & (col("status") == lit("active"))
        expected = """AND
    ├── left: GT
    │   ├── left: COL('age')
    │   └── right: LIT(18)
    └── right: EQ
        ├── left: COL('status')
        └── right: LIT('active')"""
        assert repr(expr) == expected

    def test_boolean_or_repr(self):
        """Test repr for OR operation."""
        expr = (col("age") < lit(18)) | (col("age") > lit(65))
        expected = """OR
    ├── left: LT
    │   ├── left: COL('age')
    │   └── right: LIT(18)
    └── right: GT
        ├── left: COL('age')
        └── right: LIT(65)"""
        assert repr(expr) == expected

    def test_is_in_operation_repr(self):
        """Test repr for is_in operations."""
        expr = col("status").is_in(["active", "pending"])
        expected = """IN
    ├── left: COL('status')
    └── right: LIT(['active', 'pending'])"""
        assert repr(expr) == expected

    def test_not_in_operation_repr(self):
        """Test repr for not_in operations."""
        expr = col("status").not_in(["deleted", "archived"])
        expected = """NOT_IN
    ├── left: COL('status')
    └── right: LIT(['deleted', 'archived'])"""
        assert repr(expr) == expected

    def test_all_binary_operations_repr(self):
        """Test repr for all binary operation types."""
        test_cases = [
            (col("a") + col("b"), "ADD"),
            (col("a") - col("b"), "SUB"),
            (col("a") * col("b"), "MUL"),
            (col("a") / col("b"), "DIV"),
            (col("a") // col("b"), "FLOORDIV"),
            (col("a") > col("b"), "GT"),
            (col("a") < col("b"), "LT"),
            (col("a") >= col("b"), "GE"),
            (col("a") <= col("b"), "LE"),
            (col("a") == col("b"), "EQ"),
            (col("a") != col("b"), "NE"),
            (col("a") & col("b"), "AND"),
            (col("a") | col("b"), "OR"),
        ]

        for expr, op_name in test_cases:
            expected = f"""{op_name}
    ├── left: COL('a')
    └── right: COL('b')"""
            assert repr(expr) == expected, f"Failed for operation {op_name}"

    def test_complex_nested_with_unary_repr(self):
        """Test repr for complex expression with unary operations."""
        # ~((col("age") > 18) & col("active"))
        expr = ~((col("age") > lit(18)) & col("active"))
        expected = """NOT
    └── operand: AND
        ├── left: GT
        │   ├── left: COL('age')
        │   └── right: LIT(18)
        └── right: COL('active')"""
        assert repr(expr) == expected

    def test_right_side_nested_repr(self):
        """Test repr when right side has deeper nesting."""
        # col("x") + (col("y") * col("z"))
        expr = col("x") + (col("y") * col("z"))
        expected = """ADD
    ├── left: COL('x')
    └── right: MUL
        ├── left: COL('y')
        └── right: COL('z')"""
        assert repr(expr) == expected

    def test_multiple_levels_both_sides_repr(self):
        """Test repr with deep nesting on both sides."""
        # (col("a") + col("b")) * (col("c") - col("d"))
        expr = (col("a") + col("b")) * (col("c") - col("d"))
        expected = """MUL
    ├── left: ADD
    │   ├── left: COL('a')
    │   └── right: COL('b')
    └── right: SUB
        ├── left: COL('c')
        └── right: COL('d')"""
        assert repr(expr) == expected

    def test_print_expr(self, capsys):
        """Test that printing an expression shows the tree."""
        expr = col("x") + lit(5)
        print(expr)

        expected = """ADD
    ├── left: COL('x')
    └── right: LIT(5)
"""
        captured = capsys.readouterr()
        assert captured.out == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
