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


def _build_complex_expr():
    """Build a convoluted expression that exercises all visitor code paths.

    This expression includes:
    - Binary operations: ADD, SUB, MUL, DIV, FLOORDIV, GT, LT, GE, LE, EQ, NE, AND, OR, IN, NOT_IN
    - Unary operations: NOT, IS_NULL, IS_NOT_NULL
    - Literals: int, float, string, bool, list
    - Columns
    - Aliases
    - Star expression
    - Download expression
    - UDF expression
    - Deep nesting on both left and right sides
    """
    from ray.data.datatype import DataType
    from ray.data.expressions import UDFExpr, download, star

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

    expr = ~inner_expr

    return expr


@pytest.mark.parametrize(
    "expr_fn,expected",
    [
        (
            _build_complex_expr,
            """NOT
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
                └── right: LIT(50)""",
        ),
    ],
    ids=["complex_expression"],
)
def test_expression_repr(expr_fn, expected):
    """Test tree representation of expressions with a comprehensive example."""
    expr = expr_fn()
    assert repr(expr) == expected


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
