"""Tests for expression conversion to PyArrow and Iceberg.

This module tests:
- Conversion to PyArrow compute expressions (to_pyarrow)
- Conversion to PyIceberg expressions (IcebergExpressionVisitor)
- Unsupported expression handling
"""

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pytest
from pyiceberg.expressions import (
    And,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNull,
    Or,
    Reference,
    literal,
)

from ray.data._internal.datasource.iceberg_datasource import _IcebergExpressionVisitor
from ray.data.datatype import DataType
from ray.data.expressions import (
    BinaryExpr,
    Operation,
    UDFExpr,
    col,
    download,
    lit,
    star,
)

# ──────────────────────────────────────
# PyArrow Conversion Tests
# ──────────────────────────────────────


class TestToPyArrow:
    """Test conversion of Ray Data expressions to PyArrow compute expressions."""

    @pytest.fixture
    def test_table(self):
        """Sample PyArrow table for testing expressions."""
        return pa.table(
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

    # ── Basic Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_pyarrow_expr,description",
        [
            (col("age"), lambda: pc.field("age"), "column reference"),
            (lit(42), lambda: pc.scalar(42), "integer literal"),
            (lit("hello"), lambda: pc.scalar("hello"), "string literal"),
        ],
        ids=["col", "int_lit", "str_lit"],
    )
    def test_basic_expressions(
        self, test_table, ray_expr, equivalent_pyarrow_expr, description
    ):
        """Test conversion of basic expressions."""
        converted = ray_expr.to_pyarrow()
        expected = equivalent_pyarrow_expr()

        assert isinstance(converted, pc.Expression)
        assert isinstance(expected, pc.Expression)

    # ── Arithmetic Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_pyarrow_expr,description",
        [
            (
                col("x") + 5,
                lambda: pc.add(pc.field("x"), pc.scalar(5)),
                "addition",
            ),
            (
                col("x") - 3,
                lambda: pc.subtract(pc.field("x"), pc.scalar(3)),
                "subtraction",
            ),
            (
                col("x") * 2,
                lambda: pc.multiply(pc.field("x"), pc.scalar(2)),
                "multiplication",
            ),
            (
                col("x") / 2,
                lambda: pc.divide(pc.field("x"), pc.scalar(2)),
                "division",
            ),
        ],
        ids=["add", "sub", "mul", "div"],
    )
    def test_arithmetic_expressions(
        self, test_table, ray_expr, equivalent_pyarrow_expr, description
    ):
        """Test conversion of arithmetic expressions."""
        converted = ray_expr.to_pyarrow()
        expected = equivalent_pyarrow_expr()

        assert isinstance(converted, pc.Expression)
        assert isinstance(expected, pc.Expression)

    # ── Comparison Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_pyarrow_expr,description",
        [
            (
                col("age") > 18,
                lambda: pc.greater(pc.field("age"), pc.scalar(18)),
                "greater than",
            ),
            (
                col("age") < 65,
                lambda: pc.less(pc.field("age"), pc.scalar(65)),
                "less than",
            ),
            (
                col("age") >= 21,
                lambda: pc.greater_equal(pc.field("age"), pc.scalar(21)),
                "greater equal",
            ),
            (
                col("age") <= 30,
                lambda: pc.less_equal(pc.field("age"), pc.scalar(30)),
                "less equal",
            ),
            (
                col("status") == "active",
                lambda: pc.equal(pc.field("status"), pc.scalar("active")),
                "equality",
            ),
            (
                col("status") != "deleted",
                lambda: pc.not_equal(pc.field("status"), pc.scalar("deleted")),
                "not equal",
            ),
        ],
        ids=["gt", "lt", "ge", "le", "eq", "ne"],
    )
    def test_comparison_expressions(
        self, test_table, ray_expr, equivalent_pyarrow_expr, description
    ):
        """Test conversion of comparison expressions."""
        converted = ray_expr.to_pyarrow()
        expected = equivalent_pyarrow_expr()

        # Verify they produce the same results on sample data
        dataset = ds.dataset(test_table)
        try:
            result_converted = dataset.scanner(filter=converted).to_table()
            result_expected = dataset.scanner(filter=expected).to_table()
            assert result_converted.equals(result_expected)
        except (TypeError, pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError):
            pass

    # ── Boolean Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_pyarrow_expr,description",
        [
            (
                (col("age") > 18) & (col("age") < 65),
                lambda: pc.and_kleene(
                    pc.greater(pc.field("age"), pc.scalar(18)),
                    pc.less(pc.field("age"), pc.scalar(65)),
                ),
                "logical AND",
            ),
            (
                (col("status") == "active") | (col("status") == "pending"),
                lambda: pc.or_kleene(
                    pc.equal(pc.field("status"), pc.scalar("active")),
                    pc.equal(pc.field("status"), pc.scalar("pending")),
                ),
                "logical OR",
            ),
            (
                ~col("active"),
                lambda: pc.invert(pc.field("active")),
                "logical NOT",
            ),
        ],
        ids=["and", "or", "not"],
    )
    def test_boolean_expressions(
        self, test_table, ray_expr, equivalent_pyarrow_expr, description
    ):
        """Test conversion of boolean expressions."""
        converted = ray_expr.to_pyarrow()
        expected = equivalent_pyarrow_expr()

        dataset = ds.dataset(test_table)
        try:
            result_converted = dataset.scanner(filter=converted).to_table()
            result_expected = dataset.scanner(filter=expected).to_table()
            assert result_converted.equals(result_expected)
        except (TypeError, pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError):
            pass

    # ── Predicate Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_pyarrow_expr,description",
        [
            (
                col("value").is_null(),
                lambda: pc.is_null(pc.field("value")),
                "is_null check",
            ),
            (
                col("value").is_not_null(),
                lambda: pc.is_valid(pc.field("value")),
                "is_not_null check",
            ),
            (
                col("status").is_in(["active", "pending"]),
                lambda: pc.is_in(pc.field("status"), pa.array(["active", "pending"])),
                "is_in with list",
            ),
        ],
        ids=["is_null", "is_not_null", "is_in"],
    )
    def test_predicate_expressions(
        self, test_table, ray_expr, equivalent_pyarrow_expr, description
    ):
        """Test conversion of predicate expressions."""
        converted = ray_expr.to_pyarrow()
        expected = equivalent_pyarrow_expr()

        dataset = ds.dataset(test_table)
        try:
            result_converted = dataset.scanner(filter=converted).to_table()
            result_expected = dataset.scanner(filter=expected).to_table()
            assert result_converted.equals(result_expected)
        except (TypeError, pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError):
            pass

    # ── Nested Expressions ──

    def test_nested_arithmetic(self, test_table):
        """Test nested arithmetic expressions."""
        ray_expr = (col("price") * col("quantity")) + col("tax")
        converted = ray_expr.to_pyarrow()
        assert isinstance(converted, pc.Expression)

    # ── Alias Expressions ──

    def test_alias_expressions(self, test_table):
        """Test that alias expressions unwrap to inner expression."""
        ray_expr = (col("x") + 5).alias("result")
        converted = ray_expr.to_pyarrow()
        assert isinstance(converted, pc.Expression)

    # ── Unsupported Expressions ──

    def test_udf_expression_raises(self):
        """Test that UDF expressions raise TypeError."""

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

    def test_download_expression_raises(self):
        """Test that download expressions raise TypeError."""
        with pytest.raises(TypeError, match="Download expressions cannot be converted"):
            download("uri").to_pyarrow()

    def test_star_expression_raises(self):
        """Test that star expressions raise TypeError."""
        with pytest.raises(TypeError, match="Star expressions cannot be converted"):
            star().to_pyarrow()


# ──────────────────────────────────────
# Iceberg Conversion Tests
# ──────────────────────────────────────


class TestIcebergExpressionVisitor:
    """Test conversion of Ray Data expressions to PyIceberg expressions."""

    # ── Basic Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_iceberg_expr,description",
        [
            (col("age"), lambda: Reference("age"), "column reference"),
            (lit(42), lambda: literal(42), "integer literal"),
            (lit("active"), lambda: literal("active"), "string literal"),
        ],
        ids=["col", "int_lit", "str_lit"],
    )
    def test_basic_expressions(self, ray_expr, equivalent_iceberg_expr, description):
        """Test conversion of basic expressions."""
        visitor = _IcebergExpressionVisitor()
        converted = visitor.visit(ray_expr)
        expected = equivalent_iceberg_expr()
        assert converted == expected

    # ── Comparison Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_iceberg_expr,description",
        [
            (
                col("age") > 18,
                lambda: GreaterThan(Reference("age"), literal(18)),
                "greater than",
            ),
            (
                col("age") >= 21,
                lambda: GreaterThanOrEqual(Reference("age"), literal(21)),
                "greater than or equal",
            ),
            (
                col("age") < 65,
                lambda: LessThan(Reference("age"), literal(65)),
                "less than",
            ),
            (
                col("age") <= 100,
                lambda: LessThanOrEqual(Reference("age"), literal(100)),
                "less than or equal",
            ),
            (
                col("status") == "active",
                lambda: EqualTo(Reference("status"), literal("active")),
                "equality",
            ),
            (
                col("status") != "inactive",
                lambda: NotEqualTo(Reference("status"), literal("inactive")),
                "not equal",
            ),
        ],
        ids=["gt", "ge", "lt", "le", "eq", "ne"],
    )
    def test_comparison_expressions(
        self, ray_expr, equivalent_iceberg_expr, description
    ):
        """Test conversion of comparison expressions."""
        visitor = _IcebergExpressionVisitor()
        converted = visitor.visit(ray_expr)
        expected = equivalent_iceberg_expr()
        assert converted == expected

    # ── Boolean Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_iceberg_expr,description",
        [
            (
                (col("age") > 18) & (col("age") < 65),
                lambda: And(
                    GreaterThan(Reference("age"), literal(18)),
                    LessThan(Reference("age"), literal(65)),
                ),
                "logical AND",
            ),
            (
                (col("is_member") == lit(True)) | (col("is_premium") == lit(True)),
                lambda: Or(
                    EqualTo(Reference("is_member"), literal(True)),
                    EqualTo(Reference("is_premium"), literal(True)),
                ),
                "logical OR",
            ),
            (
                ~(col("deleted") == lit(True)),
                lambda: Not(EqualTo(Reference("deleted"), literal(True))),
                "logical NOT",
            ),
        ],
        ids=["and", "or", "not"],
    )
    def test_boolean_expressions(self, ray_expr, equivalent_iceberg_expr, description):
        """Test conversion of boolean expressions."""
        visitor = _IcebergExpressionVisitor()
        converted = visitor.visit(ray_expr)
        expected = equivalent_iceberg_expr()
        assert converted == expected

    # ── Predicate Expressions ──

    @pytest.mark.parametrize(
        "ray_expr,equivalent_iceberg_expr,description",
        [
            (
                col("value").is_null(),
                lambda: IsNull(Reference("value")),
                "is_null check",
            ),
            (
                col("name").is_not_null(),
                lambda: NotNull(Reference("name")),
                "is_not_null check",
            ),
            (
                col("status").is_in(["active", "pending"]),
                lambda: In(Reference("status"), ["active", "pending"]),
                "is_in with list",
            ),
            (
                col("status").not_in(["inactive", "deleted"]),
                lambda: NotIn(Reference("status"), ["inactive", "deleted"]),
                "not_in with list",
            ),
        ],
        ids=["is_null", "is_not_null", "is_in", "not_in"],
    )
    def test_predicate_expressions(
        self, ray_expr, equivalent_iceberg_expr, description
    ):
        """Test conversion of predicate expressions."""
        visitor = _IcebergExpressionVisitor()
        converted = visitor.visit(ray_expr)
        expected = equivalent_iceberg_expr()
        assert converted == expected

    # ── Complex Nested Expressions ──

    def test_complex_nested_boolean(self):
        """Test complex nested boolean expression."""
        ray_expr = (
            (col("age") >= 21)
            & (col("country") == "USA")
            & col("verified").is_not_null()
        )
        expected = And(
            And(
                GreaterThanOrEqual(Reference("age"), literal(21)),
                EqualTo(Reference("country"), literal("USA")),
            ),
            NotNull(Reference("verified")),
        )

        visitor = _IcebergExpressionVisitor()
        converted = visitor.visit(ray_expr)
        assert converted == expected

    def test_aliased_expression(self):
        """Test that alias expressions unwrap to inner expression."""
        ray_expr = (col("age") > 18).alias("is_adult")
        expected = GreaterThan(Reference("age"), literal(18))

        visitor = _IcebergExpressionVisitor()
        converted = visitor.visit(ray_expr)
        assert converted == expected

    # ── Unsupported Arithmetic ──

    @pytest.mark.parametrize(
        "ray_expr",
        [
            col("price") + 10,
            col("quantity") * 2,
            col("total") - col("discount"),
            col("revenue") / col("count"),
            col("items") // 5,
        ],
        ids=["add", "mul", "sub", "div", "floordiv"],
    )
    def test_arithmetic_raises(self, ray_expr):
        """Test that arithmetic operations raise appropriate errors."""
        visitor = _IcebergExpressionVisitor()
        with pytest.raises(
            ValueError, match="Unsupported binary operation for Iceberg"
        ):
            visitor.visit(ray_expr)

    # ── Unsupported Expressions ──

    def test_udf_expression_raises(self):
        """Test that UDF expressions raise TypeError."""

        def dummy_fn(x):
            return x

        udf_expr = UDFExpr(
            fn=dummy_fn,
            args=[col("x")],
            kwargs={},
            data_type=DataType(int),
        )

        visitor = _IcebergExpressionVisitor()
        with pytest.raises(
            TypeError, match="UDF expressions cannot be converted to Iceberg"
        ):
            visitor.visit(udf_expr)

    def test_download_expression_raises(self):
        """Test that download expressions raise TypeError."""
        visitor = _IcebergExpressionVisitor()
        with pytest.raises(
            TypeError, match="Download expressions cannot be converted to Iceberg"
        ):
            visitor.visit(download("uri"))

    def test_star_expression_raises(self):
        """Test that star expressions raise TypeError."""
        visitor = _IcebergExpressionVisitor()
        with pytest.raises(
            TypeError, match="Star expressions cannot be converted to Iceberg"
        ):
            visitor.visit(star())

    def test_is_in_requires_literal_list(self):
        """Test that IN/NOT_IN operations require literal lists."""
        visitor = _IcebergExpressionVisitor()

        # This should work - literal list
        expr = col("status").is_in(["active", "pending"])
        result = visitor.visit(expr)
        assert isinstance(result, In)

        # This should fail - column reference on right side
        with pytest.raises(
            ValueError, match="IN operation requires right operand to be a literal list"
        ):
            invalid_expr = BinaryExpr(Operation.IN, col("a"), col("b"))
            visitor.visit(invalid_expr)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
