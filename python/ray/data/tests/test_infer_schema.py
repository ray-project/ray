import sys
from typing import Optional

import pyarrow as pa
import pytest

from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.all_to_all_operator import Repartition
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.operators.map_operator import Filter, Project
from ray.data.expressions import (
    BinaryExpr,
    ColumnExpr,
    LiteralExpr,
    Operation,
    StarExpr,
    UnaryExpr,
    col,
    download,
)


class MockLogicalOperator(LogicalOperator):
    """Mock logical operator for testing."""

    def __init__(self, schema: Optional[pa.Schema] = None):
        super().__init__("MockOperator", [])
        self._schema = schema

    def infer_schema(self) -> Optional[pa.Schema]:
        return self._schema


class TestSchemaInference:
    def test_filter_schema_inference(self):
        """Test Filter operator schema inference."""
        input_schema = pa.schema(
            [("id", pa.int64()), ("name", pa.string()), ("value", pa.float64())]
        )

        input_op = MockLogicalOperator(input_schema)

        def filter_fn(x):
            return x["value"] > 0

        filter_op = Filter(input_op, fn=filter_fn)
        result_schema = filter_op.infer_schema()

        assert result_schema == input_schema

    def test_count_schema_inference(self):
        """Test Count operator schema inference."""
        input_op = MockLogicalOperator(pa.schema([("id", pa.int64())]))
        count_op = Count(input_op)
        result_schema = count_op.infer_schema()

        expected_schema = pa.schema([(Count.COLUMN_NAME, pa.int64())])
        assert result_schema == expected_schema

    def test_project_simple_fields(self):
        """Test projecting simple fields."""
        input_schema = pa.schema(
            [("id", pa.int64()), ("name", pa.string()), ("age", pa.int32())]
        )

        mock_input = MockLogicalOperator(input_schema)

        exprs = [ColumnExpr("id"), ColumnExpr("name")]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 2
        assert result_schema.field("id").type == pa.int64()
        assert result_schema.field("name").type == pa.string()

    def test_project_with_alias(self):
        """Test projecting with aliases."""
        input_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])

        mock_input = MockLogicalOperator(input_schema)

        exprs = [
            ColumnExpr("id").alias("user_id"),
            ColumnExpr("name").alias("user_name"),
        ]

        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 2
        assert result_schema.field("user_id").type == pa.int64()
        assert result_schema.field("user_name").type == pa.string()

    def test_project_with_star(self):
        """Test projecting with star expression."""
        input_schema = pa.schema(
            [("id", pa.int64()), ("name", pa.string()), ("age", pa.int32())]
        )

        mock_input = MockLogicalOperator(input_schema)

        exprs = [StarExpr(), ColumnExpr("age").alias("user_age")]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 4
        assert result_schema.field("id").type == pa.int64()
        assert result_schema.field("name").type == pa.string()
        assert result_schema.field("age").type == pa.int32()
        assert result_schema.field("user_age").type == pa.int32()

    def test_project_mixed_expressions(self):
        """Test projecting with mixed expressions."""
        input_schema = pa.schema(
            [("id", pa.int64()), ("price", pa.float64()), ("quantity", pa.int32())]
        )

        mock_input = MockLogicalOperator(input_schema)

        exprs = [
            ColumnExpr("id"),
            BinaryExpr(Operation.MUL, col("price"), col("quantity")).alias("total"),
        ]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 2
        assert result_schema.field("id").type == pa.int64()
        assert result_schema.field("total").type == pa.float64()

    def test_project_with_literal(self):
        """Test projecting with literal values."""
        input_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])

        mock_input = MockLogicalOperator(input_schema)

        exprs = [
            LiteralExpr(42).alias("constant_int"),
            LiteralExpr("hello").alias("constant_str"),
            LiteralExpr(True).alias("constant_bool"),
            LiteralExpr(3.14).alias("constant_float"),
            LiteralExpr([1, 2, 3]).alias("constant_list"),
        ]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 5
        assert result_schema.field("constant_int").type == pa.int64()
        assert result_schema.field("constant_str").type == pa.string()
        assert result_schema.field("constant_bool").type == pa.bool_()
        assert result_schema.field("constant_float").type == pa.float64()
        assert result_schema.field("constant_list").type == pa.list_(pa.int64())

    def test_project_empty_input(self):
        """Test projecting with empty input schema."""
        input_schema = pa.schema([])
        mock_input = MockLogicalOperator(input_schema)

        exprs = [ColumnExpr("nonexistent")]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema.field("nonexistent").type == pa.string()

    def test_project_none_input(self):
        """Test projecting when input schema is None."""
        mock_input = MockLogicalOperator(None)

        exprs = [ColumnExpr("test")]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is None

    def test_project_binary_expressions(self):
        """Test projecting with various binary expressions."""
        input_schema = pa.schema(
            [
                ("a", pa.int64()),
                ("b", pa.int64()),
                ("price", pa.float64()),
                ("name", pa.string()),
            ]
        )

        mock_input = MockLogicalOperator(input_schema)

        exprs = [
            BinaryExpr(Operation.ADD, col("a"), col("b")).alias("sum_ab"),
            BinaryExpr(Operation.MUL, col("price"), col("a")).alias("total_price"),
            BinaryExpr(Operation.EQ, col("a"), col("b")).alias("is_equal"),
        ]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 3
        assert result_schema.field("sum_ab").type == pa.int64()
        assert result_schema.field("total_price").type == pa.float64()
        assert result_schema.field("is_equal").type == pa.bool_()

    def test_project_unary_expressions(self):
        """Test projecting with unary expressions."""
        input_schema = pa.schema([("flag", pa.bool_()), ("value", pa.int64())])

        mock_input = MockLogicalOperator(input_schema)
        exprs = [
            UnaryExpr(Operation.NOT, col("flag")).alias("not_flag"),
            UnaryExpr(Operation.IS_NOT_NULL, col("value")).alias("is_not_null"),
        ]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 2
        assert result_schema.field("not_flag").type == pa.bool_()
        assert result_schema.field("is_not_null").type == pa.bool_()

    def test_project_rename_expressions(self):
        """Test projecting with rename expressions"""
        input_schema = pa.schema([("foo", pa.int64()), ("bar", pa.string())])

        mock_input = MockLogicalOperator(input_schema)
        exprs = [
            StarExpr(),
            col("bar")._rename("new_bar"),
        ]
        project = Project(mock_input, exprs)
        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 2
        assert result_schema.field("foo").type == pa.int64()
        assert result_schema.field("new_bar").type == pa.string()

    def test_project_download_expressions(self):
        """Test projecting with download expressions."""
        input_schema = pa.schema([("url", pa.string()), ("filename", pa.string())])

        mock_input = MockLogicalOperator(input_schema)

        exprs = [
            download("url").alias("downloaded_content"),
            ColumnExpr("filename"),
        ]
        project = Project(mock_input, exprs)

        result_schema = project.infer_schema()
        assert result_schema is not None
        assert len(result_schema) == 2
        assert result_schema.field("downloaded_content").type == pa.binary()
        assert result_schema.field("filename").type == pa.string()

    def test_repartition_schema_inference(self):
        """Test Repartition operator schema inference."""
        input_schema = pa.schema([("id", pa.int64()), ("name", pa.string())])

        input_op = MockLogicalOperator(input_schema)
        repartition = Repartition(input_op, 4, shuffle=False)
        result_schema = repartition.infer_schema()

        assert result_schema == input_schema


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
