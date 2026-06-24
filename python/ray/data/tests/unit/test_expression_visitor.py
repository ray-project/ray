import pyarrow as pa
import pyarrow.compute as pc

from ray.data._internal.planner.plan_expression.expression_visitors import (
    _StructuralFingerprintOccurrenceCollector,
    _StructuralFingerprintVisitor,
)
from ray.data.datatype import DataType
from ray.data.expressions import (
    AliasExpr,
    BinaryExpr,
    ColumnExpr,
    PyArrowComputeUDFExpr,
    UDFExpr,
    col,
    lit,
    udf,
)


@udf(return_dtype=DataType.int64())
def add_one(x: pa.Array) -> pa.Array:
    return pc.add(x, 1)


def _fingerprint(expr):
    return _StructuralFingerprintVisitor().visit(expr)


def test_structural_fingerprint_matches_structural_equality():
    expr = (add_one(col("a")) + lit(1)).alias("result")
    equivalent_expr = (add_one(col("a")) + lit(1)).alias("result")
    different_child_expr = (add_one(col("b")) + lit(1)).alias("result")
    different_alias_expr = (add_one(col("a")) + lit(1)).alias("other")

    assert expr.structurally_equals(equivalent_expr)
    assert _fingerprint(expr) == _fingerprint(equivalent_expr)
    assert _fingerprint(expr) != _fingerprint(different_child_expr)
    assert _fingerprint(expr) != _fingerprint(different_alias_expr)


def test_structural_fingerprint_handles_pyarrow_compute_udfs():
    expr = col("a").abs()
    equivalent_expr = col("a").abs()
    different_child_expr = col("b").abs()

    assert isinstance(expr, PyArrowComputeUDFExpr)
    assert expr.structurally_equals(equivalent_expr)
    assert _fingerprint(expr) == _fingerprint(equivalent_expr)
    assert _fingerprint(expr) != _fingerprint(different_child_expr)


def test_occurrence_collector_records_bottom_up_keys_and_depths():
    expr = (add_one(col("a")) + add_one(col("a"))).alias("result")

    collector = _StructuralFingerprintOccurrenceCollector()
    root_key = collector.visit(expr)
    occurrences = collector.get_occurrences()

    assert root_key == _fingerprint(expr)
    assert [type(occurrence.expr) for occurrence in occurrences] == [
        ColumnExpr,
        UDFExpr,
        ColumnExpr,
        UDFExpr,
        BinaryExpr,
        AliasExpr,
    ]
    assert [occurrence.depth for occurrence in occurrences] == [3, 2, 3, 2, 1, 0]
    assert all(
        occurrence.key == _fingerprint(occurrence.expr) for occurrence in occurrences
    )

    udf_occurrences = [
        occurrence for occurrence in occurrences if isinstance(occurrence.expr, UDFExpr)
    ]
    assert len(udf_occurrences) == 2
    assert udf_occurrences[0].key == udf_occurrences[1].key
    assert udf_occurrences[0].expr.structurally_equals(udf_occurrences[1].expr)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
