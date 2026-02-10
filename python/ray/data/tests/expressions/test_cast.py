import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data._internal.util import rows_same
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.datatype import DataType
from ray.data.exceptions import UserCodeException
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
@pytest.mark.parametrize(
    "expr, target_type, expected_rows",
    [
        # Basic type conversions using Ray Data's DataType
        (col("id"), DataType.int64(), [{"id": i, "result": i} for i in range(5)]),
        (
            col("id"),
            DataType.float64(),
            [{"id": i, "result": float(i)} for i in range(5)],
        ),
        (
            col("id"),
            DataType.string(),
            [{"id": i, "result": str(i)} for i in range(5)],
        ),
        (
            col("id") / 2,
            DataType.int64(),
            [{"id": i, "result": i // 2} for i in range(5)],
        ),
        # col("id")/2 uses integer division in expression layer, then cast to float64
        (
            col("id") / 2,
            DataType.float64(),
            [{"id": i, "result": float(i // 2)} for i in range(5)],
        ),
    ],
)
def test_cast_expression_basic(
    ray_start_regular_shared,
    expr,
    target_type,
    expected_rows,
    target_max_block_size_infinite_or_default,
):
    """Test basic type casting with cast() method."""
    ds = ray.data.range(5).with_column("result", expr.cast(target_type))
    actual = ds.take_all()
    assert rows_same(pd.DataFrame(actual), pd.DataFrame(expected_rows))


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_cast_expression_usecase(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test the user use case: converting float result from modulo to int64."""
    ds = ray.data.range(10)
    # The modulo operation returns float, cast it to int64
    ds = ds.with_column("part", (col("id") % 2).cast(DataType.int64()))
    actual = ds.take_all()
    expected_rows = [{"id": i, "part": i % 2} for i in range(10)]
    assert rows_same(pd.DataFrame(actual), pd.DataFrame(expected_rows))

    # Verify the schema shows int64 type
    schema = ds.schema()
    assert "part" in schema.names
    part_type = schema.types[schema.names.index("part")]
    assert part_type == pa.int64()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_cast_expression_chained(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test that cast() can be chained with other expressions."""
    ds = ray.data.range(5)
    # Cast to float64 then multiply
    ds = ds.with_column("result", col("id").cast(DataType.float64()) * 2.5)
    actual = ds.take_all()
    expected_rows = [{"id": i, "result": i * 2.5} for i in range(5)]
    assert rows_same(pd.DataFrame(actual), pd.DataFrame(expected_rows))

    # Cast result of arithmetic operation
    ds = ray.data.range(5)
    ds = ds.with_column("result", (col("id") + 1).cast(DataType.string()))
    actual = ds.take_all()
    expected_rows = [{"id": i, "result": str(i + 1)} for i in range(5)]
    assert rows_same(pd.DataFrame(actual), pd.DataFrame(expected_rows))


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_cast_expression_safe_mode(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test that safe=True (default) raises errors on invalid conversions."""
    ds = ray.data.from_items([{"value": "not_a_number"}])

    # Attempting to cast non-numeric string to int should raise an error
    with pytest.raises((UserCodeException, ValueError, pa.ArrowInvalid)):
        ds.with_column("result", col("value").cast(DataType.int64())).materialize()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_cast_expression_invalid_type(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test that invalid type targets raise appropriate errors."""
    ds = ray.data.range(5)

    # Passing a non-DataType target should raise TypeError
    with pytest.raises(
        TypeError, match="target_type must be a ray.data.datatype.DataType"
    ):
        ds.with_column("result", col("id").cast("invalid_type")).materialize()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_cast_expression_multiple_types(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test casting with multiple different target types."""
    ds = ray.data.from_items([{"id": 42, "score": 3.14}])

    # Cast id to different types
    ds = ds.with_column("id_int", col("id").cast(DataType.int64()))
    ds = ds.with_column("id_float", col("id").cast(DataType.float64()))
    ds = ds.with_column("id_str", col("id").cast(DataType.string()))

    # Cast score to int (use safe=False to allow float truncation to int)
    ds = ds.with_column("score_int", col("score").cast(DataType.int64(), safe=False))

    # Use rows_same to compare the full row content (expects DataFrames).
    results = ds.take_all()
    expected = [
        {
            "id": 42,
            "score": 3.14,
            "id_int": 42,
            "id_float": 42.0,
            "id_str": "42",
            "score_int": 3,
        }
    ]
    assert rows_same(pd.DataFrame(results), pd.DataFrame(expected))


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("20.0.0"),
    reason="with_column requires PyArrow >= 20.0.0",
)
def test_cast_expression_python_type_datatype_error(
    ray_start_regular_shared, target_max_block_size_infinite_or_default
):
    """Test that using Python-type-backed DataType in cast() raises a clear error."""
    # Error is raised at expression build time when cast() is called (not at materialize).
    error_match = "Python-type-backed DataType.*requires.*values"
    with pytest.raises(TypeError, match=error_match):
        col("id").cast(DataType(int))
    with pytest.raises(TypeError, match=error_match):
        col("id").cast(DataType(str))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
