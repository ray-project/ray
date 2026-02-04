import pyarrow as pa
import pytest
from pkg_resources import parse_version

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
    "expr, target_type, expected_value, expected_type",
    [
        # Basic type conversions using Ray Data's DataType
        (col("id"), DataType.int64(), 0, int),  # int64 -> int64 (no change)
        (col("id"), DataType.float64(), 0.0, float),  # int64 -> float64
        (col("id"), DataType.string(), "0", str),  # int64 -> string
        (col("id") / 2, DataType.int64(), 0, int),  # float -> int64
        (col("id") / 2, DataType.float64(), 0.0, float),  # float -> float64 (no change)
    ],
)
def test_cast_expression_basic(
    ray_start_regular_shared,
    expr,
    target_type,
    expected_value,
    expected_type,
    target_max_block_size_infinite_or_default,
):
    """Test basic type casting with cast() method."""
    ds = ray.data.range(5).with_column("result", expr.cast(target_type))
    result = ds.take(1)[0]
    assert result["id"] == 0
    assert result["result"] == expected_value
    assert isinstance(result["result"], expected_type)


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
    results = ds.take_all()

    # Verify all part values are integers
    for row in results:
        assert isinstance(row["part"], int)
        assert row["part"] in [0, 1]

    # Verify the schema shows int64 type
    schema = ds.schema()
    assert "part" in schema.names
    # The type should be int64
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
    result = ds.take(1)[0]
    assert result["id"] == 0
    assert result["result"] == 0.0
    assert isinstance(result["result"], float)

    # Cast result of arithmetic operation
    ds = ray.data.range(5)
    ds = ds.with_column("result", (col("id") + 1).cast(DataType.string()))
    result = ds.take(1)[0]
    assert result["result"] == "1"
    assert isinstance(result["result"], str)


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
    ds = ds.with_column(
        "score_int", col("score").cast(DataType.int64(), safe=False)
    )

    # Use rows_same to compare the full row content.
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
    assert rows_same(results, expected)


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

