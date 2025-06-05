import gc
from dataclasses import dataclass, field

import numpy as np
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

from ray._private.arrow_utils import get_pyarrow_version
from ray.air.util.tensor_extensions.arrow import (
    ArrowConversionError,
    ArrowTensorArray,
    _convert_to_pyarrow_native_array,
    _infer_pyarrow_type,
    convert_to_pyarrow_array,
)
from ray.air.util.tensor_extensions.utils import create_ragged_ndarray
from ray.data import DataContext
from ray.tests.conftest import *  # noqa

import psutil


@dataclass
class UserObj:
    i: int = field()


@pytest.mark.parametrize(
    "input",
    [
        # Python native lists
        [
            [1, 2],
            [3, 4],
        ],
        # Python native tuples
        [
            (1, 2),
            (3, 4),
        ],
        # Lists as PA scalars
        [
            pa.scalar([1, 2]),
            pa.scalar([3, 4]),
        ],
    ],
)
def test_arrow_native_list_conversion(input, disable_fallback_to_object_extension):
    """Test asserts that nested lists are represented as native Arrow lists
    upon serialization into Arrow format (and are NOT converted to numpy
    tensor using extension)"""

    if isinstance(input[0], pa.Scalar) and get_pyarrow_version() <= parse_version(
        "13.0.0"
    ):
        pytest.skip(
            "Pyarrow < 13.0 not able to properly infer native types from its own Scalars"
        )

    pa_arr = convert_to_pyarrow_array(input, "a")

    # Should be able to natively convert back to Pyarrow array,
    # not using any extensions
    assert pa_arr.type == pa.list_(pa.int64()), pa_arr.type
    assert pa.array(input) == pa_arr, pa_arr


@pytest.mark.parametrize("arg_type", ["list", "ndarray"])
@pytest.mark.parametrize(
    "numpy_precision, expected_arrow_timestamp_type",
    [
        ("ms", pa.timestamp("ms")),
        ("us", pa.timestamp("us")),
        ("ns", pa.timestamp("ns")),
        # The coarsest resolution Arrow supports is seconds.
        ("Y", pa.timestamp("s")),
        ("M", pa.timestamp("s")),
        ("D", pa.timestamp("s")),
        ("h", pa.timestamp("s")),
        ("m", pa.timestamp("s")),
        ("s", pa.timestamp("s")),
        # The finest resolution Arrow supports is nanoseconds.
        ("ps", pa.timestamp("ns")),
        ("fs", pa.timestamp("ns")),
        ("as", pa.timestamp("ns")),
    ],
)
def test_convert_datetime_array(
    numpy_precision: str,
    expected_arrow_timestamp_type: pa.TimestampType,
    arg_type: str,
    restore_data_context,
):
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = False

    ndarray = np.ones(1, dtype=f"datetime64[{numpy_precision}]")

    if arg_type == "ndarray":
        column_values = ndarray
    elif arg_type == "list":
        column_values = [ndarray]
    else:
        pytest.fail(f"Unknown type: {arg_type}")

    # Step 1: Convert to PA array
    converted = convert_to_pyarrow_array(column_values, "")

    if arg_type == "ndarray":
        expected = pa.array(
            column_values.astype(f"datetime64[{expected_arrow_timestamp_type.unit}]")
        )
    elif arg_type == "list":
        expected = ArrowTensorArray.from_numpy(
            [
                column_values[0].astype(
                    f"datetime64[{expected_arrow_timestamp_type.unit}]"
                )
            ]
        )
    else:
        pytest.fail(f"Unknown type: {arg_type}")

    assert expected.type == converted.type
    assert expected == converted


@pytest.mark.parametrize("arg_type", ["list", "ndarray"])
@pytest.mark.parametrize("dtype", ["int64", "float64", "datetime64[ns]"])
def test_infer_type_does_not_leak_memory(arg_type, dtype):
    # Test for https://github.com/apache/arrow/issues/45493.
    ndarray = np.zeros(923040, dtype=dtype)  # A ~7 MiB column

    process = psutil.Process()
    gc.collect()
    pa.default_memory_pool().release_unused()
    before = process.memory_info().rss

    if arg_type == "ndarray":
        column_values = ndarray
    elif arg_type == "list":
        column_values = [ndarray]
    else:
        pytest.fail(f"Unknown type: {arg_type}")

    _infer_pyarrow_type(column_values)

    gc.collect()
    pa.default_memory_pool().release_unused()
    after = process.memory_info().rss

    assert after - before < 1024 * 1024, after - before


def test_pa_infer_type_failing_to_infer():
    # Represent a single column that will be using `ArrowPythonObjectExtension` type
    # to ser/de native Python objects into bytes
    column_vals = create_ragged_ndarray(
        [
            "hi",
            1,
            None,
            [[[[]]]],
            {"a": [[{"b": 2, "c": UserObj(i=123)}]]},
            UserObj(i=456),
        ]
    )

    inferred_dtype = _infer_pyarrow_type(column_vals)

    # Arrow (17.0) seem to fallback to assume the dtype of the first element
    assert pa.string().equals(inferred_dtype)


def test_convert_to_pyarrow_array_object_ext_type_fallback():
    column_values = create_ragged_ndarray(
        [
            "hi",
            1,
            None,
            [[[[]]]],
            {"a": [[{"b": 2, "c": UserObj(i=123)}]]},
            UserObj(i=456),
        ]
    )
    column_name = "py_object_column"

    # First, assert that straightforward conversion into Arrow native types fails
    with pytest.raises(ArrowConversionError) as exc_info:
        _convert_to_pyarrow_native_array(column_values, column_name)

    assert (
        str(exc_info.value)
        == "Error converting data to Arrow: ['hi' 1 None list([[[[]]]]) {'a': [[{'b': 2, 'c': UserObj(i=123)}]]}\n UserObj(i=456)]"  # noqa: E501
    )

    # Subsequently, assert that fallback to `ArrowObjectExtensionType` succeeds
    pa_array = convert_to_pyarrow_array(column_values, column_name)

    assert pa_array.to_pylist() == column_values.tolist()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
