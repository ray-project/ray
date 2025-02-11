import gc
from dataclasses import dataclass, field

import numpy as np
import psutil
import pyarrow as pa
import pytest

from ray.air.util.tensor_extensions.arrow import (
    ArrowConversionError,
    _convert_to_pyarrow_native_array,
    _infer_pyarrow_type,
    convert_to_pyarrow_array,
)
from ray.air.util.tensor_extensions.utils import create_ragged_ndarray
from ray.tests.conftest import *  # noqa


@dataclass
class UserObj:
    i: int = field()


@pytest.mark.parametrize(
    "numpy_precision, expected_arrow_unit",
    [
        # The lowest resolution Arrow supports is seconds.
        ("Y", "s"),
        ("M", "s"),
        ("D", "s"),
        ("h", "s"),
        ("m", "s"),
        ("s", "s"),
        ("ms", "ms"),
        ("us", "us"),
        ("ns", "ns"),
        # The highest resolution Arrow supports is nanoseconds.
        ("ps", "ns"),
        ("fs", "ns"),
        ("as", "ns"),
    ],
)
def test_convert_datetime_array(
    numpy_precision: str,
    expected_arrow_unit: str,
):
    numpy_array = np.zeros(1, dtype=f"datetime64[{numpy_precision}]")

    pyarrow_array = _convert_to_pyarrow_native_array(numpy_array, "")

    assert pyarrow_array.type.unit == expected_arrow_unit
    assert len(numpy_array) == len(pyarrow_array)


@pytest.mark.parametrize("dtype", ["int64", "float64", "datetime64[ns]"])
def test_infer_type_does_not_leak_memory(dtype):
    # Test for https://github.com/apache/arrow/issues/45493.
    column_values = np.zeros(923040, dtype=dtype)  # A ~7 MiB column

    process = psutil.Process()
    gc.collect()
    pa.default_memory_pool().release_unused()
    before = process.memory_info().rss

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
