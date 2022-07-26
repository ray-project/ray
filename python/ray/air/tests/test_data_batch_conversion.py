import pytest

import pandas as pd
import numpy as np
import pyarrow as pa

import ray
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.data_batch_conversion import convert_batch_type_to_pandas
from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type
from ray.air.util.data_batch_conversion import DataType
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.air.util.tensor_extensions.arrow import ArrowTensorArray


@pytest.fixture(params=[True, False])
def enable_automatic_tensor_extension_cast(request):
    ctx = ray.data.context.DatasetContext.get_current()
    original = ctx.enable_tensor_extension_casting
    ctx.enable_tensor_extension_casting = request.param
    yield request.param
    ctx.enable_tensor_extension_casting = original


def test_pandas_pandas():
    input_data = pd.DataFrame({"x": [1, 2, 3]})
    expected_output = input_data
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    actual_output = convert_pandas_to_batch_type(actual_output, type=DataType.PANDAS)
    pd.testing.assert_frame_equal(actual_output, input_data)


@pytest.mark.parametrize("use_tensor_extension", [True, False])
def test_pandas_multi_dim_pandas(
    enable_automatic_tensor_extension_cast, use_tensor_extension
):
    input_tensor = np.arange(12).reshape((3, 2, 2))
    input_data = pd.DataFrame(
        {"x": TensorArray(input_tensor) if use_tensor_extension else list(input_tensor)}
    )
    expected_output = pd.DataFrame(
        {
            "x": (
                list(input_tensor)
                if (enable_automatic_tensor_extension_cast or not use_tensor_extension)
                else TensorArray(input_tensor)
            )
        }
    )
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    actual_output = convert_pandas_to_batch_type(actual_output, type=DataType.PANDAS)
    pd.testing.assert_frame_equal(actual_output, input_data)


def test_numpy_pandas(enable_automatic_tensor_extension_cast):
    input_data = np.array([1, 2, 3])
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: input_data})
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_array = convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY)
    np.testing.assert_equal(output_array, input_data)


def test_numpy_multi_dim_pandas(enable_automatic_tensor_extension_cast):
    input_data = np.arange(12).reshape((3, 2, 2))
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: list(input_data)})
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_array = convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY)
    np.testing.assert_array_equal(np.array(list(output_array)), input_data)


def test_numpy_object_pandas():
    input_data = np.array([[1, 2, 3], [1]], dtype=object)
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: input_data})
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    np.testing.assert_array_equal(
        convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY), input_data
    )


def test_dict_fail():
    input_data = {"x": "y"}
    with pytest.raises(ValueError):
        convert_batch_type_to_pandas(input_data)


def test_dict_pandas(enable_automatic_tensor_extension_cast):
    input_data = {"x": np.array([1, 2, 3])}
    expected_output = pd.DataFrame({"x": input_data["x"]})
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_array = convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY)
    np.testing.assert_array_equal(output_array, input_data["x"])


def test_dict_multi_dim_to_pandas(enable_automatic_tensor_extension_cast):
    tensor = np.arange(12).reshape((3, 2, 2))
    input_data = {"x": tensor}
    expected_output = pd.DataFrame({"x": list(tensor)})
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_array = convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY)
    np.testing.assert_array_equal(np.array(list(output_array)), input_data["x"])


def test_dict_pandas_multi_column(enable_automatic_tensor_extension_cast):
    array_dict = {"x": np.array([1, 2, 3]), "y": np.array([4, 5, 6])}
    expected_output = pd.DataFrame(array_dict)
    actual_output = convert_batch_type_to_pandas(array_dict)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_dict = convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY)
    for k, v in output_dict.items():
        np.testing.assert_array_equal(v, array_dict[k])


def test_arrow_pandas():
    df = pd.DataFrame({"x": [1, 2, 3]})
    input_data = pa.Table.from_pandas(df)
    expected_output = df
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    assert convert_pandas_to_batch_type(actual_output, type=DataType.ARROW).equals(
        input_data
    )


def test_arrow_tensor_pandas(enable_automatic_tensor_extension_cast):
    np_array = np.arange(12).reshape((3, 2, 2))
    input_data = pa.Table.from_arrays(
        [ArrowTensorArray.from_numpy(np_array)], names=["x"]
    )
    actual_output = convert_batch_type_to_pandas(input_data)
    expected_output = pd.DataFrame({"x": list(np_array)})
    expected_output = pd.DataFrame(
        {
            "x": (
                list(np_array)
                if enable_automatic_tensor_extension_cast
                else TensorArray(np_array)
            )
        }
    )
    pd.testing.assert_frame_equal(expected_output, actual_output)

    assert convert_pandas_to_batch_type(actual_output, type=DataType.ARROW).equals(
        input_data
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
