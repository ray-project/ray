import pytest

import pandas as pd
import numpy as np
import pyarrow as pa

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.data_batch_conversion import convert_batch_type_to_pandas
from ray.air.util.data_batch_conversion import convert_pandas_to_batch_type
from ray.air.util.data_batch_conversion import DataType
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.air.util.tensor_extensions.arrow import ArrowTensorArray


def test_pandas_pandas():
    input_data = pd.DataFrame({"x": [1, 2, 3]})
    expected_output = input_data
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert convert_pandas_to_batch_type(actual_output, type=DataType.PANDAS).equals(
        input_data
    )


def test_numpy_pandas():
    input_data = np.array([1, 2, 3])
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: TensorArray([1, 2, 3])})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert np.array_equal(
        convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY), input_data
    )


def test_numpy_multi_dim_pandas():
    input_data = np.arange(12).reshape((3, 2, 2))
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: TensorArray(input_data)})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert np.array_equal(
        convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY), input_data
    )


def test_numpy_object_pandas():
    input_data = np.array([[1, 2, 3], [1]], dtype=object)
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: TensorArray(input_data)})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert np.array_equal(
        convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY), input_data
    )


def test_dict_fail():
    input_data = {"x": "y"}
    with pytest.raises(ValueError):
        convert_batch_type_to_pandas(input_data)


def test_dict_pandas():
    input_data = {"x": np.array([1, 2, 3])}
    expected_output = pd.DataFrame({"x": TensorArray(input_data["x"])})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    output_array = convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY)
    assert np.array_equal(output_array, input_data["x"])


def test_dict_multi_dim_to_pandas():
    tensor = np.arange(12).reshape((3, 2, 2))
    input_data = {"x": tensor}
    expected_output = pd.DataFrame({"x": TensorArray(tensor)})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    output_array = convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY)
    assert np.array_equal(output_array, input_data["x"])


def test_dict_pandas_multi_column():
    array_dict = {"x": np.array([1, 2, 3]), "y": np.array([4, 5, 6])}
    expected_output = pd.DataFrame({k: TensorArray(v) for k, v in array_dict.items()})
    actual_output = convert_batch_type_to_pandas(array_dict)
    assert expected_output.equals(actual_output)

    output_dict = convert_pandas_to_batch_type(actual_output, type=DataType.NUMPY)
    for k, v in output_dict.items():
        assert np.array_equal(v, array_dict[k])


def test_arrow_pandas():
    df = pd.DataFrame({"x": [1, 2, 3]})
    input_data = pa.Table.from_pandas(df)
    expected_output = df
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert convert_pandas_to_batch_type(actual_output, type=DataType.ARROW).equals(
        input_data
    )


def test_arrow_tensor_pandas():
    np_array = np.array([1, 2, 3])
    df = pd.DataFrame({"x": TensorArray(np_array)})
    input_data = pa.Table.from_arrays(
        [ArrowTensorArray.from_numpy(np_array)], names=["x"]
    )
    expected_output = df
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert convert_pandas_to_batch_type(actual_output, type=DataType.ARROW).equals(
        input_data
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
