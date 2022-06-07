import pytest

import pandas as pd
import numpy as np
import pyarrow as pa

from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.utils.data_batch_conversion_utils import convert_batch_type_to_pandas
from ray.air.utils.data_batch_conversion_utils import convert_pandas_to_batch_type
from ray.air.utils.tensor_extensions.pandas import TensorArray


def test_pandas_pandas():
    input_data = pd.DataFrame({"x": [1, 2, 3]})
    expected_output = input_data
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert convert_pandas_to_batch_type(actual_output, type=pd.DataFrame).equals(
        input_data
    )


def test_numpy_pandas():
    input_data = np.array([1, 2, 3])
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: TensorArray([1, 2, 3])})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert np.array_equal(
        convert_pandas_to_batch_type(actual_output, type=np.ndarray), input_data
    )


def test_numpy_multi_dim_pandas():
    input_data = np.arange(12).reshape((3, 2, 2))
    pd.DataFrame({TENSOR_COLUMN_NAME: TensorArray([1, 2, 3])})
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: TensorArray(input_data)})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert np.array_equal(
        convert_pandas_to_batch_type(actual_output, type=np.ndarray), input_data
    )


def test_numpy_object_pandas():
    input_data = np.array([[1, 2, 3], [1]], dtype=object)
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: input_data})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert np.array_equal(
        convert_pandas_to_batch_type(actual_output, type=np.ndarray), input_data
    )


def test_dict_pandas():
    input_data = {"x": np.array([1, 2, 3])}
    expected_output = pd.DataFrame({"x": TensorArray(input_data["x"])})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    output_dict = convert_pandas_to_batch_type(actual_output, type=dict)
    assert output_dict.keys() == input_data.keys()
    for k, v in output_dict.items():
        assert np.array_equal(v, input_data[k])


def test_dict_multi_dim_to_pandas():
    tensor = np.arange(12).reshape((3, 2, 2))
    input_data = {"x": tensor}
    expected_output = pd.DataFrame({"x": TensorArray(tensor)})
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    output_dict = convert_pandas_to_batch_type(actual_output, type=dict)
    assert output_dict.keys() == input_data.keys()
    for k, v in output_dict.items():
        assert np.array_equal(v, input_data[k])


def test_arrow_to_pandas():
    df = pd.DataFrame({"x": [1, 2, 3]})
    input_data = pa.Table.from_pandas(df)
    expected_output = df
    actual_output = convert_batch_type_to_pandas(input_data)
    assert expected_output.equals(actual_output)

    assert convert_pandas_to_batch_type(actual_output, type=pa.Table).equals(input_data)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
