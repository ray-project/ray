import pytest
import warnings

import pandas as pd
import numpy as np
import pyarrow as pa

from ray.air._internal.torch_utils import convert_ndarray_to_torch_tensor
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.data_batch_conversion import (
    convert_batch_type_to_pandas,
    convert_pandas_to_batch_type,
    _convert_batch_type_to_numpy,
    _cast_ndarray_columns_to_tensor_extension,
    _cast_tensor_columns_to_ndarrays,
)
from ray.air.util.data_batch_conversion import BatchFormat
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.air.util.tensor_extensions.arrow import ArrowTensorArray


def test_pandas_pandas():
    input_data = pd.DataFrame({"x": [1, 2, 3]})
    expected_output = input_data
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    actual_output = convert_pandas_to_batch_type(actual_output, type=BatchFormat.PANDAS)
    pd.testing.assert_frame_equal(actual_output, input_data)


def test_numpy_to_numpy():
    input_data = {"x": np.arange(12).reshape(3, 4)}
    expected_output = input_data
    actual_output = _convert_batch_type_to_numpy(input_data)
    assert expected_output == actual_output

    input_data = {
        "column_1": np.arange(12).reshape(3, 4),
        "column_2": np.arange(12).reshape(3, 4),
    }
    expected_output = {
        "column_1": np.arange(12).reshape(3, 4),
        "column_2": np.arange(12).reshape(3, 4),
    }
    actual_output = _convert_batch_type_to_numpy(input_data)
    assert input_data.keys() == expected_output.keys()
    np.testing.assert_array_equal(input_data["column_1"], expected_output["column_1"])
    np.testing.assert_array_equal(input_data["column_2"], expected_output["column_2"])

    input_data = np.arange(12).reshape(3, 4)
    expected_output = input_data
    actual_output = _convert_batch_type_to_numpy(input_data)
    np.testing.assert_array_equal(expected_output, actual_output)


def test_arrow_to_numpy():
    input_data = pa.table({"column_1": [1, 2, 3, 4]})
    expected_output = {"column_1": np.array([1, 2, 3, 4])}
    actual_output = _convert_batch_type_to_numpy(input_data)
    assert expected_output.keys() == actual_output.keys()
    np.testing.assert_array_equal(
        expected_output["column_1"], actual_output["column_1"]
    )

    input_data = pa.table(
        {
            TENSOR_COLUMN_NAME: ArrowTensorArray.from_numpy(
                np.arange(12).reshape(3, 2, 2)
            )
        }
    )
    expected_output = np.arange(12).reshape(3, 2, 2)
    actual_output = _convert_batch_type_to_numpy(input_data)
    np.testing.assert_array_equal(expected_output, actual_output)

    input_data = pa.table(
        {
            "column_1": [1, 2, 3, 4],
            "column_2": [1, -1, 1, -1],
        }
    )
    expected_output = {
        "column_1": np.array([1, 2, 3, 4]),
        "column_2": np.array([1, -1, 1, -1]),
    }

    actual_output = _convert_batch_type_to_numpy(input_data)
    assert expected_output.keys() == actual_output.keys()
    np.testing.assert_array_equal(
        expected_output["column_1"], actual_output["column_1"]
    )
    np.testing.assert_array_equal(
        expected_output["column_2"], actual_output["column_2"]
    )


def test_pd_dataframe_to_numpy():
    input_data = pd.DataFrame({"column_1": [1, 2, 3, 4]})
    expected_output = np.array([1, 2, 3, 4])
    actual_output = _convert_batch_type_to_numpy(input_data)
    np.testing.assert_array_equal(expected_output, actual_output)

    input_data = pd.DataFrame(
        {TENSOR_COLUMN_NAME: TensorArray(np.arange(12).reshape(3, 4))}
    )
    expected_output = np.arange(12).reshape(3, 4)
    actual_output = _convert_batch_type_to_numpy(input_data)
    np.testing.assert_array_equal(expected_output, actual_output)

    input_data = pd.DataFrame({"column_1": [1, 2, 3, 4], "column_2": [1, -1, 1, -1]})
    expected_output = {
        "column_1": np.array([1, 2, 3, 4]),
        "column_2": np.array([1, -1, 1, -1]),
    }
    actual_output = _convert_batch_type_to_numpy(input_data)
    assert expected_output.keys() == actual_output.keys()
    np.testing.assert_array_equal(
        expected_output["column_1"], actual_output["column_1"]
    )
    np.testing.assert_array_equal(
        expected_output["column_2"], actual_output["column_2"]
    )


@pytest.mark.parametrize("use_tensor_extension_for_input", [True, False])
@pytest.mark.parametrize("cast_tensor_columns", [True, False])
def test_pandas_multi_dim_pandas(cast_tensor_columns, use_tensor_extension_for_input):
    input_tensor = np.arange(12).reshape((3, 2, 2))
    input_data = pd.DataFrame(
        {
            "x": TensorArray(input_tensor)
            if use_tensor_extension_for_input
            else list(input_tensor)
        }
    )
    expected_output = pd.DataFrame(
        {
            "x": (
                list(input_tensor)
                if cast_tensor_columns or not use_tensor_extension_for_input
                else TensorArray(input_tensor)
            )
        }
    )
    actual_output = convert_batch_type_to_pandas(input_data, cast_tensor_columns)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    actual_output = convert_pandas_to_batch_type(
        actual_output, type=BatchFormat.PANDAS, cast_tensor_columns=cast_tensor_columns
    )
    pd.testing.assert_frame_equal(actual_output, input_data)


def test_no_pandas_future_warning():
    """Tests that Pandas in-place FutureWarning is
    suppressed during tensor extension casting."""

    input_tensor = np.arange(12).reshape((3, 2, 2))
    input_data = pd.DataFrame({"x": TensorArray(input_tensor)})

    with warnings.catch_warnings():
        warnings.simplefilter("error", category=FutureWarning)
        data_no_tensor_array = _cast_tensor_columns_to_ndarrays(input_data)
        _cast_ndarray_columns_to_tensor_extension(data_no_tensor_array)


@pytest.mark.parametrize("cast_tensor_columns", [True, False])
def test_numpy_pandas(cast_tensor_columns):
    input_data = np.array([1, 2, 3])
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: input_data})
    actual_output = convert_batch_type_to_pandas(input_data, cast_tensor_columns)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_array = convert_pandas_to_batch_type(
        actual_output, type=BatchFormat.NUMPY, cast_tensor_columns=cast_tensor_columns
    )
    np.testing.assert_equal(output_array, input_data)


@pytest.mark.parametrize("cast_tensor_columns", [True, False])
def test_numpy_multi_dim_pandas(cast_tensor_columns):
    input_data = np.arange(12).reshape((3, 2, 2))
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: list(input_data)})
    actual_output = convert_batch_type_to_pandas(input_data, cast_tensor_columns)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_array = convert_pandas_to_batch_type(
        actual_output, type=BatchFormat.NUMPY, cast_tensor_columns=cast_tensor_columns
    )
    np.testing.assert_array_equal(np.array(list(output_array)), input_data)


def test_numpy_object_pandas():
    input_data = np.array([[1, 2, 3], [1]], dtype=object)
    expected_output = pd.DataFrame({TENSOR_COLUMN_NAME: input_data})
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    np.testing.assert_array_equal(
        convert_pandas_to_batch_type(actual_output, type=BatchFormat.NUMPY), input_data
    )


@pytest.mark.parametrize("writable", [False, True])
def test_numpy_to_tensor_warning(writable):
    input_data = np.array([[1, 2, 3]], dtype=int)
    input_data.setflags(write=writable)

    with pytest.warns(None) as record:
        tensor = convert_ndarray_to_torch_tensor(input_data)
    assert not record.list, [w.message for w in record.list]
    assert tensor is not None


def test_dict_fail():
    input_data = {"x": "y"}
    with pytest.raises(ValueError):
        convert_batch_type_to_pandas(input_data)


@pytest.mark.parametrize("cast_tensor_columns", [True, False])
def test_dict_pandas(cast_tensor_columns):
    input_data = {"x": np.array([1, 2, 3])}
    expected_output = pd.DataFrame({"x": input_data["x"]})
    actual_output = convert_batch_type_to_pandas(input_data, cast_tensor_columns)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_array = convert_pandas_to_batch_type(
        actual_output, type=BatchFormat.NUMPY, cast_tensor_columns=cast_tensor_columns
    )
    np.testing.assert_array_equal(output_array, input_data["x"])


@pytest.mark.parametrize("cast_tensor_columns", [True, False])
def test_dict_multi_dim_to_pandas(cast_tensor_columns):
    tensor = np.arange(12).reshape((3, 2, 2))
    input_data = {"x": tensor}
    expected_output = pd.DataFrame({"x": list(tensor)})
    actual_output = convert_batch_type_to_pandas(input_data, cast_tensor_columns)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_array = convert_pandas_to_batch_type(
        actual_output, type=BatchFormat.NUMPY, cast_tensor_columns=cast_tensor_columns
    )
    np.testing.assert_array_equal(np.array(list(output_array)), input_data["x"])


@pytest.mark.parametrize("cast_tensor_columns", [True, False])
def test_dict_pandas_multi_column(cast_tensor_columns):
    array_dict = {"x": np.array([1, 2, 3]), "y": np.array([4, 5, 6])}
    expected_output = pd.DataFrame(array_dict)
    actual_output = convert_batch_type_to_pandas(array_dict, cast_tensor_columns)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    output_dict = convert_pandas_to_batch_type(
        actual_output, type=BatchFormat.NUMPY, cast_tensor_columns=cast_tensor_columns
    )
    for k, v in output_dict.items():
        np.testing.assert_array_equal(v, array_dict[k])


def test_arrow_pandas():
    df = pd.DataFrame({"x": [1, 2, 3]})
    input_data = pa.Table.from_pandas(df)
    expected_output = df
    actual_output = convert_batch_type_to_pandas(input_data)
    pd.testing.assert_frame_equal(expected_output, actual_output)

    assert convert_pandas_to_batch_type(actual_output, type=BatchFormat.ARROW).equals(
        input_data
    )


@pytest.mark.parametrize("cast_tensor_columns", [True, False])
def test_arrow_tensor_pandas(cast_tensor_columns):
    np_array = np.arange(12).reshape((3, 2, 2))
    input_data = pa.Table.from_arrays(
        [ArrowTensorArray.from_numpy(np_array)], names=["x"]
    )
    actual_output = convert_batch_type_to_pandas(input_data, cast_tensor_columns)
    expected_output = pd.DataFrame({"x": list(np_array)})
    expected_output = pd.DataFrame(
        {"x": (list(np_array) if cast_tensor_columns else TensorArray(np_array))}
    )
    pd.testing.assert_frame_equal(expected_output, actual_output)

    arrow_output = convert_pandas_to_batch_type(
        actual_output, type=BatchFormat.ARROW, cast_tensor_columns=cast_tensor_columns
    )
    assert arrow_output.equals(input_data)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
