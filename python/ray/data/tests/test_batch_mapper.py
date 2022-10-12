import numpy as np
import pandas as pd
import pyarrow as pa

import pytest
from typing import Dict, Union
from pandas.testing import assert_frame_equal

import ray
from ray.data.preprocessors import BatchMapper
from ray.data.extensions import TensorArray
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.air.util.tensor_extensions.arrow import ArrowTensorArray


# ===== Pandas dataset formats =====
def ds_pandas_single_column_format():
    in_df = pd.DataFrame({"column_1": [1, 2, 3, 4]})
    ds = ray.data.from_pandas(in_df)
    return ds


def ds_pandas_multi_column_format():
    in_df = pd.DataFrame({"column_1": [1, 2, 3, 4], "column_2": [1, -1, 1, -1]})
    ds = ray.data.from_pandas(in_df)
    return ds


# ===== Arrow dataset formats =====
def ds_arrow_single_column_format():
    ds = ray.data.from_arrow(pa.table({"column_1": [1, 2, 3, 4]}))
    return ds


def ds_arrow_single_column_tensor_format():
    ds = ray.data.from_arrow(
        pa.table(
            {
                TENSOR_COLUMN_NAME: ArrowTensorArray.from_numpy(
                    np.arange(12).reshape((3, 2, 2))
                )
            }
        )
    )
    return ds


def ds_arrow_multi_column_format():
    ds = ray.data.from_arrow(
        pa.table(
            {
                "column_1": [1, 2, 3, 4],
                "column_2": [1, -1, 1, -1],
            }
        )
    )
    return ds


# ===== Numpy dataset formats =====
def ds_numpy_single_column_tensor_format():
    ds = ray.data.from_numpy(np.arange(12).reshape((3, 2, 2)))
    return ds


def ds_numpy_list_of_ndarray_tensor_format():
    ds = ray.data.from_numpy(
        [np.arange(12).reshape((3, 2, 2)), np.arange(12, 24).reshape((3, 2, 2))]
    )
    return ds


@pytest.mark.parametrize(
    "ds_with_expected_pandas_numpy_df",
    [
        (
            ds_pandas_single_column_format(),
            pd.DataFrame(
                {
                    "column_1": [2, 3, 4, 5],
                }
            ),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: [2, 3, 4, 5],
                }
            ),
        ),
        (
            ds_pandas_multi_column_format(),
            pd.DataFrame(
                {
                    "column_1": [2, 3, 4, 5],
                    "column_2": [2, -2, 2, -2],
                }
            ),
            pd.DataFrame(
                {
                    "column_1": [2, 3, 4, 5],
                    "column_2": [2, -2, 2, -2],
                }
            ),
        ),
    ],
)
def test_batch_mapper_pandas_data_format(ds_with_expected_pandas_numpy_df):
    """Tests batch mapper functionality for pandas data format.

    Note:
        For single column pandas dataframes, we automatically convert it to
        single column tensor with column name as `__value__`.
    """
    ds, expected_df, expected_numpy_df = ds_with_expected_pandas_numpy_df

    def add_and_modify_udf_pandas(df: "pd.DataFrame"):
        df["column_1"] = df["column_1"] + 1
        if "column_2" in df:
            df["column_2"] *= 2
        return df

    def add_and_modify_udf_numpy(data: Union[np.ndarray, Dict[str, np.ndarray]]):
        if isinstance(data, np.ndarray):
            data += 1
        else:
            data["column_1"] = data["column_1"] + 1
            if "column_2" in data:
                data["column_2"] *= 2
        return data

    # Test map_batches
    transformed_ds = ds.map_batches(add_and_modify_udf_pandas, batch_format="pandas")
    out_df_map_batches = transformed_ds.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

    transformed_ds = ds.map_batches(add_and_modify_udf_numpy, batch_format="numpy")
    out_df_map_batches = transformed_ds.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_numpy_df)

    # Test BatchMapper
    batch_mapper = BatchMapper(fn=add_and_modify_udf_pandas, batch_format="pandas")
    batch_mapper.fit(ds)
    transformed_ds = batch_mapper.transform(ds)
    out_df = transformed_ds.to_pandas()
    assert_frame_equal(out_df, expected_df)

    batch_mapper = BatchMapper(fn=add_and_modify_udf_numpy, batch_format="numpy")
    batch_mapper.fit(ds)
    transformed_ds = batch_mapper.transform(ds)
    out_df = transformed_ds.to_pandas()
    assert_frame_equal(out_df, expected_numpy_df)


@pytest.mark.parametrize(
    "ds_with_expected_pandas_numpy_df",
    [
        (
            ds_arrow_single_column_format(),
            pd.DataFrame(
                {
                    "column_1": [2, 3, 4, 5],
                }
            ),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: [2, 3, 4, 5],
                }
            ),
        ),
        (
            ds_arrow_single_column_tensor_format(),
            pd.DataFrame(
                {
                    TENSOR_COLUMN_NAME: [
                        [[1, 2], [3, 4]],
                        [[5, 6], [7, 8]],
                        [[9, 10], [11, 12]],
                    ]
                }
            ),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: TensorArray(np.arange(1, 13).reshape((3, 2, 2)))
                }
            ),
        ),
        (
            ds_arrow_multi_column_format(),
            pd.DataFrame(
                {
                    "column_1": [2, 3, 4, 5],
                    "column_2": [2, -2, 2, -2],
                }
            ),
            pd.DataFrame(
                {
                    "column_1": [2, 3, 4, 5],
                    "column_2": [2, -2, 2, -2],
                }
            ),
        ),
    ],
)
def test_batch_mapper_arrow_data_format(ds_with_expected_pandas_numpy_df):
    """Tests batch mapper functionality for arrow data format.

    Note:
        For single column pandas dataframes, we automatically convert it to
        single column tensor with column name as `__value__`.
    """
    ds, expected_df, expected_numpy_df = ds_with_expected_pandas_numpy_df

    def add_and_modify_udf_pandas(df: "pd.DataFrame"):
        col_name = "column_1"
        if len(df.columns) == 1:
            col_name = list(df.columns)[0]
        df[col_name] = df[col_name] + 1
        if "column_2" in df:
            df["column_2"] *= 2
        return df

    def add_and_modify_udf_numpy(data: Union[np.ndarray, Dict[str, np.ndarray]]):
        if isinstance(data, np.ndarray):
            data = data + 1
        else:
            data["column_1"] = data["column_1"] + 1
            if "column_2" in data:
                data["column_2"] = data["column_2"] * 2
        return data

    # Test map_batches
    transformed_ds = ds.map_batches(add_and_modify_udf_pandas, batch_format="pandas")
    out_df_map_batches = transformed_ds.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

    transformed_ds = ds.map_batches(add_and_modify_udf_numpy, batch_format="numpy")
    out_df_map_batches = transformed_ds.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_numpy_df)

    # Test BatchMapper
    batch_mapper = BatchMapper(fn=add_and_modify_udf_pandas, batch_format="pandas")
    batch_mapper.fit(ds)
    transformed_ds = batch_mapper.transform(ds)
    out_df = transformed_ds.to_pandas()
    assert_frame_equal(out_df, expected_df)

    batch_mapper = BatchMapper(fn=add_and_modify_udf_numpy, batch_format="numpy")
    batch_mapper.fit(ds)
    transformed_ds = batch_mapper.transform(ds)
    out_df = transformed_ds.to_pandas()
    assert_frame_equal(out_df, expected_numpy_df)


@pytest.mark.parametrize(
    "ds_with_expected_pandas_numpy_df",
    [
        (
            ds_numpy_single_column_tensor_format(),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: [
                        [[1, 2], [3, 4]],
                        [[5, 6], [7, 8]],
                        [[9, 10], [11, 12]],
                    ]
                }
            ),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: TensorArray(np.arange(1, 13).reshape((3, 2, 2)))
                }
            ),
        ),
        (
            ds_numpy_list_of_ndarray_tensor_format(),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: [
                        [[1, 2], [3, 4]],
                        [[5, 6], [7, 8]],
                        [[9, 10], [11, 12]],
                        [[13, 14], [15, 16]],
                        [[17, 18], [19, 20]],
                        [[21, 22], [23, 24]],
                    ]
                }
            ),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: TensorArray(np.arange(1, 25).reshape((6, 2, 2)))
                }
            ),
        ),
    ],
)
def test_batch_mapper_numpy_data_format(ds_with_expected_pandas_numpy_df):
    """Tests batch mapper functionality for numpy data format.

    Note:
        For single column pandas dataframes, we automatically convert it to
        single column tensor with column name as `__value__`.
    """
    ds, expected_df, expected_numpy_df = ds_with_expected_pandas_numpy_df

    def add_and_modify_udf_pandas(df: "pd.DataFrame"):
        col_name = list(df.columns)[0]
        df[col_name] = df[col_name] + 1
        return df

    def add_and_modify_udf_numpy(data: Union[np.ndarray, Dict[str, np.ndarray]]):
        data = data + 1
        return data

    # Test map_batches
    transformed_ds = ds.map_batches(add_and_modify_udf_pandas, batch_format="pandas")
    out_df_map_batches = transformed_ds.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

    transformed_ds = ds.map_batches(add_and_modify_udf_numpy, batch_format="numpy")
    out_df_map_batches = transformed_ds.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_numpy_df)

    # Test BatchMapper
    batch_mapper = BatchMapper(fn=add_and_modify_udf_pandas, batch_format="pandas")
    batch_mapper.fit(ds)
    transformed_ds = batch_mapper.transform(ds)
    out_df = transformed_ds.to_pandas()
    assert_frame_equal(out_df, expected_df)

    batch_mapper = BatchMapper(fn=add_and_modify_udf_numpy, batch_format="numpy")
    batch_mapper.fit(ds)
    transformed_ds = batch_mapper.transform(ds)
    out_df = transformed_ds.to_pandas()
    assert_frame_equal(out_df, expected_numpy_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
