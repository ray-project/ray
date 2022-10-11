import numpy as np
import pandas as pd

import pytest
from pytest_lazyfixture import lazy_fixture
from typing import Dict, Union
from pandas.testing import assert_frame_equal

from ray.data.preprocessors import BatchMapper
from ray.data.extensions import TensorArray
from ray.air.constants import TENSOR_COLUMN_NAME
from ray.tests.conftest import *  # noqa


@pytest.mark.parametrize(
    "ds_with_expected_pandas_numpy_df",
    [
        (
            lazy_fixture("ds_pandas_single_column_format"),
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
            lazy_fixture("ds_pandas_multi_column_format"),
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
def test_batch_mapper_pandas_data_format(
    ray_start_regular_shared, ds_with_expected_pandas_numpy_df
):
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
    "ds",
    [
        lazy_fixture("ds_pandas_single_column_format"),
        lazy_fixture("ds_pandas_multi_column_format"),
        lazy_fixture("ds_arrow_single_column_format"),
        lazy_fixture("ds_arrow_single_column_tensor_format"),
        lazy_fixture("ds_arrow_multi_column_format"),
        lazy_fixture("ds_numpy_single_column_tensor_format"),
        lazy_fixture("ds_numpy_list_of_ndarray_tensor_format"),
    ],
)
def test_batch_mapper_batch_size(ray_start_regular_shared, ds):
    """Tests BatcMapper batch size."""

    batch_size = 2

    def check_batch_size(batch):
        print(batch.dtypes)
        assert len(batch) == batch_size
        return batch

    batch_mapper = BatchMapper(
        fn=check_batch_size, batch_size=batch_size, batch_format="pandas"
    )
    batch_mapper.fit(ds)
    transformed_ds = batch_mapper.transform(ds)
    out_df = transformed_ds.to_pandas()
    expected_df = ds.to_pandas()
    assert_frame_equal(out_df, expected_df)


@pytest.mark.parametrize(
    "ds_with_expected_pandas_numpy_df",
    [
        (
            lazy_fixture("ds_arrow_single_column_format"),
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
            lazy_fixture("ds_arrow_single_column_tensor_format"),
            pd.DataFrame(
                {
                    TENSOR_COLUMN_NAME: [
                        [[1, 2], [3, 4]],
                        [[5, 6], [7, 8]],
                        [[9, 10], [11, 12]],
                        [[13, 14], [15, 16]],
                    ]
                }
            ),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: TensorArray(np.arange(1, 17).reshape((4, 2, 2)))
                }
            ),
        ),
        (
            lazy_fixture("ds_arrow_multi_column_format"),
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
def test_batch_mapper_arrow_data_format(
    ray_start_regular_shared, ds_with_expected_pandas_numpy_df
):
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
            lazy_fixture("ds_numpy_single_column_tensor_format"),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: [
                        [[1, 2], [3, 4]],
                        [[5, 6], [7, 8]],
                        [[9, 10], [11, 12]],
                        [[13, 14], [15, 16]],
                    ]
                }
            ),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: TensorArray(np.arange(1, 17).reshape((4, 2, 2)))
                }
            ),
        ),
        (
            lazy_fixture("ds_numpy_list_of_ndarray_tensor_format"),
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
                        [[25, 26], [27, 28]],
                        [[29, 30], [31, 32]],
                    ]
                }
            ),
            pd.DataFrame(
                {
                    # Single column pandas automatically converts `TENSOR_COLUMN_NAME`
                    # In UDFs
                    TENSOR_COLUMN_NAME: TensorArray(np.arange(1, 33).reshape((8, 2, 2)))
                }
            ),
        ),
    ],
)
def test_batch_mapper_numpy_data_format(
    ray_start_regular_shared, ds_with_expected_pandas_numpy_df
):
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
