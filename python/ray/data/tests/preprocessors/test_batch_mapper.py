from typing import Dict

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from pytest_lazyfixture import lazy_fixture

import ray
from ray.data.preprocessors import BatchMapper
from ray.tests.conftest import *  # noqa


def test_batch_mapper_basic(ray_start_regular_shared):
    """Tests batch mapper functionality."""
    old_column = [1, 2, 3, 4]
    to_be_modified = [1, -1, 1, -1]
    in_df = pd.DataFrame.from_dict(
        {"old_column": old_column, "to_be_modified": to_be_modified}
    )
    ds = ray.data.from_pandas(in_df)

    def add_and_modify_udf(df: "pd.DataFrame"):
        df["new_col"] = df["old_column"] + 1
        df["to_be_modified"] *= 2
        return df

    batch_mapper = BatchMapper(fn=add_and_modify_udf, batch_format="pandas")
    batch_mapper.fit(ds)
    transformed = batch_mapper.transform(ds)
    out_df = transformed.to_pandas()

    expected_df = pd.DataFrame.from_dict(
        {
            "old_column": old_column,
            "to_be_modified": [2, -2, 2, -2],
            "new_col": [2, 3, 4, 5],
        }
    )

    assert out_df.equals(expected_df)


@pytest.mark.parametrize(
    "ds,expected_df,expected_numpy_df",
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
                    "column_1": [2, 3, 4, 5],
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
    ray_start_regular_shared, ds, expected_df, expected_numpy_df
):
    def add_and_modify_udf_pandas(df: "pd.DataFrame"):
        df["column_1"] = df["column_1"] + 1
        if "column_2" in df:
            df["column_2"] *= 2
        return df

    def add_and_modify_udf_numpy(data: Dict[str, np.ndarray]):
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
        lazy_fixture("ds_pandas_list_multi_column_format"),
        lazy_fixture("ds_arrow_single_column_format"),
        lazy_fixture("ds_arrow_single_column_tensor_format"),
        lazy_fixture("ds_arrow_multi_column_format"),
        lazy_fixture("ds_list_arrow_multi_column_format"),
        lazy_fixture("ds_numpy_single_column_tensor_format"),
        lazy_fixture("ds_numpy_list_of_ndarray_tensor_format"),
    ],
)
def test_batch_mapper_batch_size(ray_start_regular_shared, ds):
    """Tests BatcMapper batch size."""

    batch_size = 2

    def check_batch_size(batch):
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
    "ds,expected_df,expected_numpy_df",
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
                    "column_1": [2, 3, 4, 5],
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
    ray_start_regular_shared, ds, expected_df, expected_numpy_df
):
    """Tests batch mapper functionality for arrow data format.

    Note:
        For single column pandas dataframes, we automatically convert it to
        single column tensor with column name as `__value__`.
    """

    def add_and_modify_udf_pandas(df: "pd.DataFrame"):
        col_name = "column_1"
        if len(df.columns) == 1:
            col_name = list(df.columns)[0]
        df[col_name] = df[col_name] + 1
        if "column_2" in df:
            df["column_2"] *= 2
        return df

    def add_and_modify_udf_numpy(data: Dict[str, np.ndarray]):
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
    "ds,expected_df",
    [
        (
            lazy_fixture("ds_numpy_single_column_tensor_format"),
            pd.DataFrame(
                {
                    "data": [
                        [[1, 2], [3, 4]],
                        [[5, 6], [7, 8]],
                        [[9, 10], [11, 12]],
                        [[13, 14], [15, 16]],
                    ]
                }
            ),
        ),
        (
            lazy_fixture("ds_numpy_list_of_ndarray_tensor_format"),
            pd.DataFrame({"data": [[[1, 2], [3, 4]]] * 4}),
        ),
    ],
)
def test_batch_mapper_numpy_data_format(ds, expected_df):
    def add_and_modify_udf_pandas(df: "pd.DataFrame"):
        col_name = list(df.columns)[0]
        df[col_name] = df[col_name] + 1
        return df

    def add_and_modify_udf_numpy(data: Dict[str, np.ndarray]):
        data["data"] = data["data"] + 1
        return data

    # Test map_batches
    transformed_ds = ds.map_batches(add_and_modify_udf_pandas, batch_format="pandas")
    out_df_map_batches = transformed_ds.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

    transformed_ds = ds.map_batches(add_and_modify_udf_numpy, batch_format="numpy")
    out_df_map_batches = transformed_ds.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

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
    assert_frame_equal(out_df, expected_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
