import numpy as np
import pandas as pd
import pyarrow as pa

import pytest
from typing import Dict
from pandas.testing import assert_frame_equal

import ray
from ray.data.preprocessors import BatchMapper


@pytest.fixture
def test_ds_pandas_format():
    old_column = [1, 2, 3, 4]
    to_be_modified = [1, -1, 1, -1]
    in_df = pd.DataFrame.from_dict(
        {"old_column": old_column, "to_be_modified": to_be_modified}
    )
    ds = ray.data.from_pandas(in_df)
    yield ds, old_column


@pytest.fixture
def test_ds_multi_column_arrow_format():
    old_column = [1, 2, 3, 4]
    to_be_modified = [1, -1, 1, -1]
    ds = ray.data.from_arrow(
        pa.table(
            {
                "old_column": old_column,
                "to_be_modified": to_be_modified,
            }
        )
    )
    yield ds, old_column


def test_batch_mapper_pandas_data_format(test_ds_pandas_format):
    """Tests batch mapper functionality."""
    ds, old_column = test_ds_pandas_format

    def add_and_modify_udf_pandas(df: "pd.DataFrame"):
        df["new_col"] = df["old_column"] + 1
        df["to_be_modified"] *= 2
        return df

    def add_and_modify_udf_numpy(data: Dict[str, np.ndarray]):
        data["new_col"] = data["old_column"] + 1
        data["to_be_modified"] *= 2
        return data

    expected_df = pd.DataFrame.from_dict(
        {
            "old_column": old_column,
            "to_be_modified": [2, -2, 2, -2],
            "new_col": [2, 3, 4, 5],
        }
    )

    # Test map_batches
    transformed = ds.map_batches(add_and_modify_udf_numpy, batch_format="numpy")
    out_df_map_batches = transformed.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

    transformed = ds.map_batches(add_and_modify_udf_pandas, batch_format="pandas")
    out_df_map_batches = transformed.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

    # Test BatchMapper
    batch_mapper = BatchMapper(fn=add_and_modify_udf_pandas, batch_format="pandas")
    batch_mapper.fit(ds)
    transformed = batch_mapper.transform(ds)
    out_df = transformed.to_pandas()
    assert_frame_equal(out_df_map_batches, out_df)

    batch_mapper = BatchMapper(fn=add_and_modify_udf_numpy, batch_format="numpy")
    batch_mapper.fit(ds)
    transformed = batch_mapper.transform(ds)
    out_df = transformed.to_pandas()
    assert_frame_equal(out_df_map_batches, out_df)


def test_batch_mapper_arrow_data_format(test_ds_multi_column_arrow_format):
    """Tests batch mapper functionality."""
    ds, old_column = test_ds_multi_column_arrow_format

    def add_and_modify_udf_pandas(df: "pd.DataFrame"):
        df["new_col"] = df["old_column"] + 1
        df["to_be_modified"] *= 2
        return df

    def add_and_modify_udf_numpy(data: Dict[str, np.ndarray]):
        data["new_col"] = data["old_column"] + 1
        # Arrow's numpy output array is read-only
        data["to_be_modified"] = data["to_be_modified"] * 2
        return data

    expected_df = pd.DataFrame.from_dict(
        {
            "old_column": old_column,
            "to_be_modified": [2, -2, 2, -2],
            "new_col": [2, 3, 4, 5],
        }
    )

    # Test map_batches
    transformed = ds.map_batches(add_and_modify_udf_numpy, batch_format="numpy")
    out_df_map_batches = transformed.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

    transformed = ds.map_batches(add_and_modify_udf_pandas, batch_format="pandas")
    out_df_map_batches = transformed.to_pandas()
    assert_frame_equal(out_df_map_batches, expected_df)

    # Test BatchMapper
    batch_mapper = BatchMapper(fn=add_and_modify_udf_pandas, batch_format="pandas")
    batch_mapper.fit(ds)
    transformed = batch_mapper.transform(ds)
    out_df = transformed.to_pandas()
    assert_frame_equal(out_df_map_batches, out_df)

    batch_mapper = BatchMapper(fn=add_and_modify_udf_numpy, batch_format="numpy")
    batch_mapper.fit(ds)
    transformed = batch_mapper.transform(ds)
    out_df = transformed.to_pandas()
    assert_frame_equal(out_df_map_batches, out_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
