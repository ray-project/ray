import numpy as np
import pandas as pd
import pytest
from typing import Dict
from pandas.testing import assert_frame_equal

import ray
from ray.data.preprocessors import BatchMapper

@pytest.fixture
def test_ds():
    old_column = [1, 2, 3, 4]
    to_be_modified = [1, -1, 1, -1]
    in_df = pd.DataFrame.from_dict(
        {"old_column": old_column, "to_be_modified": to_be_modified}
    )
    ds = ray.data.from_pandas(in_df)
    yield ds, old_column

def test_batch_mapper_pandas(test_ds):
    """Tests batch mapper functionality."""
    ds, old_column = test_ds

    def add_and_modify_udf(df: "pd.DataFrame"):
        df["new_col"] = df["old_column"] + 1
        df["to_be_modified"] *= 2
        return df

    batch_mapper = BatchMapper(fn=add_and_modify_udf)
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


def test_batch_mapper_numpy(test_ds):
    """Tests batch mapper functionality."""
    ds, old_column = test_ds

    def add_and_modify_udf(data: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        data["new_col"] = data["old_column"] + 1
        data["to_be_modified"] *= 2
        return data

    transformed = ds.map_batches(add_and_modify_udf, batch_format="numpy")
    out_df_map_batches = transformed.to_pandas()

    batch_mapper = BatchMapper(add_and_modify_udf)
    batch_mapper.fit(ds)
    transformed = batch_mapper.transform(ds)
    out_df_batch_mapper = transformed.to_pandas()

    expected_df = pd.DataFrame.from_dict(
        {
            "old_column": old_column,
            "to_be_modified": [2, -2, 2, -2],
            "new_col": [2, 3, 4, 5],
        }
    )

    assert_frame_equal(out_df_map_batches, expected_df)
    assert_frame_equal(out_df_batch_mapper, expected_df)

if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))