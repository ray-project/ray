import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.preprocessors import PowerTransformer


def test_power_transformer():
    """Tests basic PowerTransformer functionality."""

    # yeo-johnson
    col_a = [-1, 0]
    col_b = [0, 1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    # yeo-johnson power=0
    transformer = PowerTransformer(["A", "B"], power=0)
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [-1.5, 0]
    processed_col_b = [0, np.log(2)]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    assert out_df.equals(expected_df)

    # yeo-johnson power=2
    transformer = PowerTransformer(["A", "B"], power=2)
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()
    processed_col_a = [-np.log(2), 0]
    processed_col_b = [0, 1.5]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    assert out_df.equals(expected_df)

    # box-cox
    col_a = [1, 2]
    col_b = [3, 4]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    # box-cox power=0
    transformer = PowerTransformer(["A", "B"], power=0, method="box-cox")
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [0, np.log(2)]
    processed_col_b = [np.log(3), np.log(4)]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    assert out_df.equals(expected_df)

    # box-cox power=2
    transformer = PowerTransformer(["A", "B"], power=2, method="box-cox")
    transformed = transformer.transform(ds)
    out_df = transformed.to_pandas()
    processed_col_a = [0, 1.5]
    processed_col_b = [4, 7.5]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    assert out_df.equals(expected_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
