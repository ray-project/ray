import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.preprocessors import Normalizer


def test_normalizer():
    """Tests basic Normalizer functionality."""

    col_a = [10, 10, 10]
    col_b = [1, 3, 3]
    col_c = [2, 4, -4]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    # l2 norm
    normalizer = Normalizer(["B", "C"])
    transformed = normalizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [1 / np.sqrt(5), 0.6, 0.6]
    processed_col_c = [2 / np.sqrt(5), 0.8, -0.8]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    assert out_df.equals(expected_df)

    # l1 norm
    normalizer = Normalizer(["B", "C"], norm="l1")

    transformed = normalizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [1 / 3, 3 / 7, 3 / 7]
    processed_col_c = [2 / 3, 4 / 7, -4 / 7]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    assert out_df.equals(expected_df)

    # max norm
    normalizer = Normalizer(["B", "C"], norm="max")

    transformed = normalizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [0.5, 0.75, 0.75]
    processed_col_c = [1.0, 1.0, -1.0]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    assert out_df.equals(expected_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
