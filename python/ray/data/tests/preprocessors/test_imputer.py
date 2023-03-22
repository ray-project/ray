import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.preprocessor import PreprocessorNotFittedException
from ray.data.preprocessors import SimpleImputer


def test_simple_imputer():
    col_a = [1, 1, 1, np.nan]
    col_b = [1, 3, None, np.nan]
    col_c = [1, 1, 1, 1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})

    ds = ray.data.from_pandas(in_df)

    imputer = SimpleImputer(["B", "C"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        imputer.transform(ds)

    # Fit data.
    imputer.fit(ds)
    assert imputer.stats_ == {"mean(B)": 2.0, "mean(C)": 1.0}

    # Transform data.
    transformed = imputer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [1.0, 3.0, 2.0, 2.0]
    processed_col_c = [1, 1, 1, 1]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    assert out_df.equals(expected_df)

    # Transform batch.
    pred_col_a = [1, 2, np.nan]
    pred_col_b = [1, 2, np.nan]
    pred_col_c = [None, None, None]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = imputer.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [1.0, 2.0, 2.0]
    pred_processed_col_c = [1.0, 1.0, 1.0]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    )

    assert pred_out_df.equals(pred_expected_df)

    # Test "most_frequent" strategy.
    most_frequent_col_a = [1, 2, 2, None, None, None]
    most_frequent_col_b = [None, "c", "c", "b", "b", "a"]
    most_frequent_df = pd.DataFrame.from_dict(
        {"A": most_frequent_col_a, "B": most_frequent_col_b}
    )
    most_frequent_ds = ray.data.from_pandas(most_frequent_df).repartition(3)

    most_frequent_imputer = SimpleImputer(["A", "B"], strategy="most_frequent")
    most_frequent_imputer.fit(most_frequent_ds)
    assert most_frequent_imputer.stats_ == {
        "most_frequent(A)": 2.0,
        "most_frequent(B)": "c",
    }

    most_frequent_transformed = most_frequent_imputer.transform(most_frequent_ds)
    most_frequent_out_df = most_frequent_transformed.to_pandas()

    most_frequent_processed_col_a = [1.0, 2.0, 2.0, 2.0, 2.0, 2.0]
    most_frequent_processed_col_b = ["c", "c", "c", "b", "b", "a"]
    most_frequent_expected_df = pd.DataFrame.from_dict(
        {"A": most_frequent_processed_col_a, "B": most_frequent_processed_col_b}
    )

    assert most_frequent_out_df.equals(most_frequent_expected_df)

    # Test "constant" strategy.
    constant_col_a = ["apple", None]
    constant_col_b = constant_col_a.copy()
    constant_df = pd.DataFrame.from_dict({"A": constant_col_a, "B": constant_col_b})
    # category dtype requires special handling
    constant_df["B"] = constant_df["B"].astype("category")
    constant_ds = ray.data.from_pandas(constant_df)

    with pytest.raises(ValueError):
        SimpleImputer(["A", "B"], strategy="constant")

    constant_imputer = SimpleImputer(
        ["A", "B"], strategy="constant", fill_value="missing"
    )
    constant_transformed = constant_imputer.transform(constant_ds)
    constant_out_df = constant_transformed.to_pandas()

    constant_processed_col_a = ["apple", "missing"]
    constant_processed_col_b = constant_processed_col_a.copy()
    constant_expected_df = pd.DataFrame.from_dict(
        {"A": constant_processed_col_a, "B": constant_processed_col_b}
    )
    constant_expected_df["B"] = constant_expected_df["B"].astype("category")

    assert constant_out_df.equals(constant_expected_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
