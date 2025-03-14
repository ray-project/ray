import pandas as pd
import pytest
import numpy as np

import ray
from ray.data.preprocessor import PreprocessorNotFittedException
from ray.data.preprocessors import (
    MaxAbsScaler,
    MinMaxScaler,
    RobustScaler,
    StandardScaler,
)


def test_min_max_scaler():
    """Tests basic MinMaxScaler functionality."""
    col_a = [-1, 0, 1]
    col_b = [1, 3, 5]
    col_c = [1, 1, None]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    scaler = MinMaxScaler(["B", "C"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        scaler.transform(ds)

    # Fit data.
    scaler.fit(ds)
    assert scaler.stats_ == {"min(B)": 1, "max(B)": 5, "min(C)": 1, "max(C)": 1}

    transformed = scaler.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [0.0, 0.5, 1.0]
    processed_col_c = [0.0, 0.0, None]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    pd.testing.assert_frame_equal(out_df, expected_df)

    # Transform batch.
    pred_col_a = [1, 2, 3]
    pred_col_b = [3, 5, 7]
    pred_col_c = [0, 1, 2]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [0.5, 1.0, 1.5]
    pred_processed_col_c = [-1.0, 0.0, 1.0]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df)

    # append mode
    with pytest.raises(ValueError):
        MinMaxScaler(columns=["B", "C"], output_columns=["B_mm_scaled"])

    scaler = MinMaxScaler(
        columns=["B", "C"], output_columns=["B_mm_scaled", "C_mm_scaled"]
    )
    scaler.fit(ds)

    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )
    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_col_a,
            "B": pred_col_b,
            "C": pred_col_c,
            "B_mm_scaled": pred_processed_col_b,
            "C_mm_scaled": pred_processed_col_c,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)


def test_max_abs_scaler():
    """Tests basic MaxAbsScaler functionality."""
    col_a = [-1, 0, 1]
    col_b = [1, 3, -5]
    col_c = [1, 1, None]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    scaler = MaxAbsScaler(["B", "C"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        scaler.transform(ds)

    # Fit data.
    scaler.fit(ds)
    assert scaler.stats_ == {"abs_max(B)": 5, "abs_max(C)": 1}

    transformed = scaler.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [0.2, 0.6, -1.0]
    processed_col_c = [1.0, 1.0, None]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Transform batch.
    pred_col_a = [1, 2, 3]
    pred_col_b = [3, 5, 7]
    pred_col_c = [0, 1, -2]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [0.6, 1.0, 1.4]
    pred_processed_col_c = [0.0, 1.0, -2.0]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)

    # append mode
    with pytest.raises(ValueError):
        MaxAbsScaler(columns=["B", "C"], output_columns=["B_ma_scaled"])

    scaler = MaxAbsScaler(
        columns=["B", "C"], output_columns=["B_ma_scaled", "C_ma_scaled"]
    )
    scaler.fit(ds)

    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )
    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_col_a,
            "B": pred_col_b,
            "C": pred_col_c,
            "B_ma_scaled": pred_processed_col_b,
            "C_ma_scaled": pred_processed_col_c,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)


def test_robust_scaler():
    """Tests basic RobustScaler functionality."""
    col_a = [-2, -1, 0, 1, 2]
    col_b = [-2, -1, 0, 1, 2]
    col_c = [-10, 1, 2, 3, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    scaler = RobustScaler(["B", "C"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        scaler.transform(ds)

    # Fit data.
    scaler.fit(ds)
    assert scaler.stats_ == {
        "low_quantile(B)": -1,
        "median(B)": 0,
        "high_quantile(B)": 1,
        "low_quantile(C)": 1,
        "median(C)": 2,
        "high_quantile(C)": 3,
    }

    transformed = scaler.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [-1.0, -0.5, 0, 0.5, 1.0]
    processed_col_c = [-6, -0.5, 0, 0.5, 4]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Transform batch.
    pred_col_a = [1, 2, 3]
    pred_col_b = [3, 5, 7]
    pred_col_c = [0, 1, 2]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [1.5, 2.5, 3.5]
    pred_processed_col_c = [-1.0, -0.5, 0.0]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)

    # append mode
    with pytest.raises(ValueError):
        RobustScaler(columns=["B", "C"], output_columns=["B_r_scaled"])

    scaler = RobustScaler(
        columns=["B", "C"], output_columns=["B_r_scaled", "C_r_scaled"]
    )
    scaler.fit(ds)

    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )
    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_col_a,
            "B": pred_col_b,
            "C": pred_col_c,
            "B_r_scaled": pred_processed_col_b,
            "C_r_scaled": pred_processed_col_c,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)


def test_standard_scaler():
    """Tests basic StandardScaler functionality."""
    col_a = [-1, 0, 1, 2]
    col_b = [1, 1, 5, 5]
    col_c = [1, 1, 1, None]
    col_d = [None, None, None, None]
    sample_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c, "D": col_d})
    ds = ray.data.from_pandas(sample_df)

    scaler = StandardScaler(["B", "C", "D"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        scaler.transform(ds)

    # Fit data.
    scaler = scaler.fit(ds)
    assert scaler.stats_ == {
        "mean(B)": 3.0,
        "mean(C)": 1.0,
        "mean(D)": None,
        "std(B)": 2.0,
        "std(C)": 0.0,
        "std(D)": None,
    }

    # Transform data.
    in_col_a = [-1, 0, 1, 2]
    in_col_b = [1, 1, 5, 5]
    in_col_c = [1, 1, 1, None]
    in_col_d = [0, None, None, None]
    in_df = pd.DataFrame.from_dict(
        {"A": in_col_a, "B": in_col_b, "C": in_col_c, "D": in_col_d}
    )
    in_ds = ray.data.from_pandas(in_df)
    transformed = scaler.transform(in_ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [-1.0, -1.0, 1.0, 1.0]
    processed_col_c = [0.0, 0.0, 0.0, None]
    processed_col_d = [np.nan, np.nan, np.nan, np.nan]
    expected_df = pd.DataFrame.from_dict(
        {
            "A": processed_col_a,
            "B": processed_col_b,
            "C": processed_col_c,
            "D": processed_col_d,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Transform batch.
    pred_col_a = [1, 2, 3]
    pred_col_b = [3, 5, 7]
    pred_col_c = [0, 1, 2]
    pred_col_d = [None, None, None]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c, "D": pred_col_d}
    )

    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [0.0, 1.0, 2.0]
    pred_processed_col_c = [-1.0, 0.0, 1.0]
    pred_processed_col_d = [None, None, None]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
            "D": pred_processed_col_d,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)

    # append mode
    with pytest.raises(ValueError):
        StandardScaler(columns=["B", "C"], output_columns=["B_s_scaled"])

    scaler = StandardScaler(
        columns=["B", "C"], output_columns=["B_s_scaled", "C_s_scaled"]
    )
    scaler.fit(ds)

    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )
    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_col_a,
            "B": pred_col_b,
            "C": pred_col_c,
            "B_s_scaled": pred_processed_col_b,
            "C_s_scaled": pred_processed_col_c,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
