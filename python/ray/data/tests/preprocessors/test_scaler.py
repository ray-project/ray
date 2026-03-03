import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.preprocessor import (
    PreprocessorNotFittedException,
    SerializablePreprocessorBase,
)
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


def test_standard_scaler_arrow_transform():
    """Test the StandardScaler _transform_arrow method directly."""
    # Create test data
    col_a = ["red", "green", "blue", "red"]
    col_b = [1.0, 3.0, 5.0, 7.0]  # mean=4, std=2.236
    col_c = [10.0, 10.0, 10.0, 10.0]  # constant column, std=0
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})

    scaler = StandardScaler(["B", "C"])
    scaler.fit(ray.data.from_pandas(in_df))

    # Create Arrow table for transformation
    table = pa.Table.from_pandas(in_df)

    # Transform using Arrow
    result_table = scaler._transform_arrow(table)

    # Verify result is an Arrow table
    assert isinstance(result_table, pa.Table)

    # Convert to pandas for easier comparison
    result_df = result_table.to_pandas()

    # Expected encoding:
    # B: (x - mean(B)) / std(B)
    # C: std(C)=0 -> std becomes 1 -> (x - mean(C)) / 1 = 0 for all
    b_mean = scaler.stats_["mean(B)"]
    b_std = scaler.stats_["std(B)"] or 0.0
    if b_std == 0:
        b_std = 1
    expected_col_b = [(x - b_mean) / b_std for x in col_b]

    c_mean = scaler.stats_["mean(C)"]
    c_std = scaler.stats_["std(C)"] or 0.0
    if c_std == 0:
        c_std = 1
    expected_col_c = [(x - c_mean) / c_std for x in col_c]

    assert result_df["A"].tolist() == col_a, "Column A should be unchanged"
    assert np.allclose(
        result_df["B"].tolist(), expected_col_b
    ), f"Column B mismatch: {result_df['B'].tolist()}"
    assert np.allclose(
        result_df["C"].tolist(), expected_col_c
    ), f"Column C mismatch: {result_df['C'].tolist()}"


def test_standard_scaler_arrow_transform_append_mode():
    """Test the StandardScaler _transform_arrow method in append mode."""
    col_a = ["red", "green", "blue"]
    col_b = [1.0, 3.0, 5.0]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})

    scaler = StandardScaler(["B"], output_columns=["B_scaled"])
    scaler.fit(ray.data.from_pandas(in_df))

    table = pa.Table.from_pandas(in_df)
    result_table = scaler._transform_arrow(table)
    result_df = result_table.to_pandas()

    # Original columns should be unchanged
    assert result_df["A"].tolist() == col_a
    assert result_df["B"].tolist() == col_b

    # New column should have scaled values: (x - 3) / 2
    b_mean = scaler.stats_["mean(B)"]
    b_std = scaler.stats_["std(B)"] or 0.0
    if b_std == 0:
        b_std = 1
    expected_b_scaled = [(x - b_mean) / b_std for x in col_b]
    assert np.allclose(result_df["B_scaled"].tolist(), expected_b_scaled)


def test_standard_scaler_arrow_transform_null_stats():
    """Test the StandardScaler _transform_arrow method with null mean/std."""
    # Use an all-null column to produce null mean/std during fit.
    in_df = pd.DataFrame.from_dict({"A": [None, None, None]})

    scaler = StandardScaler(["A"])
    scaler.fit(ray.data.from_pandas(in_df))

    table = pa.Table.from_pandas(in_df)
    result_table = scaler._transform_arrow(table)
    result_df = result_table.to_pandas()

    # All values should be null when mean/std is None
    assert result_df["A"].isna().all(), "All values should be null when stats are None"


def test_standard_scaler_arrow_transform_overlapping_columns():
    """Test StandardScaler _transform_arrow with overlapping input/output columns.

    This tests the case where output_columns[i] == columns[j] for i < j.
    The Arrow implementation must read all input columns before writing any output
    to avoid corrupting data that will be read later.
    """
    # columns=['A', 'B'], output_columns=['B', 'C']
    # Without the fix, B would be overwritten before being read as input
    col_a = [2.0, 4.0, 6.0]  # mean=4, std=2 -> scaled: [-1, 0, 1]
    col_b = [10.0, 20.0, 30.0]  # mean=20, std=10 -> scaled: [-1, 0, 1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})

    scaler = StandardScaler(["A", "B"], output_columns=["B", "C"])
    scaler.fit(ray.data.from_pandas(in_df))

    # Test Arrow transform
    table = pa.Table.from_pandas(in_df)
    result_table = scaler._transform_arrow(table)
    result_df = result_table.to_pandas()

    # Test pandas transform for comparison
    pandas_result = scaler._transform_pandas(in_df.copy())

    # Column A should be unchanged (not in output_columns with same index)
    assert result_df["A"].tolist() == col_a, "Column A should be unchanged"

    # Column B should contain scaled A: (A - 4) / 2 = [-1, 0, 1]
    a_mean = scaler.stats_["mean(A)"]
    a_std = scaler.stats_["std(A)"] or 0.0
    if a_std == 0:
        a_std = 1
    expected_b = [(x - a_mean) / a_std for x in col_a]
    assert np.allclose(result_df["B"].tolist(), expected_b), (
        f"Column B should contain scaled A. Expected {expected_b}, "
        f"got {result_df['B'].tolist()}"
    )

    # Column C should contain scaled B: (B - 20) / 10 = [-1, 0, 1]
    b_mean = scaler.stats_["mean(B)"]
    b_std = scaler.stats_["std(B)"] or 0.0
    if b_std == 0:
        b_std = 1
    expected_c = [(x - b_mean) / b_std for x in col_b]
    assert np.allclose(result_df["C"].tolist(), expected_c), (
        f"Column C should contain scaled B. Expected {expected_c}, "
        f"got {result_df['C'].tolist()}"
    )

    # Arrow and pandas results should match
    pd.testing.assert_frame_equal(
        result_df,
        pandas_result,
        check_like=True,
        obj="Arrow vs Pandas transform results should match",
    )


class TestScalerSerialization:
    """Test serialization/deserialization functionality for scaler preprocessors."""

    def setup_method(self):
        """Set up test data."""
        self.test_df = pd.DataFrame(
            {
                "feature1": [1, 2, 3, 4, 5],
                "feature2": [10, 20, 30, 40, 50],
                "feature3": [100, 200, 300, 400, 500],
                "other": ["a", "b", "c", "d", "e"],
            }
        )
        self.test_dataset = ray.data.from_pandas(self.test_df)

    @pytest.mark.parametrize(
        "scaler_class,fit_data,expected_stats,transform_data",
        [
            (
                StandardScaler,
                None,  # Use default self.test_df
                {
                    "mean(feature1)": 3.0,
                    "mean(feature2)": 30.0,
                    "std(feature1)": np.sqrt(2.0),
                    "std(feature2)": np.sqrt(200.0),
                },
                pd.DataFrame(
                    {
                        "feature1": [6, 7, 8],
                        "feature2": [60, 70, 80],
                        "other": ["f", "g", "h"],
                    }
                ),
            ),
            (
                MinMaxScaler,
                None,  # Use default self.test_df
                {
                    "min(feature1)": 1,
                    "min(feature2)": 10,
                    "max(feature1)": 5,
                    "max(feature2)": 50,
                },
                pd.DataFrame(
                    {
                        "feature1": [6, 7, 8],
                        "feature2": [60, 70, 80],
                        "other": ["f", "g", "h"],
                    }
                ),
            ),
            (
                MaxAbsScaler,
                pd.DataFrame(
                    {
                        "feature1": [-5, -2, 0, 2, 5],
                        "feature2": [-50, -20, 0, 20, 50],
                        "other": ["a", "b", "c", "d", "e"],
                    }
                ),
                {
                    "abs_max(feature1)": 5,
                    "abs_max(feature2)": 50,
                },
                pd.DataFrame(
                    {
                        "feature1": [-6, 0, 6],
                        "feature2": [-60, 0, 60],
                        "other": ["f", "g", "h"],
                    }
                ),
            ),
            (
                RobustScaler,
                None,  # Use default self.test_df
                {
                    "low_quantile(feature1)": 2.0,
                    "median(feature1)": 3.0,
                    "high_quantile(feature1)": 4.0,
                    "low_quantile(feature2)": 20.0,
                    "median(feature2)": 30.0,
                    "high_quantile(feature2)": 40.0,
                },
                pd.DataFrame(
                    {
                        "feature1": [6, 7, 8],
                        "feature2": [60, 70, 80],
                        "other": ["f", "g", "h"],
                    }
                ),
            ),
        ],
        ids=["StandardScaler", "MinMaxScaler", "MaxAbsScaler", "RobustScaler"],
    )
    def test_scaler_serialization(
        self, scaler_class, fit_data, expected_stats, transform_data
    ):
        """Test scaler serialization for all scaler types."""
        # Use custom fit data if provided, otherwise use default test dataset
        if fit_data is not None:
            fit_dataset = ray.data.from_pandas(fit_data)
        else:
            fit_dataset = self.test_dataset

        # Create and fit scaler
        scaler = scaler_class(columns=["feature1", "feature2"])
        fitted_scaler = scaler.fit(fit_dataset)

        # Verify fitted stats match expected values
        assert fitted_scaler.stats_ == expected_stats, (
            f"Stats mismatch for {scaler_class.__name__}:\n"
            f"Expected: {expected_stats}\n"
            f"Got: {fitted_scaler.stats_}"
        )

        # Test CloudPickle serialization
        serialized = fitted_scaler.serialize()
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

        # Test deserialization
        deserialized = SerializablePreprocessorBase.deserialize(serialized)
        assert deserialized.__class__.__name__ == scaler_class.__name__
        assert deserialized.columns == ["feature1", "feature2"]
        assert deserialized._fitted

        # Verify stats are preserved after deserialization
        assert deserialized.stats_ == expected_stats, (
            f"Deserialized stats mismatch for {scaler_class.__name__}:\n"
            f"Expected: {expected_stats}\n"
            f"Got: {deserialized.stats_}"
        )

        # Verify each stat key exists and has correct value
        for stat_key, stat_value in expected_stats.items():
            assert stat_key in deserialized.stats_
            if isinstance(stat_value, float):
                assert np.isclose(deserialized.stats_[stat_key], stat_value)
            else:
                assert deserialized.stats_[stat_key] == stat_value

        # Test functional equivalence
        original_result = fitted_scaler.transform_batch(transform_data.copy())
        deserialized_result = deserialized.transform_batch(transform_data.copy())

        pd.testing.assert_frame_equal(original_result, deserialized_result)

    def test_scaler_with_output_columns_serialization(self):
        """Test scaler serialization with custom output columns."""
        # Test with StandardScaler and output columns
        scaler = StandardScaler(
            columns=["feature1", "feature2"],
            output_columns=["scaled_feature1", "scaled_feature2"],
        )
        fitted_scaler = scaler.fit(self.test_dataset)

        # Serialize and deserialize
        serialized = fitted_scaler.serialize()
        deserialized = SerializablePreprocessorBase.deserialize(serialized)

        # Verify output columns are preserved
        assert deserialized.output_columns == ["scaled_feature1", "scaled_feature2"]

        # Test functional equivalence
        test_df = pd.DataFrame(
            {"feature1": [6, 7, 8], "feature2": [60, 70, 80], "other": ["f", "g", "h"]}
        )

        original_result = fitted_scaler.transform_batch(test_df.copy())
        deserialized_result = deserialized.transform_batch(test_df.copy())

        pd.testing.assert_frame_equal(original_result, deserialized_result)

    @pytest.mark.parametrize(
        "scaler_class",
        [StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler],
        ids=["StandardScaler", "MinMaxScaler", "MaxAbsScaler", "RobustScaler"],
    )
    def test_unfitted_scaler_serialization(self, scaler_class):
        """Test serialization of unfitted scalers."""
        # Test unfitted scaler
        scaler = scaler_class(columns=["feature1", "feature2"])

        # Serialize unfitted scaler
        serialized = scaler.serialize()
        deserialized = SerializablePreprocessorBase.deserialize(serialized)

        # Verify it's still unfitted
        assert not deserialized._fitted
        assert deserialized.columns == ["feature1", "feature2"]
        assert deserialized.__class__.__name__ == scaler_class.__name__

        # Should raise error when trying to transform
        test_df = pd.DataFrame({"feature1": [1, 2, 3], "feature2": [10, 20, 30]})
        with pytest.raises(PreprocessorNotFittedException):
            deserialized.transform_batch(test_df)

    @pytest.mark.parametrize(
        "scaler_class,expected_stats",
        [
            (
                StandardScaler,
                {
                    "mean(feature1)": 3.0,
                    "std(feature1)": np.sqrt(2.0),
                },
            ),
            (
                MinMaxScaler,
                {
                    "min(feature1)": 1,
                    "max(feature1)": 5,
                },
            ),
            (
                MaxAbsScaler,
                {
                    "abs_max(feature1)": 5,
                },
            ),
            (
                RobustScaler,
                {
                    "low_quantile(feature1)": 2.0,
                    "median(feature1)": 3.0,
                    "high_quantile(feature1)": 4.0,
                },
            ),
        ],
        ids=["StandardScaler", "MinMaxScaler", "MaxAbsScaler", "RobustScaler"],
    )
    def test_scaler_stats_preservation(self, scaler_class, expected_stats):
        """Test that scaler statistics are perfectly preserved during serialization."""
        # Create scaler with known stats
        scaler = scaler_class(columns=["feature1"])
        fitted_scaler = scaler.fit(self.test_dataset)

        # Verify fitted stats match expected values
        for stat_key, stat_value in expected_stats.items():
            assert stat_key in fitted_scaler.stats_
            if isinstance(stat_value, float):
                assert np.isclose(fitted_scaler.stats_[stat_key], stat_value)
            else:
                assert fitted_scaler.stats_[stat_key] == stat_value

        # Get original stats
        original_stats = fitted_scaler.stats_.copy()

        # Serialize and deserialize
        serialized = fitted_scaler.serialize()
        deserialized = SerializablePreprocessorBase.deserialize(serialized)

        # Verify stats are identical
        assert deserialized.stats_ == original_stats

        # Verify expected stat values are preserved
        for stat_key, stat_value in expected_stats.items():
            assert stat_key in deserialized.stats_
            if isinstance(stat_value, float):
                assert np.isclose(deserialized.stats_[stat_key], stat_value)
            else:
                assert deserialized.stats_[stat_key] == stat_value

    @pytest.mark.parametrize(
        "scaler_class",
        [StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler],
        ids=["StandardScaler", "MinMaxScaler", "MaxAbsScaler", "RobustScaler"],
    )
    def test_scaler_version_compatibility(self, scaler_class):
        """Test that scalers can be deserialized with version support."""
        # Create and fit scaler
        scaler = scaler_class(columns=["feature1", "feature2"])
        fitted_scaler = scaler.fit(self.test_dataset)

        # Serialize
        serialized = fitted_scaler.serialize()

        # Deserialize and verify version handling
        deserialized = SerializablePreprocessorBase.deserialize(serialized)
        assert deserialized.__class__.__name__ == scaler_class.__name__
        assert deserialized._fitted

        # Test that it works correctly
        test_df = pd.DataFrame({"feature1": [6, 7, 8], "feature2": [60, 70, 80]})

        result = deserialized.transform_batch(test_df)
        assert len(result.columns) == 2  # Should have the scaled columns
        assert "feature1" in result.columns
        assert "feature2" in result.columns


def test_standard_scaler_near_zero_std():
    """Test StandardScaler handles near-zero standard deviation correctly."""
    # Create data with very small standard deviation (near-constant values)
    col_a = [1.0, 1.0 + 1e-10, 1.0]
    col_b = [5, 10, 15]  # Normal column for comparison
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    scaler = StandardScaler(["A", "B"])
    scaler.fit(ds)
    transformed = scaler.transform(ds)
    out_df = transformed.to_pandas()

    # Column A should be scaled to zeros (near-constant)
    # Instead of NaN or inf values
    assert np.allclose(
        out_df["A"], 0.0, atol=1e-6
    ), "Near-constant column should be scaled to zeros"

    # Column B should be normally scaled
    assert not np.allclose(out_df["B"], 0.0), "Normal column should not be all zeros"

    # No NaN or inf values should be present
    assert not out_df["A"].isna().any(), "Should not contain NaN values"
    assert not np.isinf(out_df["A"]).any(), "Should not contain inf values"


def test_min_max_scaler_near_zero_range():
    """Test MinMaxScaler handles near-zero range correctly."""
    # Create data with very small range (near-constant values)
    col_a = [2.0, 2.0 + 1e-10, 2.0]
    col_b = [1, 5, 10]  # Normal column for comparison
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    scaler = MinMaxScaler(["A", "B"])
    scaler.fit(ds)
    transformed = scaler.transform(ds)
    out_df = transformed.to_pandas()

    # Column A should be scaled to zeros (near-constant)
    # Instead of NaN or inf values
    assert np.allclose(
        out_df["A"], 0.0, atol=1e-6
    ), "Near-constant column should be scaled to zeros"

    # Column B should be normally scaled
    expected_b = [0.0, 4 / 9, 1.0]
    assert np.allclose(
        out_df["B"], expected_b, atol=1e-6
    ), "Normal column should be scaled correctly"

    # No NaN or inf values should be present
    assert not out_df["A"].isna().any(), "Should not contain NaN values"
    assert not np.isinf(out_df["A"]).any(), "Should not contain inf values"


def test_standard_scaler_exact_zero_std():
    """Test StandardScaler still handles exact zero standard deviation.

    This is a regression test to ensure the epsilon-based handling
    doesn't break the existing behavior for exact zero std.
    """
    # Create constant column (exact zero std)
    col_c = [5, 5, 5]
    in_df = pd.DataFrame.from_dict({"C": col_c})
    ds = ray.data.from_pandas(in_df)

    scaler = StandardScaler(["C"])
    scaler.fit(ds)
    transformed = scaler.transform(ds)
    out_df = transformed.to_pandas()

    # Should be all zeros
    assert np.allclose(out_df["C"], 0.0), "Constant column should be scaled to zeros"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
