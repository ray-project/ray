import numpy as np
import pandas as pd
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
