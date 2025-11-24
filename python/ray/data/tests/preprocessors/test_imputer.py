"""
Tests for SimpleImputer functionality and serialization.

This file contains:
1. Basic functional tests for SimpleImputer operations
2. Comprehensive serialization/deserialization tests
"""
import tempfile
import time

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data._internal.util import rows_same
from ray.data.preprocessor import (
    PreprocessorNotFittedException,
    SerializablePreprocessorBase,
)
from ray.data.preprocessors import SimpleImputer
from ray.data.preprocessors.version_support import UnknownPreprocessorError


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

    pd.testing.assert_frame_equal(out_df, expected_df)

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

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)

    # with missing column
    pred_in_df = pd.DataFrame.from_dict({"A": pred_col_a, "B": pred_col_b})
    pred_out_df = imputer.transform_batch(pred_in_df)
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
        SimpleImputer(columns=["B", "C"], output_columns=["B_encoded"])

    imputer = SimpleImputer(
        columns=["B", "C"],
        output_columns=["B_imputed", "C_imputed"],
    )
    imputer.fit(ds)

    pred_col_a = [1, 2, np.nan]
    pred_col_b = [1, 2, np.nan]
    pred_col_c = [None, None, None]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )
    pred_out_df = imputer.transform_batch(pred_in_df)

    pred_processed_col_b = [1.0, 2.0, 2.0]
    pred_processed_col_c = [1.0, 1.0, 1.0]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_col_a,
            "B": pred_col_b,
            "C": pred_col_c,
            "B_imputed": pred_processed_col_b,
            "C_imputed": pred_processed_col_c,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)

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

    assert rows_same(most_frequent_out_df, most_frequent_expected_df)

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

    pd.testing.assert_frame_equal(
        constant_out_df, constant_expected_df, check_like=True
    )


def test_imputer_all_nan_raise_error():
    data = {
        "A": [np.nan, np.nan, np.nan, np.nan],
    }
    df = pd.DataFrame(data)
    dataset = ray.data.from_pandas(df)

    imputer = SimpleImputer(columns=["A"], strategy="mean")
    imputer.fit(dataset)

    with pytest.raises(ValueError):
        imputer.transform_batch(df)


def test_imputer_constant_categorical():
    data = {
        "A_cat": ["one", "two", None, "four"],
    }
    df = pd.DataFrame(data)
    df["A_cat"] = df["A_cat"].astype("category")
    dataset = ray.data.from_pandas(df)

    imputer = SimpleImputer(columns=["A_cat"], strategy="constant", fill_value="three")
    imputer.fit(dataset)

    transformed_df = imputer.transform_batch(df)

    expected = {
        "A_cat": ["one", "two", "three", "four"],
    }

    for column in data.keys():
        np.testing.assert_array_equal(transformed_df[column].values, expected[column])

    df = pd.DataFrame({"A": [1, 2, 3, 4]})
    transformed_df = imputer.transform_batch(df)

    expected = {
        "A": [1, 2, 3, 4],
        "A_cat": ["three", "three", "three", "three"],
    }

    for column in df:
        np.testing.assert_array_equal(transformed_df[column].values, expected[column])


class TestSimpleImputerSerialization:
    """Test CloudPickle-based serialization/deserialization functionality for SimpleImputer."""

    def setup_method(self):
        """Set up test data."""
        self.df_numeric = pd.DataFrame(
            {
                "temp": [20.0, 25.0, None, 30.0, None],
                "humidity": [60.0, None, 70.0, 80.0, 65.0],
                "other": ["a", "b", "c", "d", "e"],  # Non-processed column
            }
        )

    def test_basic_serialization(self):
        """Test basic serialization and deserialization functionality."""
        # Create and fit a simple imputer
        imputer = SimpleImputer(columns=["temp", "humidity"], strategy="mean")

        # Create test data
        df = pd.DataFrame(
            {
                "temp": [1.0, 2.0, None, 4.0],
                "humidity": [None, 2.0, 3.0, 4.0],
                "other": [1, 2, 3, 4],
            }
        )

        # Fit the imputer
        dataset = ray.data.from_pandas(df)
        fitted_imputer = imputer.fit(dataset)

        # Serialize using CloudPickle (primary format)
        serialized = fitted_imputer.serialize()

        # Verify it's binary CloudPickle format
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

        # Deserialize
        deserialized = SimpleImputer.deserialize(serialized)

        # Verify type and state
        assert isinstance(deserialized, SimpleImputer)
        assert deserialized._fitted
        assert deserialized.columns == ["temp", "humidity"]
        assert deserialized.strategy == "mean"

        # Verify stats are preserved
        assert "mean(temp)" in deserialized.stats_
        assert "mean(humidity)" in deserialized.stats_
        assert abs(deserialized.stats_["mean(temp)"] - 2.333333) < 0.001
        assert abs(deserialized.stats_["mean(humidity)"] - 3.0) < 0.001

    def test_serialization_formats(self):
        """Test serialization and deserialization."""
        imputer = SimpleImputer(columns=["temp"], strategy="mean")
        dataset = ray.data.from_pandas(self.df_numeric)
        fitted_imputer = imputer.fit(dataset)

        # Test CloudPickle format (default)
        serialized = fitted_imputer.serialize()
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

        # Deserialize and verify it works
        deserialized = SimpleImputer.deserialize(serialized)

        # Verify it works correctly
        test_df = pd.DataFrame({"temp": [None, 35.0], "other": [1, 2]})
        result = deserialized.transform_batch(test_df.copy())

        # Verify the result has the expected structure
        assert "temp" in result.columns
        assert "other" in result.columns

    def test_functional_equivalence(self):
        """Test that deserialized SimpleImputer works identically to original."""
        # Create and fit original
        imputer = SimpleImputer(columns=["value"], strategy="mean")
        train_df = pd.DataFrame({"value": [10, 20, None, 40], "id": [1, 2, 3, 4]})
        train_dataset = ray.data.from_pandas(train_df)
        fitted_imputer = imputer.fit(train_dataset)

        # Test data
        test_df = pd.DataFrame({"value": [None, 50, None], "id": [5, 6, 7]})

        # Transform with original
        original_result = fitted_imputer.transform_batch(test_df.copy())

        # Serialize, deserialize, and transform (using CloudPickle)
        serialized = fitted_imputer.serialize()
        deserialized = SerializablePreprocessorBase.deserialize(serialized)
        deserialized_result = deserialized.transform_batch(test_df.copy())

        # Results should be identical
        pd.testing.assert_frame_equal(original_result, deserialized_result)

        # Verify specific values
        expected_mean = (10 + 20 + 40) / 3  # 23.333...
        assert abs(original_result.iloc[0]["value"] - expected_mean) < 1e-10
        assert abs(deserialized_result.iloc[0]["value"] - expected_mean) < 1e-10

    def test_complex_stats_preservation(self):
        """Test that CloudPickle perfectly preserves complex stats with various key types."""
        imputer = SimpleImputer(columns=["A"], strategy="mean")

        # Manually set complex stats that would be problematic for other formats
        imputer.stats_ = {
            # Simple stats
            "mean(A)": 5.0,
            "count(A)": 100,
            # Complex key types that CloudPickle handles natively
            "unique_values(ints)": {1: 0, 2: 1, 3: 2, 4: 3, 5: 4},  # int keys
            "unique_values(floats)": {1.1: 0, 2.2: 1, 3.3: 2},  # float keys
            "unique_values(bools)": {True: 0, False: 1},  # bool keys
            "unique_values(none)": {None: 0},  # None keys
            "unique_values(tuples)": {
                ("red", "car"): 0,
                ("blue", "bike"): 1,
                (1, 2, 3): 2,
                ("nested", ("inner", "tuple")): 3,
            },
            "unique_values(sets)": {
                frozenset([1, 2, 3]): 0,
                frozenset(["a", "b"]): 1,
            },
            "unique_values(mixed)": {
                "string": 0,
                42: 1,
                (1, 2): 2,
                frozenset([3, 4]): 3,
                None: 4,
                True: 5,
            },
        }
        imputer._fitted = True

        # Serialize and deserialize (using CloudPickle)
        serialized = imputer.serialize()
        deserialized = SimpleImputer.deserialize(serialized)

        # Verify ALL stats are perfectly preserved
        assert deserialized.stats_ == imputer.stats_

        # Verify specific complex key preservation
        for stat_name, stat_dict in imputer.stats_.items():
            if isinstance(stat_dict, dict):
                original_keys = set(stat_dict.keys())
                restored_keys = set(deserialized.stats_[stat_name].keys())

                # Keys should be identical (including types)
                assert original_keys == restored_keys

                # Values should be identical
                for key in original_keys:
                    assert stat_dict[key] == deserialized.stats_[stat_name][key]

                # Key types should be preserved
                for orig_key, rest_key in zip(original_keys, restored_keys):
                    if orig_key == rest_key:  # Same key
                        assert type(orig_key) is type(rest_key)

    def test_performance_comparison(self):
        """Test CloudPickle performance and simplicity."""
        # Create a large imputer with many stats
        imputer = SimpleImputer(
            columns=[f"col_{i}" for i in range(10)], strategy="mean"
        )

        # Create large stats dictionary
        large_stats = {}
        for i in range(10):
            large_stats[f"mean(col_{i})"] = float(i)
            large_stats[f"count(col_{i})"] = 1000 + i

            # Add complex key stats that CloudPickle handles natively
            large_stats[f"unique_values(col_{i})"] = {
                (f"key_{j}", j): j for j in range(100)  # 100 tuple keys per column
            }

        imputer.stats_ = large_stats
        imputer._fitted = True

        # Test serialization performance and correctness (using CloudPickle)
        start_time = time.time()
        serialized = imputer.serialize()
        serialize_time = time.time() - start_time

        start_time = time.time()
        deserialized = SimpleImputer.deserialize(serialized)
        deserialize_time = time.time() - start_time

        # Verify correctness
        assert deserialized.stats_ == imputer.stats_
        assert len(deserialized.stats_) == len(imputer.stats_)

        # Performance should be reasonable (less than 1 second for this size)
        assert serialize_time < 1.0
        assert deserialize_time < 1.0

        # Verify no data loss with complex keys
        for stat_name in large_stats:
            if "unique_values" in stat_name:
                original_keys = set(large_stats[stat_name].keys())
                restored_keys = set(deserialized.stats_[stat_name].keys())
                assert original_keys == restored_keys

    def test_cloudpickle_native_support(self):
        """Test that CloudPickle handles all Python types natively without transformation."""
        imputer = SimpleImputer(columns=["A"], strategy="mean")

        # Test all the key types that used to require custom transformation
        test_keys = [
            # Basic types
            "string_key",
            42,  # int
            3.14,  # float
            True,  # bool
            False,  # bool
            None,  # None
            # Complex types that CloudPickle handles natively
            (1, 2, 3),  # tuple
            ("nested", ("inner", "tuple")),  # nested tuple
            frozenset([1, 2, 3]),  # frozenset
            frozenset(["a", "b"]),  # frozenset with strings
        ]

        # Create stats with all these key types
        imputer.stats_ = {
            "test_dict": {key: f"value_{i}" for i, key in enumerate(test_keys)}
        }
        imputer._fitted = True

        # Serialize and deserialize (using CloudPickle)
        serialized = imputer.serialize()
        deserialized = SimpleImputer.deserialize(serialized)

        # Verify perfect preservation
        original_dict = imputer.stats_["test_dict"]
        restored_dict = deserialized.stats_["test_dict"]

        assert len(original_dict) == len(restored_dict)

        # Check each key-value pair and key type preservation
        for orig_key, orig_value in original_dict.items():
            # Key should exist and have same value
            assert orig_key in restored_dict
            assert restored_dict[orig_key] == orig_value

            # Find the corresponding restored key to check type
            for rest_key in restored_dict.keys():
                if rest_key == orig_key:
                    assert type(orig_key) is type(rest_key)
                    break

    def test_edge_case_empty_stats(self):
        """Test serialization with empty stats."""
        imputer = SimpleImputer(columns=["A"], strategy="constant", fill_value=0)
        # Constant strategy doesn't need fitting, so stats will be empty

        serialized = imputer.serialize()
        deserialized = SimpleImputer.deserialize(serialized)

        assert deserialized.stats_ == {}
        assert deserialized.strategy == "constant"
        assert deserialized.fill_value == 0
        assert deserialized._is_fittable is False

    def test_edge_case_none_values(self):
        """Test serialization with None values in stats."""
        imputer = SimpleImputer(columns=["A"], strategy="mean")
        imputer._fitted = True
        imputer.stats_ = {
            "mean(A)": None,
            "count(A)": 0,
            "complex_dict": {
                None: "none_key",
                "none_value": None,
                (None, "tuple"): "tuple_with_none",
            },
        }

        serialized = imputer.serialize()
        deserialized = SimpleImputer.deserialize(serialized)

        assert deserialized.stats_ == imputer.stats_
        assert deserialized.stats_["mean(A)"] is None
        assert None in deserialized.stats_["complex_dict"]

    def test_nested_complex_structures(self):
        """Test deeply nested complex data structures."""
        imputer = SimpleImputer(columns=["A"], strategy="mean")
        imputer._fitted = True

        # Create deeply nested structure with various key types
        imputer.stats_ = {
            "nested_structure": {
                ("level1", "tuple"): {
                    frozenset([1, 2]): "frozenset_key",
                    42: {"nested_dict": "value"},
                    None: [1, 2, 3],
                    True: {"another": {"level": "deep"}},
                }
            }
        }

        serialized = imputer.serialize()
        deserialized = SimpleImputer.deserialize(serialized)

        assert deserialized.stats_ == imputer.stats_

        # Verify specific nested access works
        nested = deserialized.stats_["nested_structure"]
        tuple_key = ("level1", "tuple")
        assert tuple_key in nested
        assert frozenset([1, 2]) in nested[tuple_key]

    def test_unknown_preprocessor_type(self):
        """Test error when trying to deserialize unknown preprocessor type."""
        import cloudpickle

        # Create fake serialized data with unknown type
        unknown_data = {
            "type": "NonExistentPreprocessor",
            "version": 1,
            "fields": {"columns": ["test"]},
            "stats": {},
            "stats_type": "cloudpickle",
        }

        fake_serialized = (
            SerializablePreprocessorBase.MAGIC_CLOUDPICKLE
            + cloudpickle.dumps(unknown_data)
        )

        with pytest.raises(UnknownPreprocessorError) as exc_info:
            SerializablePreprocessorBase.deserialize(fake_serialized)

        # Verify the exception contains the correct preprocessor type
        assert exc_info.value.preprocessor_type == "NonExistentPreprocessor"
        assert "Unknown preprocessor type: NonExistentPreprocessor" in str(
            exc_info.value
        )

    def test_file_system_integration(self):
        """Test integration with file system operations."""
        imputer = SimpleImputer(columns=["value"], strategy="mean")
        df = pd.DataFrame({"value": [1, 2, None, 4]})
        dataset = ray.data.from_pandas(df)
        fitted = imputer.fit(dataset)

        # Test with binary files (CloudPickle)
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".cloudpickle") as f:
            # Save as CloudPickle
            serialized = fitted.serialize()
            f.write(serialized)
            f.flush()

            # Load from file
            with open(f.name, "rb") as read_f:
                loaded_data = read_f.read()

            deserialized = SerializablePreprocessorBase.deserialize(loaded_data)
            assert isinstance(deserialized, SimpleImputer)
            assert abs(deserialized.stats_["mean(value)"] - 2.333333333333333) < 1e-10

    def test_special_numeric_values(self):
        """Test serialization with inf, -inf, and NaN values."""
        # Test with inf fill_value
        imputer1 = SimpleImputer(columns=["col"], strategy="mean")
        imputer1.stats_ = {"mean(col)": float("inf")}
        imputer1._fitted = True

        serialized = imputer1.serialize()
        deserialized = SerializablePreprocessorBase.deserialize(serialized)
        assert np.isinf(deserialized.stats_["mean(col)"])

        # Test with -inf fill_value
        imputer2 = SimpleImputer(columns=["col"], strategy="mean")
        imputer2.stats_ = {"mean(col)": float("-inf")}
        imputer2._fitted = True

        serialized = imputer2.serialize()
        deserialized = SerializablePreprocessorBase.deserialize(serialized)
        assert (
            np.isinf(deserialized.stats_["mean(col)"])
            and deserialized.stats_["mean(col)"] < 0
        )

        # Test with NaN fill_value
        imputer3 = SimpleImputer(columns=["col"], strategy="mean")
        imputer3.stats_ = {"mean(col)": float("nan")}
        imputer3._fitted = True

        serialized = imputer3.serialize()
        deserialized = SerializablePreprocessorBase.deserialize(serialized)
        assert np.isnan(deserialized.stats_["mean(col)"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
