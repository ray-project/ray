from typing import Any, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.exceptions import UserCodeException
from ray.data.preprocessor import (
    PreprocessorNotFittedException,
    SerializablePreprocessorBase as SerializablePreprocessor,
)
from ray.data.preprocessors import (
    Categorizer,
    LabelEncoder,
    MultiHotEncoder,
    OneHotEncoder,
    OrdinalEncoder,
)


# Helper functions for parameterized OrdinalEncoder tests
def _create_pandas_stats(unique_values: Dict[str, list]) -> Dict[str, Dict[Any, int]]:
    """Create stats in pandas dict format: {value: index}."""
    return {
        f"unique_values({col})": {v: i for i, v in enumerate(sorted(values))}
        for col, values in unique_values.items()
    }


def _create_arrow_stats(
    unique_values: Dict[str, list],
) -> Dict[str, tuple]:
    """Create stats in Arrow tuple format: (keys_array, values_array)."""
    result = {}
    for col, values in unique_values.items():
        sorted_values = sorted(values)
        keys_array = pa.array(sorted_values)
        values_array = pa.array(range(len(sorted_values)), type=pa.int64())
        result[f"unique_values({col})"] = (keys_array, values_array)
    return result


def _stats_to_dict(stats_value) -> Dict[Any, int]:
    """Convert stats to dict format regardless of whether it's Arrow or pandas format."""
    if isinstance(stats_value, dict):
        return stats_value
    elif isinstance(stats_value, tuple):
        # Arrow format: (keys_array, values_array)
        keys_array, values_array = stats_value
        return {k.as_py(): v.as_py() for k, v in zip(keys_array, values_array)}
    else:
        raise ValueError(f"Unknown stats format: {type(stats_value)}")


def _assert_stats_equal(actual_stats: Dict, expected_stats: Dict):
    """Assert that stats are equal, regardless of Arrow or pandas format."""
    for key, expected_value in expected_stats.items():
        assert key in actual_stats, f"Missing key: {key}"
        actual_value = _stats_to_dict(actual_stats[key])
        assert (
            actual_value == expected_value
        ), f"Stats mismatch for {key}: expected {expected_value}, got {actual_value}"


def test_ordinal_encoder_strings():
    """Test the OrdinalEncoder for strings."""

    input_dataframe = pd.DataFrame({"sex": ["male"] * 2000 + ["female"]})

    ds = ray.data.from_pandas(input_dataframe)
    encoder = OrdinalEncoder(columns=["sex"])
    encoded_ds = encoder.fit_transform(ds)
    encoded_ds_pd = encoded_ds.to_pandas()

    # Check if the "sex" column exists and is correctly encoded as integers
    assert (
        "sex" in encoded_ds_pd.columns
    ), "The 'sex' column is missing in the encoded DataFrame"
    assert (
        encoded_ds_pd["sex"].dtype == "int64"
    ), "The 'sex' column is not encoded as integers"

    # Verify that the encoding worked as expected.
    # We expect "male" to be encoded as 0 and "female" as 1
    unique_values = encoded_ds_pd["sex"].unique()
    assert set(unique_values) == {
        0,
        1,
    }, f"Unexpected unique values in 'sex' column: {unique_values}"
    expected_encoding = {"male": 1, "female": 0}
    for original, encoded in zip(input_dataframe["sex"], encoded_ds_pd["sex"]):
        assert (
            encoded == expected_encoding[original]
        ), f"Expected {original} to be encoded as {expected_encoding[original]}, but got {encoded}"  # noqa: E501


def test_ordinal_encoder_arrow_transform():
    """Test the OrdinalEncoder _transform_arrow method."""
    # Create test data
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})

    encoder = OrdinalEncoder(["B", "C"])

    # B: sorted unique = [cold, hot, warm] -> indices [0, 1, 2]
    # C: sorted unique = [1, 5, 10] -> indices [0, 1, 2]
    fit_df = pd.DataFrame({"B": ["cold", "hot", "warm"], "C": [1, 5, 10]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # Create Arrow table for transformation
    table = pa.Table.from_pandas(in_df)

    # Transform using Arrow
    result_table = encoder._transform_arrow(table)

    # Verify result is an Arrow table
    assert isinstance(result_table, pa.Table)

    # Convert to pandas for easier comparison
    result_df = result_table.to_pandas()

    # Expected encoding: sorted unique values get indices 0, 1, 2, ...
    # B: cold=0, hot=1, warm=2
    # C: 1=0, 5=1, 10=2
    expected_col_b = [2, 0, 1, 0]  # warm=2, cold=0, hot=1, cold=0
    expected_col_c = [0, 2, 1, 2]  # 1=0, 10=2, 5=1, 10=2

    assert result_df["A"].tolist() == col_a, "Column A should be unchanged"
    assert (
        result_df["B"].tolist() == expected_col_b
    ), f"Column B mismatch: {result_df['B'].tolist()}"
    assert (
        result_df["C"].tolist() == expected_col_c
    ), f"Column C mismatch: {result_df['C'].tolist()}"


def test_ordinal_encoder_arrow_transform_append_mode():
    """Test the OrdinalEncoder _transform_arrow method in append mode."""
    col_a = ["red", "green", "blue"]
    col_b = ["warm", "cold", "hot"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})

    encoder = OrdinalEncoder(["B"], output_columns=["B_encoded"])

    # B: sorted unique = [cold, hot, warm] -> indices [0, 1, 2]
    fit_df = pd.DataFrame({"B": ["cold", "hot", "warm"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    table = pa.Table.from_pandas(in_df)
    result_table = encoder._transform_arrow(table)
    result_df = result_table.to_pandas()

    # Original columns should be unchanged
    assert result_df["A"].tolist() == col_a
    assert result_df["B"].tolist() == col_b

    # New column should have encoded values
    # B: cold=0, hot=1, warm=2
    expected_b_encoded = [2, 0, 1]  # warm=2, cold=0, hot=1
    assert result_df["B_encoded"].tolist() == expected_b_encoded


def test_ordinal_encoder_arrow_transform_unknown_values():
    """Test the OrdinalEncoder _transform_arrow method with unknown values."""
    encoder = OrdinalEncoder(["B"])

    # Fit encoder with only "warm" and "cold" (not "unknown")
    # B: sorted unique = [cold, warm] -> indices [0, 1]
    fit_df = pd.DataFrame({"B": ["cold", "warm"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # Transform data with an unknown value
    test_df = pd.DataFrame({"B": ["warm", "cold", "unknown"]})
    table = pa.Table.from_pandas(test_df)
    result_table = encoder._transform_arrow(table)
    result_df = result_table.to_pandas()

    # warm=1, cold=0, unknown should be null
    # pc.index_in returns null for values not found
    assert result_df["B"].tolist()[0] == 1  # warm
    assert result_df["B"].tolist()[1] == 0  # cold
    assert pd.isna(result_df["B"].tolist()[2])  # unknown -> null


# =============================================================================
# Parameterized tests for OrdinalEncoder (testing both pandas and arrow paths)
# =============================================================================


@pytest.mark.parametrize("batch_format", ["pandas", "arrow"])
def test_ordinal_encoder_transform_scalars(batch_format):
    """Test OrdinalEncoder transformation for scalar values with both pandas and arrow."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})

    encoder = OrdinalEncoder(["B", "C"])

    # B: sorted unique = [cold, hot, warm] -> indices [0, 1, 2]
    # C: sorted unique = [1, 5, 10] -> indices [0, 1, 2]
    fit_df = pd.DataFrame({"B": ["cold", "hot", "warm"], "C": [1, 5, 10]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # For pandas batch_format test, convert Arrow stats to pandas format
    if batch_format == "pandas":
        unique_values = {"B": ["cold", "hot", "warm"], "C": [1, 5, 10]}
        encoder.stats_ = _create_pandas_stats(unique_values)

    # Transform using the appropriate method
    if batch_format == "pandas":
        result_df = encoder._transform_pandas(in_df.copy())
    else:
        table = pa.Table.from_pandas(in_df)
        result_table = encoder._transform_arrow(table)
        result_df = result_table.to_pandas()

    # Expected encoding: sorted unique values get indices 0, 1, 2, ...
    # B: cold=0, hot=1, warm=2
    # C: 1=0, 5=1, 10=2
    expected_col_b = [2, 0, 1, 0]  # warm=2, cold=0, hot=1, cold=0
    expected_col_c = [0, 2, 1, 2]  # 1=0, 10=2, 5=1, 10=2

    assert result_df["A"].tolist() == col_a, "Column A should be unchanged"
    assert (
        result_df["B"].tolist() == expected_col_b
    ), f"Column B mismatch: {result_df['B'].tolist()}"
    assert (
        result_df["C"].tolist() == expected_col_c
    ), f"Column C mismatch: {result_df['C'].tolist()}"


@pytest.mark.parametrize("batch_format", ["pandas", "arrow"])
def test_ordinal_encoder_transform_append_mode(batch_format):
    """Test OrdinalEncoder append mode with both pandas and arrow."""
    col_a = ["red", "green", "blue"]
    col_b = ["warm", "cold", "hot"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})

    encoder = OrdinalEncoder(["B"], output_columns=["B_encoded"])

    fit_df = pd.DataFrame({"B": ["cold", "hot", "warm"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # For pandas batch_format test, convert Arrow stats to pandas format
    if batch_format == "pandas":
        unique_values = {"B": ["cold", "hot", "warm"]}
        encoder.stats_ = _create_pandas_stats(unique_values)

    # Transform using the appropriate method
    if batch_format == "pandas":
        result_df = encoder._transform_pandas(in_df.copy())
    else:
        table = pa.Table.from_pandas(in_df)
        result_table = encoder._transform_arrow(table)
        result_df = result_table.to_pandas()

    # Original columns should be unchanged
    assert result_df["A"].tolist() == col_a
    assert result_df["B"].tolist() == col_b

    # New column should have encoded values
    # B: cold=0, hot=1, warm=2
    expected_b_encoded = [2, 0, 1]  # warm=2, cold=0, hot=1
    assert result_df["B_encoded"].tolist() == expected_b_encoded


@pytest.mark.parametrize("batch_format", ["pandas", "arrow"])
def test_ordinal_encoder_transform_unknown_values(batch_format):
    """Test OrdinalEncoder with unknown values using both pandas and arrow."""
    encoder = OrdinalEncoder(["B"])

    # Fit encoder with only "warm" and "cold" (not "unknown")
    fit_df = pd.DataFrame({"B": ["cold", "warm"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # For pandas batch_format test, convert Arrow stats to pandas format
    if batch_format == "pandas":
        unique_values = {"B": ["cold", "warm"]}
        encoder.stats_ = _create_pandas_stats(unique_values)

    # Transform data with an unknown value
    test_df = pd.DataFrame({"B": ["warm", "cold", "unknown"]})

    if batch_format == "pandas":
        result_df = encoder._transform_pandas(test_df.copy())
    else:
        table = pa.Table.from_pandas(test_df)
        result_table = encoder._transform_arrow(table)
        result_df = result_table.to_pandas()

    # warm=1, cold=0, unknown should be null/None
    assert result_df["B"].tolist()[0] == 1  # warm
    assert result_df["B"].tolist()[1] == 0  # cold
    assert pd.isna(result_df["B"].tolist()[2])  # unknown -> null


@pytest.mark.parametrize("batch_format", ["pandas", "arrow"])
def test_ordinal_encoder_transform_multiple_columns(batch_format):
    """Test OrdinalEncoder with multiple columns using both pandas and arrow."""
    in_df = pd.DataFrame(
        {
            "color": ["red", "blue", "green", "red"],
            "size": ["small", "large", "medium", "small"],
            "count": [1, 3, 2, 1],
        }
    )

    encoder = OrdinalEncoder(["color", "size", "count"])

    fit_df = pd.DataFrame(
        {
            "color": ["blue", "green", "red"],
            "size": ["large", "medium", "small"],
            "count": [1, 2, 3],
        }
    )
    encoder.fit(ray.data.from_pandas(fit_df))

    # For pandas batch_format test, convert Arrow stats to pandas format
    if batch_format == "pandas":
        unique_values = {
            "color": ["blue", "green", "red"],
            "size": ["large", "medium", "small"],
            "count": [1, 2, 3],
        }
        encoder.stats_ = _create_pandas_stats(unique_values)

    if batch_format == "pandas":
        result_df = encoder._transform_pandas(in_df.copy())
    else:
        table = pa.Table.from_pandas(in_df)
        result_table = encoder._transform_arrow(table)
        result_df = result_table.to_pandas()

    # Verify encodings
    # color: blue=0, green=1, red=2 -> [2, 0, 1, 2]
    # size: large=0, medium=1, small=2 -> [2, 0, 1, 2]
    # count: 1=0, 2=1, 3=2 -> [0, 2, 1, 0]
    assert result_df["color"].tolist() == [2, 0, 1, 2]
    assert result_df["size"].tolist() == [2, 0, 1, 2]
    assert result_df["count"].tolist() == [0, 2, 1, 0]


@pytest.mark.parametrize("batch_format", ["pandas", "arrow"])
def test_ordinal_encoder_transform_integers(batch_format):
    """Test OrdinalEncoder with integer columns using both pandas and arrow."""
    in_df = pd.DataFrame({"values": [100, 50, 200, 50, 100]})

    encoder = OrdinalEncoder(["values"])

    fit_df = pd.DataFrame({"values": [50, 100, 200]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # For pandas batch_format test, convert Arrow stats to pandas format
    if batch_format == "pandas":
        unique_values = {"values": [50, 100, 200]}
        encoder.stats_ = _create_pandas_stats(unique_values)

    if batch_format == "pandas":
        result_df = encoder._transform_pandas(in_df.copy())
    else:
        table = pa.Table.from_pandas(in_df)
        result_table = encoder._transform_arrow(table)
        result_df = result_table.to_pandas()

    # 50=0, 100=1, 200=2 -> [1, 0, 2, 0, 1]
    assert result_df["values"].tolist() == [1, 0, 2, 0, 1]


def test_ordinal_encoder_list_fallback_to_pandas():
    """Test that Arrow transform falls back to pandas for list columns."""
    # This test verifies the fallback behavior when Arrow encounters list columns
    col_d = [["warm", "cold"], ["hot"], ["warm", "hot", "cold"]]
    in_df = pd.DataFrame({"D": col_d})

    encoder = OrdinalEncoder(["D"], encode_lists=True)

    # Fit encoder on data with list values containing all unique elements
    fit_df = pd.DataFrame({"D": [["cold", "hot", "warm"]]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # For list columns with fallback, we need pandas-format stats
    # (Arrow transform will fall back to pandas for list columns)
    encoder.stats_ = {"unique_values(D)": {"cold": 0, "hot": 1, "warm": 2}}

    # Create Arrow table with list column
    table = pa.Table.from_pandas(in_df)

    # Verify column is detected as list type
    assert pa.types.is_list(table.schema.field("D").type)

    # Transform should fall back to pandas and work correctly
    result_table = encoder._transform_arrow(table)
    result_df = result_table.to_pandas()

    # Verify encoding: cold=0, hot=1, warm=2
    expected = [[2, 0], [1], [2, 1, 0]]
    result_lists = [list(arr) for arr in result_df["D"]]
    assert result_lists == expected


# =============================================================================
# Tests for vectorized Arrow encoding
# =============================================================================


def test_ordinal_encoder_encode_column_vectorized():
    """Test _encode_column_vectorized method directly."""
    encoder = OrdinalEncoder(["col"])

    fit_df = pd.DataFrame({"col": ["a", "b", "c"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # Create a chunked array to encode
    column = pa.chunked_array([["b", "a", "c", "a", "b"]])

    result = encoder._encode_column_vectorized(column, "col")

    # a=0, b=1, c=2
    assert result.to_pylist() == [1, 0, 2, 0, 1]


def test_ordinal_encoder_encode_column_with_unknown_values():
    """Test encoding handles unknown values correctly."""
    encoder = OrdinalEncoder(["col"])

    # Fit encoder with only "a" and "b" (not "c")
    fit_df = pd.DataFrame({"col": ["a", "b"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # Column with unknown value "c"
    column = pa.chunked_array([["a", "b", "c"]])

    result = encoder._encode_column_vectorized(column, "col")
    assert result.to_pylist()[0] == 0  # a
    assert result.to_pylist()[1] == 1  # b
    assert result.to_pylist()[2] is None  # c (unknown)


def test_ordinal_encoder_vectorized_multiple_columns():
    """Test vectorized encoding works correctly with multiple columns."""
    col_a = ["x", "y"] * 50
    col_b = [1, 2, 3] * 34
    col_b = col_b[:100]

    in_df = pd.DataFrame({"A": col_a, "B": col_b})

    encoder = OrdinalEncoder(["A", "B"])

    fit_df = pd.DataFrame({"A": ["x", "y", "x"], "B": [1, 2, 3]})
    encoder.fit(ray.data.from_pandas(fit_df))

    table = pa.Table.from_pandas(in_df)
    result_table = encoder._transform_arrow(table)
    result_df = result_table.to_pandas()

    # Verify both columns are encoded correctly
    expected_a = [{"x": 0, "y": 1}[v] for v in col_a]
    expected_b = [{1: 0, 2: 1, 3: 2}[v] for v in col_b]

    assert result_df["A"].tolist() == expected_a
    assert result_df["B"].tolist() == expected_b


def test_ordinal_encoder():
    """Tests basic OrdinalEncoder functionality."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    col_d = [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c, "D": col_d})
    ds = ray.data.from_pandas(in_df)

    encoder = OrdinalEncoder(["B", "C", "D"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)
    # Stats may be in Arrow tuple format or pandas dict format depending on
    # preferred_batch_format. Use helper to verify regardless of format.
    _assert_stats_equal(
        encoder.stats_,
        {
            "unique_values(B)": {"cold": 0, "hot": 1, "warm": 2},
            "unique_values(C)": {1: 0, 5: 1, 10: 2},
            "unique_values(D)": {"cold": 0, "hot": 1, "warm": 2},
        },
    )

    # Transform data.
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [2, 0, 1, 0]
    processed_col_c = [0, 2, 1, 2]
    processed_col_d = [[2], [], [1, 2, 0], [0, 0]]
    expected_df = pd.DataFrame.from_dict(
        {
            "A": processed_col_a,
            "B": processed_col_b,
            "C": processed_col_c,
            "D": processed_col_d,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df)

    # Transform batch.
    pred_col_a = ["blue", "yellow", None]
    pred_col_b = ["cold", "warm", "other"]
    pred_col_c = [10, 1, 20]
    pred_col_d = [["cold", "warm"], [], ["other", "cold"]]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c, "D": pred_col_d}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [0, 2, None]
    pred_processed_col_c = [2, 0, None]
    pred_processed_col_d = [[0, 2], [], [None, 0]]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
            "D": pred_processed_col_d,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df)

    # append mode
    with pytest.raises(ValueError):
        OrdinalEncoder(columns=["B", "C", "D"], output_columns=["B_encoded"])

    encoder = OrdinalEncoder(
        columns=["B", "C", "D"], output_columns=["B_encoded", "C_encoded", "D_encoded"]
    )
    encoder.fit(ds)

    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c, "D": pred_col_d}
    )
    pred_out_df = encoder.transform_batch(pred_in_df)
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_col_a,
            "B": pred_col_b,
            "C": pred_col_c,
            "D": pred_col_d,
            "B_encoded": pred_processed_col_b,
            "C_encoded": pred_processed_col_c,
            "D_encoded": pred_processed_col_d,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df, check_like=True)

    # Test null behavior.
    null_col = [1, None]
    nonnull_col = [1, 1]
    null_df = pd.DataFrame.from_dict({"A": null_col})
    null_ds = ray.data.from_pandas(null_df)
    nonnull_df = pd.DataFrame.from_dict({"A": nonnull_col})
    nonnull_ds = ray.data.from_pandas(nonnull_df)
    null_encoder = OrdinalEncoder(["A"])

    # Verify fit fails for null values.
    with pytest.raises(ValueError):
        null_encoder.fit(null_ds)
    null_encoder.fit(nonnull_ds)

    # Verify transform fails for null values.
    with pytest.raises((UserCodeException, ValueError)):
        null_encoder.transform(null_ds).materialize()
    null_encoder.transform(nonnull_ds)

    # Verify transform_batch fails for null values.
    with pytest.raises(ValueError):
        null_encoder.transform_batch(null_df)
    null_encoder.transform_batch(nonnull_df)


def test_ordinal_encoder_no_encode_list():
    """Tests OrdinalEncoder with encode_lists=False."""
    in_df = pd.DataFrame.from_dict(
        {
            "A": ["red", "green", "blue", "red"],
            "B": ["warm", "cold", "hot", "cold"],
            "C": [1, 10, 5, 10],
            "D": [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]],
        }
    )
    ds = ray.data.from_pandas(in_df)

    encoder = OrdinalEncoder(["B", "C", "D"], encode_lists=False)

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)
    # Stats may be in Arrow tuple format or pandas dict format
    assert _stats_to_dict(encoder.stats_["unique_values(B)"]) == {
        "cold": 0,
        "hot": 1,
        "warm": 2,
    }
    assert _stats_to_dict(encoder.stats_["unique_values(C)"]) == {1: 0, 5: 1, 10: 2}
    hash_dict = _stats_to_dict(encoder.stats_["unique_values(C)"])
    assert len(set(hash_dict.keys())) == len(set(hash_dict.values())) == len(hash_dict)
    assert max(hash_dict.values()) == len(hash_dict) - 1

    # Transform data.
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    assert out_df["A"].equals(pd.Series(in_df["A"]))
    assert out_df["B"].equals(pd.Series([2, 0, 1, 0]))
    assert out_df["C"].equals(pd.Series([0, 2, 1, 2]))
    assert set(out_df["D"].to_list()) == {3, 0, 2, 1}

    # Transform batch.
    pred_in_df = pd.DataFrame.from_dict(
        {
            "A": ["blue", "yellow", None],
            "B": ["cold", "warm", "other"],
            "C": [10, 1, 20],
            "D": [["cold", "cold"], [], ["other", "cold"]],
        }
    )

    pred_out_df: pd.DataFrame = encoder.transform_batch(pred_in_df)
    assert pred_out_df["A"].equals(pred_in_df["A"])
    assert pred_out_df["B"].equals(pd.Series([0, 2, None]))
    assert pred_out_df["C"].equals(pd.Series([2, 0, None]))
    assert pd.isnull(pred_out_df["D"].iloc[-1]), "Expected last value to be null"
    assert (
        len(pred_out_df["D"].iloc[:-1].dropna().drop_duplicates())
        == len(pred_out_df) - 1
    ), "All values excluding last one must be unique and non-null"


def _assert_one_hot_equal(actual_series, expected_values):
    """Assert one-hot encoded columns are equal, handling both list and numpy array types."""
    assert len(actual_series) == len(expected_values)
    for actual, expected in zip(actual_series, expected_values):
        assert list(actual) == list(expected)


def _assert_list_column_equal(actual_series, expected_series):
    """Assert list columns are equal, handling Arrow round-trip type changes."""
    assert len(actual_series) == len(expected_series)
    for actual, expected in zip(actual_series, expected_series):
        assert list(actual) == list(expected)


# =============================================================================
# Tests for OneHotEncoder Arrow transform
# =============================================================================


def test_one_hot_encoder_arrow_transform():
    """Test the OneHotEncoder _transform_arrow method."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})

    encoder = OneHotEncoder(["B", "C"])

    # B: cold=0, hot=1, warm=2
    # C: 1=0, 5=1, 10=2
    fit_df = pd.DataFrame({"B": ["cold", "hot", "warm"], "C": [1, 5, 10]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # Create Arrow table for transformation
    table = pa.Table.from_pandas(in_df)

    # Transform using Arrow
    result_table = encoder._transform_arrow(table)

    # Verify result is an Arrow table
    assert isinstance(result_table, pa.Table)

    # Convert to pandas for easier comparison
    result_df = result_table.to_pandas()

    # Expected one-hot encoding:
    # B: warm=[0,0,1], cold=[1,0,0], hot=[0,1,0]
    # C: 1=[1,0,0], 10=[0,0,1], 5=[0,1,0]
    expected_col_b = [[0, 0, 1], [1, 0, 0], [0, 1, 0], [1, 0, 0]]
    expected_col_c = [[1, 0, 0], [0, 0, 1], [0, 1, 0], [0, 0, 1]]

    assert result_df["A"].tolist() == col_a, "Column A should be unchanged"
    _assert_one_hot_equal(result_df["B"], expected_col_b)
    _assert_one_hot_equal(result_df["C"], expected_col_c)


def test_one_hot_encoder_arrow_transform_append_mode():
    """Test the OneHotEncoder _transform_arrow method in append mode."""
    col_a = ["red", "green", "blue"]
    col_b = ["warm", "cold", "hot"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})

    encoder = OneHotEncoder(["B"], output_columns=["B_encoded"])

    fit_df = pd.DataFrame({"B": ["cold", "hot", "warm"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    table = pa.Table.from_pandas(in_df)
    result_table = encoder._transform_arrow(table)
    result_df = result_table.to_pandas()

    # Original columns should be unchanged
    assert result_df["A"].tolist() == col_a
    assert result_df["B"].tolist() == col_b

    # New column should have one-hot encoded values
    expected_b_encoded = [[0, 0, 1], [1, 0, 0], [0, 1, 0]]
    _assert_one_hot_equal(result_df["B_encoded"], expected_b_encoded)


def test_one_hot_encoder_arrow_transform_unknown_values():
    """Test the OneHotEncoder _transform_arrow method with unknown values."""
    encoder = OneHotEncoder(["B"])

    # Fit encoder with only "warm" and "cold" (not "unknown")
    fit_df = pd.DataFrame({"B": ["cold", "warm"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # Transform data with an unknown value
    test_df = pd.DataFrame({"B": ["warm", "cold", "unknown"]})
    table = pa.Table.from_pandas(test_df)
    result_table = encoder._transform_arrow(table)
    result_df = result_table.to_pandas()

    # warm=[0,1], cold=[1,0], unknown=[0,0] (all zeros for unknown)
    _assert_one_hot_equal(result_df["B"], [[0, 1], [1, 0], [0, 0]])


def test_one_hot_encoder_list_fallback_to_pandas():
    """Test that Arrow transform falls back to pandas for list columns."""
    col_d = [["warm", "cold"], ["hot"], ["warm", "hot", "cold"]]
    in_df = pd.DataFrame({"D": col_d})

    encoder = OneHotEncoder(["D"])

    # Fit encoder on data with list values (lists are treated as categories)
    fit_df = pd.DataFrame({"D": [["cold"], ["hot"], ["warm"]]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # For list columns with fallback, we need pandas-format stats
    # (Arrow transform will fall back to pandas for list columns)
    encoder.stats_ = {"unique_values(D)": {("cold",): 0, ("hot",): 1, ("warm",): 2}}

    # Create Arrow table with list column
    table = pa.Table.from_pandas(in_df)

    # Verify column is detected as list type
    assert pa.types.is_list(table.schema.field("D").type)

    # Transform should fall back to pandas and work correctly
    result_table = encoder._transform_arrow(table)
    result_df = result_table.to_pandas()

    # Verify one-hot encoding for list columns (handled by pandas fallback)
    # Each list element maps to one-hot vector
    assert len(result_df["D"]) == 3


def test_one_hot_encoder_multi_chunk_column():
    """Test OneHotEncoder with multi-chunk ChunkedArray input.

    This test ensures that _encode_column_one_hot correctly handles ChunkedArrays
    with multiple chunks, which can occur with partitioned or concatenated data.
    The implementation uses zero_copy_only=False when calling to_numpy() for
    compatibility across PyArrow versions.
    """
    encoder = OneHotEncoder(["col"])

    fit_df = pd.DataFrame({"col": ["a", "b", "c"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # Create a table with a multi-chunk column (simulates partitioned/concatenated data)
    chunk1 = pa.array(["a", "b", "c"])
    chunk2 = pa.array(["b", "a", "c"])
    chunk3 = pa.array(["c", "c", "a"])
    multi_chunk_column = pa.chunked_array([chunk1, chunk2, chunk3])

    # Verify we have multiple chunks in the input
    assert multi_chunk_column.num_chunks == 3

    # Create table with the multi-chunk column
    table = pa.table({"col": multi_chunk_column})

    # Transform using Arrow path - this exercises _encode_column_one_hot
    result_table = encoder._transform_arrow(table)
    result_df = result_table.to_pandas()

    # Verify correct one-hot encoding
    # a=[1,0,0], b=[0,1,0], c=[0,0,1]
    expected = [
        [1, 0, 0],  # a
        [0, 1, 0],  # b
        [0, 0, 1],  # c
        [0, 1, 0],  # b
        [1, 0, 0],  # a
        [0, 0, 1],  # c
        [0, 0, 1],  # c
        [0, 0, 1],  # c
        [1, 0, 0],  # a
    ]
    _assert_one_hot_equal(result_df["col"], expected)


@pytest.mark.parametrize("batch_format", ["pandas", "arrow"])
def test_one_hot_encoder_transform_scalars(batch_format):
    """Test OneHotEncoder transformation for scalar values with both pandas and arrow."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})

    encoder = OneHotEncoder(["B"])

    fit_df = pd.DataFrame({"B": ["cold", "hot", "warm"]})
    encoder.fit(ray.data.from_pandas(fit_df))

    # Transform using the appropriate method
    if batch_format == "pandas":
        result_df = encoder._transform_pandas(in_df.copy())
    else:
        table = pa.Table.from_pandas(in_df)
        result_table = encoder._transform_arrow(table)
        result_df = result_table.to_pandas()

    # Expected one-hot encoding: cold=[1,0,0], hot=[0,1,0], warm=[0,0,1]
    expected_col_b = [[0, 0, 1], [1, 0, 0], [0, 1, 0], [1, 0, 0]]

    assert result_df["A"].tolist() == col_a, "Column A should be unchanged"
    _assert_one_hot_equal(result_df["B"], expected_col_b)


def test_one_hot_encoder():
    """Tests basic OneHotEncoder functionality."""
    in_df = pd.DataFrame.from_dict(
        {
            "A": ["red", "green", "blue", "red"],
            "B": ["warm", "cold", "hot", "cold"],
            "C": [1, 10, 5, 10],
            "D": [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]],
        }
    )
    ds = ray.data.from_pandas(in_df)

    encoder = OneHotEncoder(["B", "C", "D"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)

    assert encoder.stats_["unique_values(B)"] == {
        "cold": 0,
        "hot": 1,
        "warm": 2,
    }
    assert encoder.stats_["unique_values(C)"] == {1: 0, 5: 1, 10: 2}
    hash_dict = encoder.stats_["unique_values(D)"]
    assert len(set(hash_dict.keys())) == len(set(hash_dict.values())) == len(hash_dict)
    assert max(hash_dict.values()) == len(hash_dict) - 1

    # Transform data.
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    assert out_df["A"].equals(in_df["A"])
    _assert_one_hot_equal(out_df["B"], [[0, 0, 1], [1, 0, 0], [0, 1, 0], [1, 0, 0]])
    _assert_one_hot_equal(out_df["C"], [[1, 0, 0], [0, 0, 1], [0, 1, 0], [0, 0, 1]])
    assert {tuple(row) for row in out_df["D"]} == {
        tuple(row)
        for row in pd.Series([[0, 0, 0, 1], [1, 0, 0, 0], [0, 0, 1, 0], [0, 1, 0, 0]])
    }

    # Transform batch.
    pred_in_df = pd.DataFrame.from_dict(
        {
            "A": ["blue", "yellow", None],
            "B": ["cold", "warm", "other"],
            "C": [10, 1, 20],
            "D": [["cold", "cold"], [], ["other", "cold"]],
        }
    )

    pred_out_df: pd.DataFrame = encoder.transform_batch(pred_in_df.copy())

    assert pred_out_df["A"].equals(pred_in_df["A"])
    _assert_one_hot_equal(pred_out_df["B"], [[1, 0, 0], [0, 0, 1], [0, 0, 0]])
    _assert_one_hot_equal(pred_out_df["C"], [[0, 0, 1], [1, 0, 0], [0, 0, 0]])
    assert list(pred_out_df["D"].iloc[-1]) == [0, 0, 0, 0]
    assert (
        len(
            {
                i
                for row in pred_out_df["D"].iloc[:-1]
                for i, val in enumerate(row)
                if val == 1
            }
        )
        == 2
    )

    # append mode
    with pytest.raises(ValueError):
        OneHotEncoder(columns=["B", "C", "D"], output_columns=["B_encoded"])

    encoder = OneHotEncoder(
        columns=["B", "C", "D"],
        output_columns=["B_onehot_encoded", "C_onehot_encoded", "D_onehot_encoded"],
    )
    encoder.fit(ds)
    pred_out_append_df: pd.DataFrame = encoder.transform_batch(pred_in_df.copy())
    assert pred_out_append_df["A"].equals(pred_in_df["A"])
    assert pred_out_append_df["B"].equals(pred_in_df["B"])
    assert pred_out_append_df["C"].equals(pred_in_df["C"])
    # List column D may have type changes after Arrow round-trip
    _assert_list_column_equal(pred_out_append_df["D"], pred_in_df["D"])
    _assert_one_hot_equal(
        pred_out_append_df["B_onehot_encoded"], pred_out_df["B"].tolist()
    )
    _assert_one_hot_equal(
        pred_out_append_df["C_onehot_encoded"], pred_out_df["C"].tolist()
    )
    _assert_one_hot_equal(
        pred_out_append_df["D_onehot_encoded"], pred_out_df["D"].tolist()
    )

    # Test null behavior.
    null_col = [1, None]
    nonnull_col = [1, 1]
    null_df = pd.DataFrame.from_dict({"A": null_col})
    null_ds = ray.data.from_pandas(null_df)
    nonnull_df = pd.DataFrame.from_dict({"A": nonnull_col})
    nonnull_ds = ray.data.from_pandas(nonnull_df)
    null_encoder = OneHotEncoder(["A"])

    # Verify fit fails for null values.
    with pytest.raises(ValueError):
        null_encoder.fit(null_ds)
    null_encoder.fit(nonnull_ds)

    # Verify transform fails for null values.
    with pytest.raises((UserCodeException, ValueError)):
        null_encoder.transform(null_ds).materialize()
    null_encoder.transform(nonnull_ds)

    # Verify transform_batch fails for null values.
    with pytest.raises(ValueError):
        null_encoder.transform_batch(null_df)
    null_encoder.transform_batch(nonnull_df)


def test_one_hot_encoder_with_max_categories():
    """Tests basic OneHotEncoder functionality with limit."""
    col_a = ["red", "green", "blue", "red", "red"]
    col_b = ["warm", "cold", "hot", "cold", "hot"]
    col_c = [1, 10, 5, 10, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    encoder = OneHotEncoder(["B", "C"], max_categories={"B": 2})

    ds_out = encoder.fit_transform(ds)
    df_out = ds_out.to_pandas()
    assert len(ds_out.to_pandas().columns) == 3

    expected_df = pd.DataFrame(
        {
            "A": col_a,
            "B": [[0, 0], [1, 0], [0, 1], [1, 0], [0, 1]],
            "C": [[1, 0, 0], [0, 0, 1], [0, 1, 0], [0, 0, 1], [0, 0, 1]],
        }
    )
    pd.testing.assert_frame_equal(df_out, expected_df, check_like=True)


def test_multi_hot_encoder():
    """Tests basic MultiHotEncoder functionality."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    col_d = [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c, "D": col_d})
    ds = ray.data.from_pandas(in_df)

    encoder = MultiHotEncoder(["B", "C", "D"])

    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)

    assert encoder.stats_ == {
        "unique_values(B)": {"cold": 0, "hot": 1, "warm": 2},
        "unique_values(C)": {1: 0, 5: 1, 10: 2},
        "unique_values(D)": {"cold": 0, "hot": 1, "warm": 2},
    }

    # Transform data.
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [[0, 0, 1], [1, 0, 0], [0, 1, 0], [1, 0, 0]]
    processed_col_c = [[1, 0, 0], [0, 0, 1], [0, 1, 0], [0, 0, 1]]
    processed_col_d = [[0, 0, 1], [0, 0, 0], [1, 1, 1], [2, 0, 0]]
    expected_df = pd.DataFrame.from_dict(
        {
            "A": processed_col_a,
            "B": processed_col_b,
            "C": processed_col_c,
            "D": processed_col_d,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df)

    # Transform batch.
    pred_col_a = ["blue", "yellow", None]
    pred_col_b = ["cold", "warm", "other"]
    pred_col_c = [10, 1, 20]
    pred_col_d = [["cold", "warm"], [], ["other", "cold"]]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c, "D": pred_col_d}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)
    print(pred_out_df.to_string())

    pred_processed_col_a = ["blue", "yellow", None]
    pred_processed_col_b = [[1, 0, 0], [0, 0, 1], [0, 0, 0]]
    pred_processed_col_c = [[0, 0, 1], [1, 0, 0], [0, 0, 0]]
    pred_processed_col_d = [[1, 0, 1], [0, 0, 0], [1, 0, 0]]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
            "D": pred_processed_col_d,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df)

    # append mode
    with pytest.raises(ValueError):
        MultiHotEncoder(columns=["B", "C", "D"], output_columns=["B_encoded"])

    encoder = MultiHotEncoder(
        columns=["B", "C", "D"],
        output_columns=[
            "B_multihot_encoded",
            "C_multihot_encoded",
            "D_multihot_encoded",
        ],
    )
    encoder.fit(ds)

    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c, "D": pred_col_d}
    )
    pred_out_df = encoder.transform_batch(pred_in_df)
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_col_a,
            "B": pred_col_b,
            "C": pred_col_c,
            "D": pred_col_d,
            "B_multihot_encoded": pred_processed_col_b,
            "C_multihot_encoded": pred_processed_col_c,
            "D_multihot_encoded": pred_processed_col_d,
        }
    )

    pd.testing.assert_frame_equal(pred_out_df, pred_expected_df)

    # Test null behavior.
    null_col = [1, None]
    nonnull_col = [1, 1]
    null_df = pd.DataFrame.from_dict({"A": null_col})
    null_ds = ray.data.from_pandas(null_df)
    nonnull_df = pd.DataFrame.from_dict({"A": nonnull_col})
    nonnull_ds = ray.data.from_pandas(nonnull_df)
    null_encoder = MultiHotEncoder(["A"])

    # Verify fit fails for null values.
    with pytest.raises(ValueError):
        null_encoder.fit(null_ds)
    null_encoder.fit(nonnull_ds)

    # Verify transform fails for null values.
    with pytest.raises((UserCodeException, ValueError)):
        null_encoder.transform(null_ds).materialize()
    null_encoder.transform(nonnull_ds)

    # Verify transform_batch fails for null values.
    with pytest.raises(ValueError):
        null_encoder.transform_batch(null_df)
    null_encoder.transform_batch(nonnull_df)


def test_multi_hot_encoder_with_max_categories():
    """Tests basic MultiHotEncoder functionality with limit."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    col_d = [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c, "D": col_d})
    ds = ray.data.from_pandas(in_df)

    encoder = MultiHotEncoder(["B", "C", "D"], max_categories={"B": 2})

    ds_out = encoder.fit_transform(ds)
    assert len(ds_out.to_pandas()["B"].iloc[0]) == 2
    assert len(ds_out.to_pandas()["C"].iloc[0]) == 3
    assert len(ds_out.to_pandas()["D"].iloc[0]) == 3


def test_label_encoder():
    """Tests basic LabelEncoder functionality."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "cold", "hot"]
    col_c = [1, 2, 3, 4]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    encoder = LabelEncoder("A")

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)

    assert encoder.stats_ == {"unique_values(A)": {"blue": 0, "green": 1, "red": 2}}

    # Transform data.
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [2, 1, 0, 2]
    processed_col_b = col_b
    processed_col_c = col_c
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )
    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # append mode
    append_encoder = LabelEncoder("A", output_column="A_encoded")
    append_encoder.fit(ds)
    append_transformed = append_encoder.transform(ds)
    out_df = append_transformed.to_pandas()

    expected_df = pd.DataFrame.from_dict(
        {"A": col_a, "B": col_b, "C": col_c, "A_encoded": processed_col_a}
    )
    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Inverse transform data.
    inverse_transformed = encoder.inverse_transform(transformed)
    inverse_df = inverse_transformed.to_pandas()

    pd.testing.assert_frame_equal(inverse_df, in_df, check_like=True)

    inverse_append_transformed = append_encoder.inverse_transform(append_transformed)
    inverse_append_df = inverse_append_transformed.to_pandas()
    expected_df = pd.DataFrame.from_dict(
        {"A": col_a, "B": col_b, "C": col_c, "A_encoded": processed_col_a}
    )
    pd.testing.assert_frame_equal(inverse_append_df, expected_df, check_like=True)

    # Inverse transform without fitting.
    new_encoder = LabelEncoder("A")

    with pytest.raises(RuntimeError):
        new_encoder.inverse_transform(ds)

    # Inverse transform on fitted preprocessor that hasn't transformed anything.
    new_encoder.fit(ds)
    inv_non_fitted = new_encoder.inverse_transform(transformed)
    inv_non_fitted_df = inv_non_fitted.to_pandas()

    assert inv_non_fitted_df.equals(in_df)

    # Transform batch.
    pred_col_a = ["blue", "red", "yellow"]
    pred_col_b = ["cold", "unknown", None]
    pred_col_c = [10, 20, None]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = [0, 2, None]
    pred_processed_col_b = pred_col_b
    pred_processed_col_c = pred_col_c
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    )
    assert pred_out_df.equals(pred_expected_df)

    # Test null behavior.
    null_col = [1, None]
    nonnull_col = [1, 1]
    null_df = pd.DataFrame.from_dict({"A": null_col})
    null_ds = ray.data.from_pandas(null_df)
    nonnull_df = pd.DataFrame.from_dict({"A": nonnull_col})
    nonnull_ds = ray.data.from_pandas(nonnull_df)
    null_encoder = LabelEncoder("A")

    # Verify fit fails for null values.
    with pytest.raises(ValueError):
        null_encoder.fit(null_ds)
    null_encoder.fit(nonnull_ds)

    # Verify transform fails for null values.
    with pytest.raises((UserCodeException, ValueError)):
        null_encoder.transform(null_ds).materialize()
    null_encoder.transform(nonnull_ds)

    # Verify transform_batch fails for null values.
    with pytest.raises(ValueError):
        null_encoder.transform_batch(null_df)
    null_encoder.transform_batch(nonnull_df)


@pytest.mark.parametrize("predefined_dtypes", [True, False])
def test_categorizer(predefined_dtypes):
    """Tests basic Categorizer functionality."""
    col_a = ["red", "green", "blue", "red", "red"]
    col_b = ["warm", "cold", "hot", "cold", None]
    col_c = [1, 10, 5, 10, 1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    columns = ["B", "C"]
    if predefined_dtypes:
        expected_dtypes = {
            "B": pd.CategoricalDtype(["cold", "hot", "warm"], ordered=True),
            "C": pd.CategoricalDtype([1, 5, 10]),
        }
        dtypes = {"B": pd.CategoricalDtype(["cold", "hot", "warm"], ordered=True)}
    else:
        expected_dtypes = {
            "B": pd.CategoricalDtype(["cold", "hot", "warm"]),
            "C": pd.CategoricalDtype([1, 5, 10]),
        }
        columns = ["B", "C"]
        dtypes = None

    encoder = Categorizer(columns, dtypes)

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)
    assert encoder.stats_ == expected_dtypes

    # Transform data.
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    assert out_df.dtypes["A"] == np.object_
    assert out_df.dtypes["B"] == expected_dtypes["B"]
    assert out_df.dtypes["C"] == expected_dtypes["C"]

    # Transform batch.
    pred_col_a = ["blue", "yellow", None]
    pred_col_b = ["cold", "warm", "other"]
    pred_col_c = [10, 1, 20]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    assert pred_out_df.dtypes["A"] == np.object_
    assert pred_out_df.dtypes["B"] == expected_dtypes["B"]
    assert pred_out_df.dtypes["C"] == expected_dtypes["C"]

    # append mode
    with pytest.raises(ValueError):
        Categorizer(columns=["B", "C"], output_columns=["B_categorized"])

    encoder = Categorizer(
        columns=["B", "C"],
        output_columns=["B_categorized", "C_categorized"],
        dtypes=dtypes,
    )
    encoder.fit(ds)

    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )
    pred_out_df = encoder.transform_batch(pred_in_df)

    assert pred_out_df.dtypes["A"] == np.object_
    assert pred_out_df.dtypes["B"] == np.object_
    assert pred_out_df.dtypes["C"] == np.int64
    assert pred_out_df.dtypes["B_categorized"] == expected_dtypes["B"]
    assert pred_out_df.dtypes["C_categorized"] == expected_dtypes["C"]


class TestEncoderSerialization:
    """Test basic serialization/deserialization functionality for all encoder preprocessors."""

    def setup_method(self):
        """Set up test data for encoders."""
        # Data for categorical encoders
        self.categorical_df = pd.DataFrame(
            {
                "category": ["A", "B", "C", "A", "B", "C", "A"],
                "grade": ["high", "medium", "low", "high", "medium", "low", "high"],
                "region": ["north", "south", "east", "west", "north", "south", "east"],
            }
        )

        # Data for multi-hot encoder (with lists)
        self.multihot_df = pd.DataFrame(
            {
                "tags": [
                    ["red", "car"],
                    ["blue", "bike"],
                    ["red", "truck"],
                    ["green", "car"],
                ],
                "features": [
                    ["fast", "loud"],
                    ["quiet"],
                    ["fast", "heavy"],
                    ["quiet", "light"],
                ],
            }
        )

        # Data for label encoder
        self.label_df = pd.DataFrame(
            {
                "target": ["cat", "dog", "bird", "cat", "dog", "bird"],
                "other": [1, 2, 3, 4, 5, 6],
            }
        )

    def test_ordinal_encoder_serialization(self):
        """Test OrdinalEncoder save/load functionality."""
        # Create and fit encoder
        encoder = OrdinalEncoder(columns=["category", "grade"])
        dataset = ray.data.from_pandas(self.categorical_df)
        fitted_encoder = encoder.fit(dataset)

        # Test CloudPickle serialization (primary format)
        serialized = fitted_encoder.serialize()
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessor.MAGIC_CLOUDPICKLE)

        # Test deserialization
        deserialized = SerializablePreprocessor.deserialize(serialized)
        assert isinstance(deserialized, OrdinalEncoder)
        assert deserialized._fitted
        assert deserialized.columns == ["category", "grade"]
        assert deserialized.encode_lists is True  # default value

        # Test functional equivalence
        test_df = pd.DataFrame({"category": ["A", "B"], "grade": ["high", "low"]})

        original_result = fitted_encoder.transform_batch(test_df.copy())
        deserialized_result = deserialized.transform_batch(test_df.copy())

        pd.testing.assert_frame_equal(original_result, deserialized_result)

    def test_onehot_encoder_serialization(self):
        """Test OneHotEncoder save/load functionality."""
        # Create and fit encoder
        encoder = OneHotEncoder(columns=["category"], max_categories={"category": 3})
        dataset = ray.data.from_pandas(self.categorical_df)
        fitted_encoder = encoder.fit(dataset)

        # Test CloudPickle serialization (primary format)
        serialized = fitted_encoder.serialize()
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessor.MAGIC_CLOUDPICKLE)

        # Test deserialization
        deserialized = SerializablePreprocessor.deserialize(serialized)
        assert isinstance(deserialized, OneHotEncoder)
        assert deserialized._fitted
        assert deserialized.columns == ["category"]
        assert deserialized.max_categories == {"category": 3}

        # Test functional equivalence
        test_df = pd.DataFrame({"category": ["A", "B", "C"]})

        original_result = fitted_encoder.transform_batch(test_df.copy())
        deserialized_result = deserialized.transform_batch(test_df.copy())

        pd.testing.assert_frame_equal(original_result, deserialized_result)

    def test_multihot_encoder_serialization(self):
        """Test MultiHotEncoder save/load functionality."""
        # Create and fit encoder
        encoder = MultiHotEncoder(columns=["tags"], max_categories={"tags": 5})
        dataset = ray.data.from_pandas(self.multihot_df)
        fitted_encoder = encoder.fit(dataset)

        # Test CloudPickle serialization (primary format)
        serialized = fitted_encoder.serialize()
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessor.MAGIC_CLOUDPICKLE)

        # Test deserialization
        deserialized = SerializablePreprocessor.deserialize(serialized)
        assert isinstance(deserialized, MultiHotEncoder)
        assert deserialized._fitted
        assert deserialized.columns == ["tags"]
        assert deserialized.max_categories == {"tags": 5}

        # Test functional equivalence
        test_df = pd.DataFrame({"tags": [["red", "car"], ["blue", "bike"]]})

        original_result = fitted_encoder.transform_batch(test_df.copy())
        deserialized_result = deserialized.transform_batch(test_df.copy())

        pd.testing.assert_frame_equal(original_result, deserialized_result)

    def test_label_encoder_serialization(self):
        """Test LabelEncoder save/load functionality."""
        # Create and fit encoder
        encoder = LabelEncoder(label_column="target")
        dataset = ray.data.from_pandas(self.label_df)
        fitted_encoder = encoder.fit(dataset)

        # Test CloudPickle serialization (primary format)
        serialized = fitted_encoder.serialize()
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessor.MAGIC_CLOUDPICKLE)

        # Test deserialization
        deserialized = SerializablePreprocessor.deserialize(serialized)
        assert isinstance(deserialized, LabelEncoder)
        assert deserialized._fitted
        assert deserialized.label_column == "target"
        assert deserialized.output_column == "target"  # default

        # Test functional equivalence
        test_df = pd.DataFrame({"target": ["cat", "dog", "bird"]})

        original_result = fitted_encoder.transform_batch(test_df.copy())
        deserialized_result = deserialized.transform_batch(test_df.copy())

        pd.testing.assert_frame_equal(original_result, deserialized_result)

    def test_categorizer_serialization(self):
        """Test Categorizer save/load functionality."""
        # Create categorizer with predefined dtypes
        sex_dtype = pd.CategoricalDtype(categories=["male", "female"], ordered=False)
        grade_dtype = pd.CategoricalDtype(
            categories=["high", "medium", "low"], ordered=True
        )

        categorizer = Categorizer(
            columns=["category", "grade"],
            dtypes={"category": sex_dtype, "grade": grade_dtype},
        )

        # Test CloudPickle serialization (primary format, even without fitting)
        serialized = categorizer.serialize()
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessor.MAGIC_CLOUDPICKLE)

        # Test deserialization
        deserialized = SerializablePreprocessor.deserialize(serialized)
        assert isinstance(deserialized, Categorizer)
        assert deserialized.columns == ["category", "grade"]

        # Test dtypes preservation
        assert len(deserialized.dtypes) == 2
        assert isinstance(deserialized.dtypes["category"], pd.CategoricalDtype)
        assert isinstance(deserialized.dtypes["grade"], pd.CategoricalDtype)

        # Check category preservation
        assert list(deserialized.dtypes["category"].categories) == ["male", "female"]
        assert deserialized.dtypes["category"].ordered is False

        assert list(deserialized.dtypes["grade"].categories) == [
            "high",
            "medium",
            "low",
        ]
        assert deserialized.dtypes["grade"].ordered is True

    def test_categorizer_fitted_serialization(self):
        """Test Categorizer save/load functionality after fitting."""
        # Create and fit categorizer (without predefined dtypes)
        categorizer = Categorizer(columns=["category", "grade"])
        dataset = ray.data.from_pandas(self.categorical_df)
        fitted_categorizer = categorizer.fit(dataset)

        # Test CloudPickle serialization (primary format)
        serialized = fitted_categorizer.serialize()
        assert isinstance(serialized, bytes)
        assert serialized.startswith(SerializablePreprocessor.MAGIC_CLOUDPICKLE)

        # Test deserialization
        deserialized = SerializablePreprocessor.deserialize(serialized)
        assert isinstance(deserialized, Categorizer)
        assert deserialized._fitted
        assert deserialized.columns == ["category", "grade"]

        # Test functional equivalence
        test_df = pd.DataFrame({"category": ["A", "B"], "grade": ["high", "low"]})

        original_result = fitted_categorizer.transform_batch(test_df.copy())
        deserialized_result = deserialized.transform_batch(test_df.copy())

        pd.testing.assert_frame_equal(original_result, deserialized_result)

    def test_encoder_serialization_formats(self):
        """Test that encoders work with different serialization formats."""
        encoder = OrdinalEncoder(columns=["category"])
        dataset = ray.data.from_pandas(self.categorical_df)
        fitted_encoder = encoder.fit(dataset)

        # Test CloudPickle format (default)
        cloudpickle_serialized = fitted_encoder.serialize()
        assert isinstance(cloudpickle_serialized, bytes)

        # Test Pickle format (legacy)
        pickle_serialized = fitted_encoder.serialize()
        assert isinstance(pickle_serialized, bytes)

        # Both should deserialize to equivalent objects
        cloudpickle_deserialized = SerializablePreprocessor.deserialize(
            cloudpickle_serialized
        )
        pickle_deserialized = SerializablePreprocessor.deserialize(pickle_serialized)

        # Test functional equivalence
        test_df = pd.DataFrame({"category": ["A", "B"]})

        cloudpickle_result = cloudpickle_deserialized.transform_batch(test_df.copy())
        pickle_result = pickle_deserialized.transform_batch(test_df.copy())

        pd.testing.assert_frame_equal(cloudpickle_result, pickle_result)

    def test_encoder_error_handling(self):
        """Test error handling for encoder serialization."""
        # Test unknown preprocessor type
        import cloudpickle

        unknown_data = {
            "type": "NonExistentEncoder",
            "version": 1,
            "fields": {"columns": ["test"]},
            "stats": {},
            "stats_type": "default",
        }

        fake_serialized = (
            SerializablePreprocessor.MAGIC_CLOUDPICKLE + cloudpickle.dumps(unknown_data)
        )

        from ray.data.preprocessors.version_support import UnknownPreprocessorError

        with pytest.raises(UnknownPreprocessorError) as exc_info:
            SerializablePreprocessor.deserialize(fake_serialized)

        assert exc_info.value.preprocessor_type == "NonExistentEncoder"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
