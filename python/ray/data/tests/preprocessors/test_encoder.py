import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.exceptions import UserCodeException
from ray.data.preprocessor import PreprocessorNotFittedException
from ray.data.preprocessors import (
    Categorizer,
    LabelEncoder,
    MultiHotEncoder,
    OneHotEncoder,
    OrdinalEncoder,
)


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
    assert encoder.stats_ == {
        "unique_values(B)": {"cold": 0, "hot": 1, "warm": 2},
        "unique_values(C)": {1: 0, 5: 1, 10: 2},
        "unique_values(D)": {"cold": 0, "hot": 1, "warm": 2},
    }

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

    assert out_df.equals(expected_df)

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

    assert pred_out_df.equals(pred_expected_df)

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
    assert encoder.stats_["unique_values(B)"] == {"cold": 0, "hot": 1, "warm": 2}
    assert encoder.stats_["unique_values(C)"] == {1: 0, 5: 1, 10: 2}
    hash_dict = encoder.stats_["unique_values(C)"]
    assert len(set(hash_dict.keys())) == len(set(hash_dict.values())) == len(hash_dict)
    assert max(hash_dict.values()) == len(hash_dict) - 1

    # Transform data.
    print("transform")
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

    expected_df = pd.DataFrame.from_dict(
        {
            "A": in_df["A"],
            "B": [[0, 0, 1], [1, 0, 0], [0, 1, 0], [1, 0, 0]],
            "C": [[1, 0, 0], [0, 0, 1], [0, 1, 0], [0, 0, 1]],
        }
    )

    assert out_df["A"].equals(expected_df["A"])
    assert out_df["B"].equals(expected_df["B"])
    assert out_df["C"].equals(expected_df["C"])
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

    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_in_df["A"],
            "B": [[1.0, 0.0, 0.0], [0.0, 0.0, 1.0], [0, 0, 0]],
            "C": [[0, 0, 1], [1, 0, 0], [0, 0, 0]],
        }
    )

    assert pred_out_df["A"].equals(pred_expected_df["A"])
    assert pred_out_df["B"].equals(pred_expected_df["B"])
    assert pred_out_df["C"].equals(pred_expected_df["C"])
    assert pred_out_df["D"].iloc[-1] == [0, 0, 0, 0]
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
    assert pred_out_append_df["D"].equals(pred_in_df["D"])
    assert pred_out_append_df["B_onehot_encoded"].equals(pred_out_df["B"])
    assert pred_out_append_df["C_onehot_encoded"].equals(pred_out_df["C"])
    assert pred_out_append_df["D_onehot_encoded"].equals(pred_out_df["D"])

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


def test_one_hot_encoder_mixed_data_types():
    """Tests OneHotEncoder functionality with mixed data types (strings and lists)."""

    test_inputs = {"category": ["1", [1]]}
    test_pd_df = pd.DataFrame(test_inputs)
    test_data_for_fitting = {"category": ["1", "[1]", "a", "[]", "True"]}
    test_ray_dataset_for_fitting = ray.data.from_pandas(
        pd.DataFrame(test_data_for_fitting)
    )

    encoder = OneHotEncoder(columns=["category"])
    encoder.fit(test_ray_dataset_for_fitting)

    pandas_output = encoder.transform_batch(test_pd_df)
    expected_output = pd.DataFrame({"category": [[1, 0, 0, 0, 0], [0, 0, 0, 0, 0]]})

    pd.testing.assert_frame_equal(pandas_output, expected_output)


def test_multi_hot_encoder():
    """Tests basic MultiHotEncoder functionality."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    col_d = [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c, "D": col_d})
    ds = ray.data.from_pandas(in_df)

    encoder = MultiHotEncoder(["B", "C", "D"])

    # Transform with unfitted preprocessor.
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

    assert out_df.equals(expected_df)

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

    assert pred_out_df.equals(pred_expected_df)

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
    assert pred_out_df.equals(pred_expected_df)

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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
