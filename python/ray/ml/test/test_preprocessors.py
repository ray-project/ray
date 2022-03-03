import numpy as np
import pandas as pd
import pytest

import ray
from ray.ml.preprocessor import PreprocessorNotFittedException
from ray.ml.preprocessors import (
    StandardScaler,
    MinMaxScaler,
    OrdinalEncoder,
    OneHotEncoder,
    LabelEncoder,
    SimpleImputer,
    Chain,
)


def test_standard_scaler():
    """Tests basic StandardScaler functionality."""
    col_a = [-1, 0, 1, 2]
    col_b = [1, 1, 5, 5]
    col_c = [1, 1, 1, None]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    scaler = StandardScaler(["B", "C"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        scaler.transform(ds)

    # Fit data.
    scaler.fit(ds)
    assert scaler.stats_ == {
        "mean(B)": 3.0,
        "mean(C)": 1.0,
        "std(B)": 2.0,
        "std(C)": 0.0,
    }

    # Transform data.
    transformed = scaler.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [-1.0, -1.0, 1.0, 1.0]
    processed_col_c = [0.0, 0.0, 0.0, None]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    assert out_df.equals(expected_df)

    # Transform batch.
    pred_col_a = [1, 2, 3]
    pred_col_b = [3, 5, 7]
    pred_col_c = [0, 1, 2]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = scaler.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [0.0, 1.0, 2.0]
    pred_processed_col_c = [-1.0, 0.0, 1.0]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    )

    assert pred_out_df.equals(pred_expected_df)


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

    assert out_df.equals(expected_df)

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

    assert pred_out_df.equals(pred_expected_df)


def test_ordinal_encoder():
    """Tests basic OrdinalEncoder functionality."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    encoder = OrdinalEncoder(["B", "C"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)
    assert encoder.stats_ == {
        "unique_values(B)": {"cold": 0, "hot": 1, "warm": 2},
        "unique_values(C)": {1: 0, 5: 1, 10: 2},
    }

    # Transform data.
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [2, 0, 1, 0]
    processed_col_c = [0, 2, 1, 2]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    assert out_df.equals(expected_df)

    # Transform batch.
    pred_col_a = ["blue", "yellow", None]
    pred_col_b = ["cold", "unknown", None]
    pred_col_c = [10, 20, None]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [0, None, None]
    pred_processed_col_c = [2, None, None]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    )

    assert pred_out_df.equals(pred_expected_df)


def test_one_hot_encoder():
    """Tests basic OneHotEncoder functionality."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    encoder = OneHotEncoder(["B", "C"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)

    assert encoder.stats_ == {
        "unique_values(B)": {"cold": 0, "hot": 1, "warm": 2},
        "unique_values(C)": {1: 0, 5: 1, 10: 2},
    }

    # Transform data.
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b_cold = [0, 1, 0, 1]
    processed_col_b_hot = [0, 0, 1, 0]
    processed_col_b_warm = [1, 0, 0, 0]
    processed_col_c_1 = [1, 0, 0, 0]
    processed_col_c_5 = [0, 0, 1, 0]
    processed_col_c_10 = [0, 1, 0, 1]
    expected_df = pd.DataFrame.from_dict(
        {
            "A": processed_col_a,
            "B_cold": processed_col_b_cold,
            "B_hot": processed_col_b_hot,
            "B_warm": processed_col_b_warm,
            "C_1": processed_col_c_1,
            "C_5": processed_col_c_5,
            "C_10": processed_col_c_10,
        }
    )

    assert out_df.equals(expected_df)

    # Transform batch.
    pred_col_a = ["blue", "yellow", None]
    pred_col_b = ["cold", "unknown", None]
    pred_col_c = [10, 20, None]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = ["blue", "yellow", None]
    pred_processed_col_b_cold = [1, 0, 0]
    pred_processed_col_b_hot = [0, 0, 0]
    pred_processed_col_b_warm = [0, 0, 0]
    pred_processed_col_c_1 = [0, 0, 0]
    pred_processed_col_c_5 = [0, 0, 0]
    pred_processed_col_c_10 = [1, 0, 0]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B_cold": pred_processed_col_b_cold,
            "B_hot": pred_processed_col_b_hot,
            "B_warm": pred_processed_col_b_warm,
            "C_1": pred_processed_col_c_1,
            "C_5": pred_processed_col_c_5,
            "C_10": pred_processed_col_c_10,
        }
    )

    assert pred_out_df.equals(pred_expected_df)


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
    assert out_df.equals(expected_df)

    # Transform batch.
    pred_col_a = ["blue", "yellow", None]
    pred_col_b = ["cold", "unknown", None]
    pred_col_c = [10, 20, None]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = [0, None, None]
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


def test_chain():
    """Tests basic Chain functionality."""
    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    scaler = StandardScaler(["A", "B"])
    imputer = SimpleImputer(["B"])
    encoder = LabelEncoder("C")
    chain = Chain(scaler, imputer, encoder)

    # Fit data.
    chain.fit(ds)
    assert scaler.stats_ == {
        "mean(A)": 0.0,
        "mean(B)": 1.0,
        "std(A)": 1.0,
        "std(B)": 0.0,
    }
    assert imputer.stats_ == {
        "mean(B)": 0.0,
    }
    assert encoder.stats_ == {
        "unique_values(C)": {"monday": 0, "sunday": 1, "tuesday": 2}
    }

    # Transform data.
    transformed = chain.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [-1.0, -1.0, 1.0, 1.0]
    processed_col_b = [0.0, 0.0, 0.0, 0.0]
    processed_col_c = [1, 0, 2, 2]
    expected_df = pd.DataFrame.from_dict(
        {"A": processed_col_a, "B": processed_col_b, "C": processed_col_c}
    )

    assert out_df.equals(expected_df)

    # Transform batch.
    pred_col_a = [1, 2, None]
    pred_col_b = [0, None, 2]
    pred_col_c = ["monday", "tuesday", "wednesday"]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = chain.transform_batch(pred_in_df)

    pred_processed_col_a = [1, 2, None]
    pred_processed_col_b = [-1.0, 0.0, 1.0]
    pred_processed_col_c = [0, 2, None]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
        }
    )

    assert pred_out_df.equals(pred_expected_df)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
