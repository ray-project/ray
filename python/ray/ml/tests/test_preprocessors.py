import warnings
from collections import Counter
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import ray
from ray.ml.preprocessor import PreprocessorNotFittedException
from ray.ml.preprocessors import (
    BatchMapper,
    StandardScaler,
    MinMaxScaler,
    OrdinalEncoder,
    OneHotEncoder,
    LabelEncoder,
    SimpleImputer,
    Chain,
)
from ray.ml.preprocessors.encoder import Categorizer
from ray.ml.preprocessors.hasher import FeatureHasher
from ray.ml.preprocessors.normalizer import Normalizer
from ray.ml.preprocessors.scaler import MaxAbsScaler, RobustScaler
from ray.ml.preprocessors.tokenizer import Tokenizer
from ray.ml.preprocessors.transformer import PowerTransformer
from ray.ml.preprocessors.utils import simple_split_tokenizer, simple_hash
from ray.ml.preprocessors.vectorizer import CountVectorizer, HashingVectorizer


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


@patch.object(warnings, "warn")
def test_fit_twice(mocked_warn):
    """Tests that a warning msg should be printed."""
    col_a = [-1, 0, 1]
    col_b = [1, 3, 5]
    col_c = [1, 1, None]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    scaler = MinMaxScaler(["B", "C"])

    # Fit data.
    scaler.fit(ds)
    assert scaler.stats_ == {"min(B)": 1, "max(B)": 5, "min(C)": 1, "max(C)": 1}

    ds = ds.map_batches(lambda x: x * 2)
    # Fit again
    scaler.fit(ds)
    # Assert that the fitted state is corresponding to the second ds.
    assert scaler.stats_ == {"min(B)": 2, "max(B)": 10, "min(C)": 2, "max(C)": 2}
    msg = (
        "`fit` has already been called on the preprocessor (or at least one "
        "contained preprocessors if this is a chain). "
        "All previously fitted state will be overwritten!"
    )
    mocked_warn.assert_called_once_with(msg)


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

    assert out_df.equals(expected_df)

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

    assert pred_out_df.equals(pred_expected_df)


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
    pred_processed_col_b = [1.5, 2.5, 3.5]
    pred_processed_col_c = [-1.0, -0.5, 0.0]
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
    pred_col_b = ["cold", "warm", "other"]
    pred_col_c = [10, 1, 20]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [0, 2, None]
    pred_processed_col_c = [2, 0, None]
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
    null_encoder = OrdinalEncoder(["A"])

    # Verify fit fails for null values.
    with pytest.raises(ValueError):
        null_encoder.fit(null_ds)
    null_encoder.fit(nonnull_ds)

    # Verify transform fails for null values.
    with pytest.raises(ValueError):
        null_encoder.transform(null_ds)
    null_encoder.transform(nonnull_ds)

    # Verify transform_batch fails for null values.
    with pytest.raises(ValueError):
        null_encoder.transform_batch(null_df)
    null_encoder.transform_batch(nonnull_df)


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
    pred_col_b = ["cold", "warm", "other"]
    pred_col_c = [10, 1, 20]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = ["blue", "yellow", None]
    pred_processed_col_b_cold = [1, 0, 0]
    pred_processed_col_b_hot = [0, 0, 0]
    pred_processed_col_b_warm = [0, 1, 0]
    pred_processed_col_c_1 = [0, 1, 0]
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
    with pytest.raises(ValueError):
        null_encoder.transform(null_ds)
    null_encoder.transform(nonnull_ds)

    # Verify transform_batch fails for null values.
    with pytest.raises(ValueError):
        null_encoder.transform_batch(null_df)
    null_encoder.transform_batch(nonnull_df)


def test_one_hot_encoder_with_limit():
    """Tests basic OneHotEncoder functionality with limit."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    encoder = OneHotEncoder(["B", "C"], limit={"B": 2})

    ds_out = encoder.fit_transform(ds)
    assert len(ds_out.to_pandas().columns) == 1 + 2 + 3


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
    with pytest.raises(ValueError):
        null_encoder.transform(null_ds)
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

    if predefined_dtypes:
        expected_dtypes = {
            "B": pd.CategoricalDtype(["cold", "hot", "warm"], ordered=True),
            "C": pd.CategoricalDtype([1, 5, 10]),
        }
        columns = {
            "B": pd.CategoricalDtype(["cold", "hot", "warm"], ordered=True),
            "C": None,
        }
    else:
        expected_dtypes = {
            "B": pd.CategoricalDtype(["cold", "hot", "warm"]),
            "C": pd.CategoricalDtype([1, 5, 10]),
        }
        columns = ["B", "C"]

    encoder = Categorizer(columns)

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
    constant_df = pd.DataFrame.from_dict({"A": constant_col_a})
    constant_ds = ray.data.from_pandas(constant_df)

    with pytest.raises(ValueError):
        SimpleImputer(["A"], strategy="constant")

    constant_imputer = SimpleImputer(
        ["A", "B"], strategy="constant", fill_value="missing"
    )
    constant_transformed = constant_imputer.transform(constant_ds)
    constant_out_df = constant_transformed.to_pandas()

    constant_processed_col_a = ["apple", "missing"]
    constant_expected_df = pd.DataFrame.from_dict({"A": constant_processed_col_a})

    assert constant_out_df.equals(constant_expected_df)


def test_chain():
    """Tests basic Chain functionality."""
    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    def udf(df):
        df["A"] *= 2
        return df

    batch_mapper = BatchMapper(fn=udf)
    imputer = SimpleImputer(["B"])
    scaler = StandardScaler(["A", "B"])
    encoder = LabelEncoder("C")
    chain = Chain(scaler, imputer, encoder, batch_mapper)

    # Fit data.
    chain.fit(ds)
    assert imputer.stats_ == {
        "mean(B)": 0.0,
    }
    assert scaler.stats_ == {
        "mean(A)": 0.0,
        "mean(B)": 1.0,
        "std(A)": 1.0,
        "std(B)": 0.0,
    }
    assert encoder.stats_ == {
        "unique_values(C)": {"monday": 0, "sunday": 1, "tuesday": 2}
    }

    # Transform data.
    transformed = chain.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [-2.0, -2.0, 2.0, 2.0]
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

    pred_processed_col_a = [2, 4, None]
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


def test_batch_mapper():
    """Tests batch mapper functionality."""
    old_column = [1, 2, 3, 4]
    to_be_modified = [1, -1, 1, -1]
    in_df = pd.DataFrame.from_dict(
        {"old_column": old_column, "to_be_modified": to_be_modified}
    )
    ds = ray.data.from_pandas(in_df)

    def add_and_modify_udf(df: "pd.DataFrame"):
        df["new_col"] = df["old_column"] + 1
        df["to_be_modified"] *= 2
        return df

    batch_mapper = BatchMapper(fn=add_and_modify_udf)
    batch_mapper.fit(ds)
    transformed = batch_mapper.transform(ds)
    out_df = transformed.to_pandas()

    expected_df = pd.DataFrame.from_dict(
        {
            "old_column": old_column,
            "to_be_modified": [2, -2, 2, -2],
            "new_col": [2, 3, 4, 5],
        }
    )

    assert out_df.equals(expected_df)


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


def test_tokenizer():
    """Tests basic Tokenizer functionality."""

    col_a = ["this is a test", "apple"]
    col_b = ["the quick brown fox jumps over the lazy dog", "banana banana"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    tokenizer = Tokenizer(["A", "B"])
    transformed = tokenizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [["this", "is", "a", "test"], ["apple"]]
    processed_col_b = [
        ["the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"],
        ["banana", "banana"],
    ]
    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    assert out_df.equals(expected_df)


def test_feature_hasher():
    """Tests basic FeatureHasher functionality."""

    col_a = [0, "a", "b"]
    col_b = [0, "a", "c"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    hasher = FeatureHasher(["A", "B"], num_features=5)
    transformed = hasher.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_0 = [0, 0, 1]
    processed_col_1 = [0, 0, 1]
    processed_col_2 = [0, 2, 0]
    processed_col_3 = [2, 0, 0]
    processed_col_4 = [0, 0, 0]

    expected_df = pd.DataFrame.from_dict(
        {
            "hash_A_B_0": processed_col_0,
            "hash_A_B_1": processed_col_1,
            "hash_A_B_2": processed_col_2,
            "hash_A_B_3": processed_col_3,
            "hash_A_B_4": processed_col_4,
        }
    )

    assert out_df.equals(expected_df)


def test_hashing_vectorizer():
    """Tests basic HashingVectorizer functionality."""

    col_a = ["a b b c c c", "a a a a c"]
    col_b = ["apple", "banana banana banana"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    vectorizer = HashingVectorizer(["A", "B"], num_features=3)

    transformed = vectorizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a_0 = [2, 0]
    processed_col_a_1 = [1, 4]
    processed_col_a_2 = [3, 1]
    processed_col_b_0 = [1, 0]
    processed_col_b_1 = [0, 3]
    processed_col_b_2 = [0, 0]

    expected_df = pd.DataFrame.from_dict(
        {
            "hash_A_0": processed_col_a_0,
            "hash_A_1": processed_col_a_1,
            "hash_A_2": processed_col_a_2,
            "hash_B_0": processed_col_b_0,
            "hash_B_1": processed_col_b_1,
            "hash_B_2": processed_col_b_2,
        }
    )

    assert out_df.equals(expected_df)


def test_count_vectorizer():
    """Tests basic CountVectorizer functionality."""

    col_a = ["a b b c c c", "a a a a c"]
    col_b = ["apple", "banana banana banana"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    vectorizer = CountVectorizer(["A", "B"])
    vectorizer.fit(ds)
    assert vectorizer.stats_ == {
        "token_counts(A)": Counter({"a": 5, "c": 4, "b": 2}),
        "token_counts(B)": Counter({"banana": 3, "apple": 1}),
    }

    transformed = vectorizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a_a = [1, 4]
    processed_col_a_c = [3, 1]
    processed_col_a_b = [2, 0]
    processed_col_b_banana = [0, 3]
    processed_col_b_apple = [1, 0]

    expected_df = pd.DataFrame.from_dict(
        {
            "A_a": processed_col_a_a,
            "A_c": processed_col_a_c,
            "A_b": processed_col_a_b,
            "B_banana": processed_col_b_banana,
            "B_apple": processed_col_b_apple,
        }
    )

    assert out_df.equals(expected_df)

    # max_features
    vectorizer = CountVectorizer(["A", "B"], max_features=2)
    vectorizer.fit(ds)

    assert vectorizer.stats_ == {
        "token_counts(A)": Counter({"a": 5, "c": 4}),
        "token_counts(B)": Counter({"banana": 3, "apple": 1}),
    }

    transformed = vectorizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a_a = [1, 4]
    processed_col_a_c = [3, 1]
    processed_col_b_banana = [0, 3]
    processed_col_b_apple = [1, 0]

    expected_df = pd.DataFrame.from_dict(
        {
            "A_a": processed_col_a_a,
            "A_c": processed_col_a_c,
            "B_banana": processed_col_b_banana,
            "B_apple": processed_col_b_apple,
        }
    )

    assert out_df.equals(expected_df)


def test_simple_split_tokenizer():
    # Tests simple_split_tokenizer.
    assert simple_split_tokenizer("one_word") == ["one_word"]
    assert simple_split_tokenizer("two words") == ["two", "words"]
    assert simple_split_tokenizer("One fish. Two fish.") == [
        "One",
        "fish.",
        "Two",
        "fish.",
    ]


def test_simple_hash():
    # Tests simple_hash determinism.
    assert simple_hash(1, 100) == 83
    assert simple_hash("a", 100) == 52
    assert simple_hash("banana", 100) == 16
    assert simple_hash([1, 2, "apple"], 100) == 37


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
