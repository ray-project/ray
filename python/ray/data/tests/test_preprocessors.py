import warnings
from collections import Counter
import re
from unittest.mock import patch
from typing import Dict, Union

import numpy as np
import pandas as pd
import pyarrow
import pytest

import ray
from ray.data.preprocessor import Preprocessor, PreprocessorNotFittedException
from ray.data.preprocessors import (
    BatchMapper,
    Chain,
    LabelEncoder,
    MinMaxScaler,
    OneHotEncoder,
    OrdinalEncoder,
    SimpleImputer,
    StandardScaler,
    CustomKBinsDiscretizer,
    UniformKBinsDiscretizer,
)
from ray.data.preprocessors.encoder import Categorizer, MultiHotEncoder
from ray.data.preprocessors.hasher import FeatureHasher
from ray.data.preprocessors.normalizer import Normalizer
from ray.data.preprocessors.scaler import MaxAbsScaler, RobustScaler
from ray.data.preprocessors.concatenator import Concatenator
from ray.data.preprocessors.tokenizer import Tokenizer
from ray.data.preprocessors.transformer import PowerTransformer
from ray.data.preprocessors.utils import simple_hash, simple_split_tokenizer
from ray.data.preprocessors.vectorizer import CountVectorizer, HashingVectorizer
from ray.air.constants import MAX_REPR_LENGTH


@pytest.fixture
def create_dummy_preprocessors():
    class DummyPreprocessorWithNothing(Preprocessor):
        _is_fittable = False

    class DummyPreprocessorWithPandas(DummyPreprocessorWithNothing):
        def _transform_pandas(self, df: "pd.DataFrame") -> "pd.DataFrame":
            return df

    class DummyPreprocessorWithNumpy(DummyPreprocessorWithNothing):
        batch_format = "numpy"

        def _transform_numpy(
            self, np_data: Union[np.ndarray, Dict[str, np.ndarray]]
        ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
            return np_data

    class DummyPreprocessorWithPandasAndNumpy(DummyPreprocessorWithNothing):
        def _transform_pandas(self, df: "pd.DataFrame") -> "pd.DataFrame":
            return df

        def _transform_numpy(
            self, np_data: Union[np.ndarray, Dict[str, np.ndarray]]
        ) -> Union[np.ndarray, Dict[str, np.ndarray]]:
            return np_data

    yield (
        DummyPreprocessorWithNothing(),
        DummyPreprocessorWithPandas(),
        DummyPreprocessorWithNumpy(),
        DummyPreprocessorWithPandasAndNumpy(),
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


@pytest.mark.parametrize(
    "preprocessor",
    [
        BatchMapper(fn=lambda x: x),
        Categorizer(columns=["X"]),
        CountVectorizer(columns=["X"]),
        Chain(StandardScaler(columns=["X"]), MinMaxScaler(columns=["X"])),
        FeatureHasher(columns=["X"], num_features=1),
        HashingVectorizer(columns=["X"], num_features=1),
        LabelEncoder(label_column="X"),
        MaxAbsScaler(columns=["X"]),
        MinMaxScaler(columns=["X"]),
        MultiHotEncoder(columns=["X"]),
        Normalizer(columns=["X"]),
        OneHotEncoder(columns=["X"]),
        OrdinalEncoder(columns=["X"]),
        PowerTransformer(columns=["X"], power=1),
        RobustScaler(columns=["X"]),
        SimpleImputer(columns=["X"]),
        StandardScaler(columns=["X"]),
        Concatenator(),
        Tokenizer(columns=["X"]),
    ],
)
def test_repr(preprocessor):
    representation = repr(preprocessor)

    assert len(representation) < MAX_REPR_LENGTH
    pattern = re.compile(f"^{preprocessor.__class__.__name__}\\((.*)\\)$")
    assert pattern.match(representation)


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


def test_ordinal_encoder_no_encode_list():
    """Tests OrdinalEncoder with encode_lists=False."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    col_d = [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c, "D": col_d})
    ds = ray.data.from_pandas(in_df)

    encoder = OrdinalEncoder(["B", "C", "D"], encode_lists=False)

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)
    assert encoder.stats_ == {
        "unique_values(B)": {"cold": 0, "hot": 1, "warm": 2},
        "unique_values(C)": {1: 0, 5: 1, 10: 2},
        "unique_values(D)": {
            tuple(): 0,
            ("cold", "cold"): 1,
            ("hot", "warm", "cold"): 2,
            ("warm",): 3,
        },
    }

    # Transform data.
    print("transform")
    transformed = encoder.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = col_a
    processed_col_b = [2, 0, 1, 0]
    processed_col_c = [0, 2, 1, 2]
    processed_col_d = [3, 0, 2, 1]
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
    pred_col_d = [["cold", "cold"], [], ["other", "cold"]]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c, "D": pred_col_d}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = pred_col_a
    pred_processed_col_b = [0, 2, None]
    pred_processed_col_c = [2, 0, None]
    pred_processed_col_d = [1, 0, None]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B": pred_processed_col_b,
            "C": pred_processed_col_c,
            "D": pred_processed_col_d,
        }
    )

    assert pred_out_df.equals(pred_expected_df)


def test_one_hot_encoder():
    """Tests basic OneHotEncoder functionality."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    col_d = [["warm"], [], ["hot", "warm", "cold"], ["cold", "cold"]]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c, "D": col_d})
    ds = ray.data.from_pandas(in_df)

    encoder = OneHotEncoder(["B", "C", "D"])

    # Transform with unfitted preprocessor.
    with pytest.raises(PreprocessorNotFittedException):
        encoder.transform(ds)

    # Fit data.
    encoder.fit(ds)

    assert encoder.stats_ == {
        "unique_values(B)": {"cold": 0, "hot": 1, "warm": 2},
        "unique_values(C)": {1: 0, 5: 1, 10: 2},
        "unique_values(D)": {
            tuple(): 0,
            ("cold", "cold"): 1,
            ("hot", "warm", "cold"): 2,
            ("warm",): 3,
        },
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
    processed_col_d_empty = [0, 1, 0, 0]
    processed_col_d_cold_cold = [0, 0, 0, 1]
    processed_col_d_hot_warm_cold = [0, 0, 1, 0]
    processed_col_d_warm = [1, 0, 0, 0]
    expected_df = pd.DataFrame.from_dict(
        {
            "A": processed_col_a,
            "B_cold": processed_col_b_cold,
            "B_hot": processed_col_b_hot,
            "B_warm": processed_col_b_warm,
            "C_1": processed_col_c_1,
            "C_5": processed_col_c_5,
            "C_10": processed_col_c_10,
            "D_()": processed_col_d_empty,
            "D_('cold', 'cold')": processed_col_d_cold_cold,
            "D_('hot', 'warm', 'cold')": processed_col_d_hot_warm_cold,
            "D_('warm',)": processed_col_d_warm,
        }
    )

    assert out_df.equals(expected_df)

    # Transform batch.
    pred_col_a = ["blue", "yellow", None]
    pred_col_b = ["cold", "warm", "other"]
    pred_col_c = [10, 1, 20]
    pred_col_d = [["cold", "cold"], [], ["other", "cold"]]
    pred_in_df = pd.DataFrame.from_dict(
        {"A": pred_col_a, "B": pred_col_b, "C": pred_col_c, "D": pred_col_d}
    )

    pred_out_df = encoder.transform_batch(pred_in_df)

    pred_processed_col_a = ["blue", "yellow", None]
    pred_processed_col_b_cold = [1, 0, 0]
    pred_processed_col_b_hot = [0, 0, 0]
    pred_processed_col_b_warm = [0, 1, 0]
    pred_processed_col_c_1 = [0, 1, 0]
    pred_processed_col_c_5 = [0, 0, 0]
    pred_processed_col_c_10 = [1, 0, 0]
    processed_col_d_empty = [0, 1, 0]
    processed_col_d_cold_cold = [1, 0, 0]
    processed_col_d_hot_warm_cold = [0, 0, 0]
    processed_col_d_warm = [0, 0, 0]
    pred_expected_df = pd.DataFrame.from_dict(
        {
            "A": pred_processed_col_a,
            "B_cold": pred_processed_col_b_cold,
            "B_hot": pred_processed_col_b_hot,
            "B_warm": pred_processed_col_b_warm,
            "C_1": pred_processed_col_c_1,
            "C_5": pred_processed_col_c_5,
            "C_10": pred_processed_col_c_10,
            "D_()": processed_col_d_empty,
            "D_('cold', 'cold')": processed_col_d_cold_cold,
            "D_('hot', 'warm', 'cold')": processed_col_d_hot_warm_cold,
            "D_('warm',)": processed_col_d_warm,
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


def test_one_hot_encoder_with_max_categories():
    """Tests basic OneHotEncoder functionality with limit."""
    col_a = ["red", "green", "blue", "red"]
    col_b = ["warm", "cold", "hot", "cold"]
    col_c = [1, 10, 5, 10]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    encoder = OneHotEncoder(["B", "C"], max_categories={"B": 2})

    ds_out = encoder.fit_transform(ds)
    assert len(ds_out.to_pandas().columns) == 1 + 2 + 3


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


def test_concatenator():
    """Tests basic Concatenator functionality."""
    df = pd.DataFrame(
        {
            "a": [1, 2, 3, 4],
            "b": [5, 6, 7, 8],
        }
    )
    ds = ray.data.from_pandas(df)
    prep = Concatenator(output_column_name="c")
    new_ds = prep.transform(ds)
    for i, row in enumerate(new_ds.take()):
        assert np.array_equal(row["c"], np.array([i + 1, i + 5]))

    df = pd.DataFrame({"a": [1, 2, 3, 4]})
    ds = ray.data.from_pandas(df)
    prep = Concatenator(output_column_name="c", exclude=["b"], raise_if_missing=True)

    with pytest.raises(ValueError, match="'b'"):
        prep.transform(ds)

    # Test exclude working
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [2, 3, 4, 5], "c": [3, 4, 5, 6]})
    ds = ray.data.from_pandas(df)
    prep = Concatenator(exclude=["b"])
    new_ds = prep.transform(ds)
    for i, row in enumerate(new_ds.take()):
        assert set(row) == {"concat_out", "b"}

    # Test include working
    prep = Concatenator(include=["a", "b"])
    new_ds = prep.transform(ds)
    for i, row in enumerate(new_ds.take()):
        assert set(row) == {"concat_out", "c"}

    # Test exclude overrides include
    prep = Concatenator(include=["a", "b"], exclude=["b"])
    new_ds = prep.transform(ds)
    for i, row in enumerate(new_ds.take()):
        assert set(row) == {"concat_out", "b", "c"}

    # Test that it works with string types.
    df = pd.DataFrame({"a": ["string", "string2", "string3"]})
    ds = ray.data.from_pandas(df)
    prep = Concatenator(output_column_name="huh")
    new_ds = prep.transform(ds)
    assert "huh" in set(new_ds.schema().names)


@pytest.mark.parametrize("bins", (3, {"A": 4, "B": 3}))
@pytest.mark.parametrize(
    "dtypes",
    (
        None,
        {"A": int, "B": int},
        {"A": int, "B": pd.CategoricalDtype(["cat1", "cat2", "cat3"], ordered=True)},
    ),
)
@pytest.mark.parametrize("right", (True, False))
@pytest.mark.parametrize("include_lowest", (True, False))
def test_uniform_kbins_discretizer(
    bins,
    dtypes,
    right,
    include_lowest,
):
    """Tests basic UniformKBinsDiscretizer functionality."""

    col_a = [0.2, 1.4, 2.5, 6.2, 9.7, 2.1]
    col_b = [0.2, 1.4, 2.5, 6.2, 9.7, 2.1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df).repartition(2)

    discretizer = UniformKBinsDiscretizer(
        ["A", "B"], bins=bins, dtypes=dtypes, right=right, include_lowest=include_lowest
    )

    transformed = discretizer.fit_transform(ds)
    out_df = transformed.to_pandas()

    if isinstance(bins, dict):
        bins_A = bins["A"]
        bins_B = bins["B"]
    else:
        bins_A = bins_B = bins

    labels_A = False
    ordered_A = True
    labels_B = False
    ordered_B = True
    if isinstance(dtypes, dict):
        if isinstance(dtypes.get("A"), pd.CategoricalDtype):
            labels_A = dtypes.get("A").categories
            ordered_A = dtypes.get("A").ordered
        if isinstance(dtypes.get("B"), pd.CategoricalDtype):
            labels_B = dtypes.get("B").categories
            ordered_B = dtypes.get("B").ordered

    assert out_df["A"].equals(
        pd.cut(
            in_df["A"],
            bins_A,
            labels=labels_A,
            ordered=ordered_A,
            right=right,
            include_lowest=include_lowest,
        )
    )
    assert out_df["B"].equals(
        pd.cut(
            in_df["B"],
            bins_B,
            labels=labels_B,
            ordered=ordered_B,
            right=right,
            include_lowest=include_lowest,
        )
    )


@pytest.mark.parametrize(
    "bins", ([3, 4, 6, 9], {"A": [3, 4, 6, 8, 9], "B": [3, 4, 6, 9]})
)
@pytest.mark.parametrize(
    "dtypes",
    (
        None,
        {"A": int, "B": int},
        {"A": int, "B": pd.CategoricalDtype(["cat1", "cat2", "cat3"], ordered=True)},
    ),
)
@pytest.mark.parametrize("right", (True, False))
@pytest.mark.parametrize("include_lowest", (True, False))
def test_custom_kbins_discretizer(
    bins,
    dtypes,
    right,
    include_lowest,
):
    """Tests basic CustomKBinsDiscretizer functionality."""

    col_a = [0.2, 1.4, 2.5, 6.2, 9.7, 2.1]
    col_b = [0.2, 1.4, 2.5, 6.2, 9.7, 2.1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df).repartition(2)

    discretizer = CustomKBinsDiscretizer(
        ["A", "B"], bins=bins, dtypes=dtypes, right=right, include_lowest=include_lowest
    )

    transformed = discretizer.transform(ds)
    out_df = transformed.to_pandas()

    if isinstance(bins, dict):
        bins_A = bins["A"]
        bins_B = bins["B"]
    else:
        bins_A = bins_B = bins

    labels_A = False
    ordered_A = True
    labels_B = False
    ordered_B = True
    if isinstance(dtypes, dict):
        if isinstance(dtypes.get("A"), pd.CategoricalDtype):
            labels_A = dtypes.get("A").categories
            ordered_A = dtypes.get("A").ordered
        if isinstance(dtypes.get("B"), pd.CategoricalDtype):
            labels_B = dtypes.get("B").categories
            ordered_B = dtypes.get("B").ordered

    assert out_df["A"].equals(
        pd.cut(
            in_df["A"],
            bins_A,
            labels=labels_A,
            ordered=ordered_A,
            right=right,
            include_lowest=include_lowest,
        )
    )
    assert out_df["B"].equals(
        pd.cut(
            in_df["B"],
            bins_B,
            labels=labels_B,
            ordered=ordered_B,
            right=right,
            include_lowest=include_lowest,
        )
    )


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
    # This dataframe represents the counts from the documents "I like Python" and "I
    # dislike Python".
    token_counts = pd.DataFrame(
        {"I": [1, 1], "like": [1, 0], "dislike": [0, 1], "Python": [1, 1]}
    )

    hasher = FeatureHasher(["I", "like", "dislike", "Python"], num_features=256)
    document_term_matrix = hasher.fit_transform(
        ray.data.from_pandas(token_counts)
    ).to_pandas()

    # Document-term matrix should have shape (# documents, # features)
    assert document_term_matrix.shape == (2, 256)

    # The tokens tokens "I", "like", and "Python" should be hashed to distinct indices
    # for adequately large `num_features`.
    assert document_term_matrix.iloc[0].sum() == 3
    assert all(document_term_matrix.iloc[0] <= 1)

    # The tokens tokens "I", "dislike", and "Python" should be hashed to distinct
    # indices for adequately large `num_features`.
    assert document_term_matrix.iloc[1].sum() == 3
    assert all(document_term_matrix.iloc[1] <= 1)


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


def test_numpy_pandas_support_simple_dataset(create_dummy_preprocessors):
    # Case 1: simple dataset. No support
    (
        with_nothing,
        with_pandas,
        with_numpy,
        with_pandas_and_numpy,
    ) = create_dummy_preprocessors

    ds = ray.data.range(10)
    with pytest.raises(ValueError):
        with_nothing.transform(ds)

    with pytest.raises(ValueError):
        with_pandas.transform(ds)

    with pytest.raises(ValueError):
        with_numpy.transform(ds)

    with pytest.raises(ValueError):
        with_pandas_and_numpy.transform(ds)


def test_numpy_pandas_support_pandas_dataset(create_dummy_preprocessors):
    # Case 2: pandas dataset
    (
        with_nothing,
        with_pandas,
        _,
        with_pandas_and_numpy,
    ) = create_dummy_preprocessors
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["A", "B", "C"])

    ds = ray.data.from_pandas(df)
    with pytest.raises(NotImplementedError):
        with_nothing.transform(ds)

    assert with_pandas.transform(ds)._dataset_format() == "pandas"

    assert with_pandas_and_numpy.transform(ds)._dataset_format() == "pandas"


def test_numpy_pandas_support_arrow_dataset(create_dummy_preprocessors):
    # Case 3: arrow dataset
    (
        with_nothing,
        with_pandas,
        with_numpy,
        with_pandas_and_numpy,
    ) = create_dummy_preprocessors
    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["A", "B", "C"])

    ds = ray.data.from_arrow(pyarrow.Table.from_pandas(df))
    with pytest.raises(NotImplementedError):
        with_nothing.transform(ds)

    assert with_pandas.transform(ds)._dataset_format() == "pandas"

    assert with_numpy.transform(ds)._dataset_format() == "arrow"

    # Auto select data_format = "arrow" -> batch_format = "numpy" for performance
    assert with_pandas_and_numpy.transform(ds)._dataset_format() == "arrow"


def test_numpy_pandas_support_transform_batch_wrong_format(create_dummy_preprocessors):
    # Case 1: simple dataset. No support
    (
        with_nothing,
        with_pandas,
        with_numpy,
        with_pandas_and_numpy,
    ) = create_dummy_preprocessors

    batch = [1, 2, 3]
    with pytest.raises(NotImplementedError):
        with_nothing.transform_batch(batch)

    with pytest.raises(NotImplementedError):
        with_pandas.transform_batch(batch)

    with pytest.raises(NotImplementedError):
        with_numpy.transform_batch(batch)

    with pytest.raises(NotImplementedError):
        with_pandas_and_numpy.transform_batch(batch)


def test_numpy_pandas_support_transform_batch_pandas(create_dummy_preprocessors):
    # Case 2: pandas dataset
    (
        with_nothing,
        with_pandas,
        with_numpy,
        with_pandas_and_numpy,
    ) = create_dummy_preprocessors

    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["A", "B", "C"])
    df_single_column = pd.DataFrame([1, 2, 3], columns=["A"])
    with pytest.raises(NotImplementedError):
        with_nothing.transform_batch(df)
    with pytest.raises(NotImplementedError):
        with_nothing.transform_batch(df_single_column)

    assert isinstance(with_pandas.transform_batch(df), pd.DataFrame)
    assert isinstance(with_pandas.transform_batch(df_single_column), pd.DataFrame)

    assert isinstance(with_numpy.transform_batch(df), (np.ndarray, dict))
    # We can get pd.DataFrame after returning numpy data from UDF
    assert isinstance(with_numpy.transform_batch(df_single_column), (np.ndarray, dict))

    assert isinstance(with_pandas_and_numpy.transform_batch(df), pd.DataFrame)
    assert isinstance(
        with_pandas_and_numpy.transform_batch(df_single_column), pd.DataFrame
    )


def test_numpy_pandas_support_transform_batch_arrow(create_dummy_preprocessors):
    # Case 3: arrow dataset
    (
        with_nothing,
        with_pandas,
        with_numpy,
        with_pandas_and_numpy,
    ) = create_dummy_preprocessors

    df = pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=["A", "B", "C"])
    df_single_column = pd.DataFrame([1, 2, 3], columns=["A"])

    table = pyarrow.Table.from_pandas(df)
    table_single_column = pyarrow.Table.from_pandas(df_single_column)
    with pytest.raises(NotImplementedError):
        with_nothing.transform_batch(table)
    with pytest.raises(NotImplementedError):
        with_nothing.transform_batch(table_single_column)

    assert isinstance(with_pandas.transform_batch(table), pd.DataFrame)
    assert isinstance(with_pandas.transform_batch(table_single_column), pd.DataFrame)

    assert isinstance(with_numpy.transform_batch(table), (np.ndarray, dict))
    # We can get pyarrow.Table after returning numpy data from UDF
    assert isinstance(
        with_numpy.transform_batch(table_single_column), (np.ndarray, dict)
    )
    # Auto select data_format = "arrow" -> batch_format = "numpy" for performance
    assert isinstance(with_pandas_and_numpy.transform_batch(table), (np.ndarray, dict))
    # We can get pyarrow.Table after returning numpy data from UDF
    assert isinstance(
        with_pandas_and_numpy.transform_batch(table_single_column), (np.ndarray, dict)
    )


def test_numpy_pandas_support_transform_batch_tensor(create_dummy_preprocessors):
    # Case 4: tensor dataset created by from numpy data directly
    (
        with_nothing,
        _,
        with_numpy,
        with_pandas_and_numpy,
    ) = create_dummy_preprocessors
    np_data = np.arange(12).reshape(3, 2, 2)
    np_single_column = {"A": np.arange(12).reshape(3, 2, 2)}
    np_multi_column = {
        "A": np.arange(12).reshape(3, 2, 2),
        "B": np.arange(12, 24).reshape(3, 2, 2),
    }

    with pytest.raises(NotImplementedError):
        with_nothing.transform_batch(np_data)
    with pytest.raises(NotImplementedError):
        with_nothing.transform_batch(np_single_column)
    with pytest.raises(NotImplementedError):
        with_nothing.transform_batch(np_multi_column)

    assert isinstance(with_numpy.transform_batch(np_data), np.ndarray)
    assert isinstance(with_numpy.transform_batch(np_single_column), dict)
    assert isinstance(with_numpy.transform_batch(np_multi_column), dict)

    assert isinstance(with_pandas_and_numpy.transform_batch(np_data), np.ndarray)
    assert isinstance(with_pandas_and_numpy.transform_batch(np_single_column), dict)
    assert isinstance(with_pandas_and_numpy.transform_batch(np_multi_column), dict)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
