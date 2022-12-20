import re
import warnings
from typing import Dict, Union
from unittest.mock import patch

import numpy as np
import pandas as pd
import pyarrow
import pytest

import ray
from ray.air.constants import MAX_REPR_LENGTH
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors import (
    BatchMapper,
    Categorizer,
    Chain,
    Concatenator,
    CountVectorizer,
    FeatureHasher,
    HashingVectorizer,
    LabelEncoder,
    MaxAbsScaler,
    MinMaxScaler,
    MultiHotEncoder,
    Normalizer,
    OneHotEncoder,
    OrdinalEncoder,
    PowerTransformer,
    RobustScaler,
    SimpleImputer,
    StandardScaler,
    Tokenizer,
)


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


@pytest.mark.parametrize(
    "preprocessor",
    [
        BatchMapper(fn=lambda x: x, batch_format="pandas"),
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

    assert with_pandas.transform(ds).dataset_format() == "pandas"

    assert with_pandas_and_numpy.transform(ds).dataset_format() == "pandas"


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

    assert with_pandas.transform(ds).dataset_format() == "pandas"

    assert with_numpy.transform(ds).dataset_format() == "arrow"

    # Auto select data_format = "arrow" -> batch_format = "numpy" for performance
    assert with_pandas_and_numpy.transform(ds).dataset_format() == "arrow"


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
