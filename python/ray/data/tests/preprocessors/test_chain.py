import pandas as pd
import pytest

import ray
from ray.air.util.data_batch_conversion import BatchFormat
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors import Chain, LabelEncoder, SimpleImputer, StandardScaler


def test_chain():
    """Tests basic Chain functionality."""
    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    imputer = SimpleImputer(["B"])
    scaler = StandardScaler(["A", "B"])
    encoder = LabelEncoder("C")
    chain = Chain(scaler, imputer, encoder)

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


def test_nested_chain_state():
    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    def create_chain():
        imputer = SimpleImputer(["B"])
        scaler = StandardScaler(["A", "B"])
        encoder = LabelEncoder("C")
        return Chain(Chain(scaler, imputer), encoder)

    chain = create_chain()
    assert chain.fit_status() == Preprocessor.FitStatus.NOT_FITTED

    chain = create_chain()
    chain.preprocessors[1].fit(ds)
    assert chain.fit_status() == Preprocessor.FitStatus.PARTIALLY_FITTED

    chain = create_chain()
    chain.preprocessors[0].fit(ds)
    assert chain.fit_status() == Preprocessor.FitStatus.PARTIALLY_FITTED

    chain.preprocessors[1].fit(ds)
    assert chain.fit_status() == Preprocessor.FitStatus.FITTED

    chain = create_chain()
    chain.fit(ds)
    assert chain.fit_status() == Preprocessor.FitStatus.FITTED


def test_nested_chain():
    """Tests Chain-inside-Chain functionality."""
    col_a = [-1, -1, 1, 1]
    col_b = [1, 1, 1, None]
    col_c = ["sunday", "monday", "tuesday", "tuesday"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
    ds = ray.data.from_pandas(in_df)

    imputer = SimpleImputer(["B"])
    scaler = StandardScaler(["A", "B"])
    encoder = LabelEncoder("C")
    chain = Chain(Chain(scaler, imputer), encoder)

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


class PreprocessorWithoutTransform(Preprocessor):
    pass


def test_determine_transform_to_use():
    # Test that _determine_transform_to_use doesn't throw any exceptions
    # and selects the transform function of the underlying preprocessor
    # while dealing with the nested Chain case.

    # Check that error is propagated correctly
    with pytest.raises(NotImplementedError):
        chain = Chain(PreprocessorWithoutTransform())
        chain._determine_transform_to_use()

    # Should have no errors from here on
    preprocessor = SimpleImputer(["A"])
    chain1 = Chain(preprocessor)
    format1 = chain1._determine_transform_to_use()
    assert format1 == BatchFormat.PANDAS

    chain2 = Chain(chain1)
    format2 = chain2._determine_transform_to_use()

    assert format1 == format2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
