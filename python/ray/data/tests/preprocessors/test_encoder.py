import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.preprocessor import PreprocessorNotFittedException
from ray.data.preprocessors import (
    Categorizer,
    LabelEncoder,
    MultiHotEncoder,
    OneHotEncoder,
    OrdinalEncoder,
)


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

    # Verify that `fit` and `transform` work with ndarrays.
    df = pd.DataFrame({"column": [np.array(["A"]), np.array(["A", "B"])]})
    ds = ray.data.from_pandas(df)
    encoder = MultiHotEncoder(["column"])
    transformed = encoder.fit_transform(ds)
    encodings = [record["column"] for record in transformed.take_all()]
    assert encodings == [[1, 0], [1, 1]]


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
