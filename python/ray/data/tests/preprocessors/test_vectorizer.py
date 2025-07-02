from collections import Counter

import pandas as pd
import pytest

import ray
from ray.data.preprocessors import CountVectorizer, HashingVectorizer


def test_count_vectorizer():
    """Tests basic CountVectorizer functionality."""

    # Increase data size & repartition to test for
    # discuss.ray.io/t/xgboost-ray-crashes-when-used-for-multiclass-text-classification
    row_multiplier = 100000

    col_a = ["a b b c c c", "a a a a c"] * row_multiplier
    col_b = ["apple", "banana banana banana"] * row_multiplier
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df).repartition(10)

    vectorizer = CountVectorizer(["A", "B"])
    vectorizer.fit(ds)
    assert vectorizer.stats_ == {
        "token_counts(A)": Counter(
            {"a": 5 * row_multiplier, "c": 4 * row_multiplier, "b": 2 * row_multiplier}
        ),
        "token_counts(B)": Counter(
            {"banana": 3 * row_multiplier, "apple": 1 * row_multiplier}
        ),
    }

    transformed = vectorizer.transform(ds)
    out_df = transformed.to_pandas(limit=float("inf"))

    processed_col_a = [[1, 3, 2], [4, 1, 0]] * row_multiplier
    processed_col_b = [[0, 1], [3, 0]] * row_multiplier

    expected_df = pd.DataFrame.from_dict(
        {
            "A": processed_col_a,
            "B": processed_col_b,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # max_features
    vectorizer = CountVectorizer(["A", "B"], max_features=2)
    vectorizer.fit(ds)

    assert vectorizer.stats_ == {
        "token_counts(A)": Counter({"a": 5 * row_multiplier, "c": 4 * row_multiplier}),
        "token_counts(B)": Counter(
            {"banana": 3 * row_multiplier, "apple": 1 * row_multiplier}
        ),
    }

    transformed = vectorizer.transform(ds)
    out_df = transformed.to_pandas(limit=float("inf"))

    processed_col_a = [[1, 3], [4, 1]] * row_multiplier
    processed_col_b = [[0, 1], [3, 0]] * row_multiplier

    expected_df = pd.DataFrame.from_dict(
        {
            "A": processed_col_a,
            "B": processed_col_b,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Test append mode
    with pytest.raises(
        ValueError, match="The length of columns and output_columns must match."
    ):
        CountVectorizer(
            columns=["A", "B"],
            output_columns=[
                "A_counts"
            ],  # Should provide same number of output columns as input
        )

    vectorizer = CountVectorizer(["A", "B"], output_columns=["A_counts", "B_counts"])
    vectorizer.fit(ds)

    transformed = vectorizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [[1, 3, 2], [4, 1, 0]] * row_multiplier
    processed_col_b = [[0, 1], [3, 0]] * row_multiplier

    expected_df = pd.DataFrame.from_dict(
        {
            "A": col_a,
            "B": col_b,
            "A_counts": processed_col_a,
            "B_counts": processed_col_b,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)


def test_hashing_vectorizer():
    """Tests basic HashingVectorizer functionality."""

    col_a = ["a b b c c c", "a a a a c"]
    col_b = ["apple", "banana banana banana"]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
    ds = ray.data.from_pandas(in_df)

    vectorizer = HashingVectorizer(["A", "B"], num_features=3)

    transformed = vectorizer.transform(ds)
    out_df = transformed.to_pandas()

    processed_col_a = [[2, 1, 3], [0, 4, 1]]
    processed_col_b = [[1, 0, 0], [0, 3, 0]]

    expected_df = pd.DataFrame.from_dict({"A": processed_col_a, "B": processed_col_b})

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)

    # Test append mode
    with pytest.raises(
        ValueError, match="The length of columns and output_columns must match."
    ):
        HashingVectorizer(
            columns=["A", "B"],
            num_features=3,
            output_columns=[
                "A_hashed"
            ],  # Should provide same number of output columns as input
        )

    vectorizer = HashingVectorizer(
        ["A", "B"], num_features=3, output_columns=["A_hashed", "B_hashed"]
    )

    transformed = vectorizer.transform(ds)
    out_df = transformed.to_pandas()

    expected_df = pd.DataFrame.from_dict(
        {
            "A": col_a,
            "B": col_b,
            "A_hashed": processed_col_a,
            "B_hashed": processed_col_b,
        }
    )

    pd.testing.assert_frame_equal(out_df, expected_df, check_like=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
