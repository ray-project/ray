import pandas as pd
import pytest

import ray
from ray.data.preprocessors import FeatureHasher, HashingVectorizer


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
