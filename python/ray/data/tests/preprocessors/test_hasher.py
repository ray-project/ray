import pandas as pd
import pytest

import ray
from ray.data.preprocessors import FeatureHasher


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
