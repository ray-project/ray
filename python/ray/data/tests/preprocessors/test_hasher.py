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

    hasher = FeatureHasher(
        ["I", "like", "dislike", "Python"],
        num_features=256,
        output_column="hashed_features",
    )
    document_term_matrix = hasher.fit_transform(
        ray.data.from_pandas(token_counts)
    ).to_pandas()

    hashed_features = document_term_matrix["hashed_features"]
    # Document-term matrix should have shape (# documents, # features)
    assert hashed_features.shape == (2,)

    # The tokens tokens "I", "like", and "Python" should be hashed to distinct indices
    # for adequately large `num_features`.
    assert len(hashed_features.iloc[0]) == 256
    assert hashed_features.iloc[0].sum() == 3
    assert all(hashed_features.iloc[0] <= 1)

    # The tokens tokens "I", "dislike", and "Python" should be hashed to distinct
    # indices for adequately large `num_features`.
    assert len(hashed_features.iloc[1]) == 256
    assert hashed_features.iloc[1].sum() == 3
    assert all(hashed_features.iloc[1] <= 1)


def test_feature_hasher_serialization():
    """Test FeatureHasher serialization and deserialization functionality."""
    from ray.data.preprocessor import SerializablePreprocessorBase

    # Create hasher
    hasher = FeatureHasher(
        columns=["I", "like", "Python"], num_features=8, output_column="hashed"
    )

    # Serialize using CloudPickle
    serialized = hasher.serialize()

    # Verify it's binary CloudPickle format
    assert isinstance(serialized, bytes)
    assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

    # Deserialize
    deserialized = FeatureHasher.deserialize(serialized)

    # Verify type and field values
    assert isinstance(deserialized, FeatureHasher)
    assert deserialized.columns == ["I", "like", "Python"]
    assert deserialized.num_features == 8
    assert deserialized.output_column == "hashed"

    # Verify it works correctly
    df = pd.DataFrame({"I": [1, 1], "like": [1, 0], "Python": [1, 1]})
    result = deserialized.transform_batch(df)

    # Verify hashing was applied correctly
    assert "hashed" in result.columns
    assert len(result["hashed"][0]) == 8
    assert len(result["hashed"][1]) == 8


def test_feature_hasher_non_default_index_and_empty_batch():
    """Tests FeatureHasher on DataFrames with non-default indices and zero rows."""
    hasher = FeatureHasher(
        columns=["I", "like", "Python"], num_features=8, output_column="hashed"
    )

    df = pd.DataFrame({"I": [1, 1], "like": [1, 0], "Python": [1, 1]}, index=[10, 20])
    result = hasher.transform_batch(df)
    assert list(result.index) == [10, 20]
    assert not result["hashed"].isna().any()
    assert result.loc[10, "hashed"].sum() == 3
    assert result.loc[20, "hashed"].sum() == 2

    empty_df = pd.DataFrame(
        {
            "I": pd.Series([], dtype="int64"),
            "like": pd.Series([], dtype="int64"),
            "Python": pd.Series([], dtype="int64"),
        }
    )
    empty_result = hasher.transform_batch(empty_df)
    assert len(empty_result) == 0
    assert "hashed" in empty_result.columns


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
