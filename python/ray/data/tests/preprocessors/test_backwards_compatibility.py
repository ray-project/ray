"""
Backwards compatibility tests for Preprocessor private field renaming.

These tests verify that preprocessors pickled with old public field names
(e.g., 'columns') can be deserialized correctly after fields were renamed
to private (e.g., '_columns').

The __setstate__ method in each preprocessor handles migration automatically.
"""

import numpy as np
import pandas as pd
import pytest

import ray
from ray.data.preprocessors import (
    Chain,
    Concatenator,
    CountVectorizer,
    FeatureHasher,
    HashingVectorizer,
    Normalizer,
    PowerTransformer,
    StandardScaler,
    Tokenizer,
)


class TestConcatenatorBackwardsCompatibility:
    """Test backwards compatibility for Concatenator preprocessor."""

    def test_concatenator_deserialize_backward_compat(self):
        """Deserialize with missing _flatten field applies default value."""
        concatenator = Concatenator(columns=["A", "B"], flatten=True)
        delattr(concatenator, "_flatten")

        data = concatenator.serialize()
        restored = Concatenator.deserialize(data)

        assert isinstance(restored, Concatenator)
        assert restored.flatten is False

    def test_concatenator_all_old_public_field_names(self):
        """Migration from old public field names to new private fields."""
        concatenator = Concatenator.__new__(Concatenator)
        state = {
            "columns": ["A", "B"],
            "output_column_name": "concat",
            "dtype": np.float32,
            "raise_if_missing": True,
            "flatten": True,
        }
        concatenator.__setstate__(state)

        assert concatenator.columns == ["A", "B"]
        assert concatenator.output_column_name == "concat"
        assert concatenator.dtype == np.float32
        assert concatenator.raise_if_missing is True
        assert concatenator.flatten is True


class TestNormalizerBackwardsCompatibility:
    """Test backwards compatibility for Normalizer preprocessor."""

    def test_normalizer_deserialize_backward_compat(self):
        """Deserialize with missing private fields applies defaults."""
        normalizer = Normalizer(columns=["A", "B"], norm="l1")
        delattr(normalizer, "_columns")
        delattr(normalizer, "_norm")
        delattr(normalizer, "_output_columns")

        data = normalizer.serialize()
        restored = Normalizer.deserialize(data)

        assert isinstance(restored, Normalizer)
        assert restored.columns == []
        assert restored.norm == "l2"
        assert restored.output_columns == []

    def test_normalizer_old_public_field_names(self):
        """Migration from old public field names to new private fields."""
        normalizer = Normalizer.__new__(Normalizer)
        state = {
            "columns": ["A", "B"],
            "norm": "l1",
            "output_columns": ["A_norm", "B_norm"],
        }
        normalizer.__setstate__(state)

        assert normalizer.columns == ["A", "B"]
        assert normalizer.norm == "l1"
        assert normalizer.output_columns == ["A_norm", "B_norm"]


class TestTokenizerBackwardsCompatibility:
    """Test backwards compatibility for Tokenizer preprocessor."""

    def test_tokenizer_deserialize_backward_compat(self):
        """Deserialize with missing private fields applies defaults."""
        tokenizer = Tokenizer(columns=["text"])
        delattr(tokenizer, "_columns")
        delattr(tokenizer, "_tokenization_fn")
        delattr(tokenizer, "_output_columns")

        data = tokenizer.serialize()
        restored = Tokenizer.deserialize(data)

        assert isinstance(restored, Tokenizer)
        assert restored.columns == []
        assert callable(restored.tokenization_fn)
        assert restored.output_columns == []

    def test_tokenizer_old_public_field_names(self):
        """Migration from old public field names to new private fields."""
        tokenizer = Tokenizer.__new__(Tokenizer)
        state = {
            "columns": ["text"],
            "tokenization_fn": lambda s: s.split(),
            "output_columns": ["text_tokens"],
        }
        tokenizer.__setstate__(state)

        assert tokenizer.columns == ["text"]
        assert callable(tokenizer.tokenization_fn)
        assert tokenizer.output_columns == ["text_tokens"]


class TestPowerTransformerBackwardsCompatibility:
    """Test backwards compatibility for PowerTransformer preprocessor."""

    def test_power_transformer_deserialize_backward_compat_missing_required(self):
        """Deserialization fails when required field 'power' is missing."""
        transformer = PowerTransformer(columns=["A", "B"], power=2, method="box-cox")
        delattr(transformer, "_power")

        data = transformer.serialize()
        with pytest.raises(ValueError, match="missing required field 'power'"):
            PowerTransformer.deserialize(data)

    def test_power_transformer_old_public_field_names(self):
        """Migration from old public field names to new private fields."""
        transformer = PowerTransformer.__new__(PowerTransformer)
        state = {
            "columns": ["A", "B"],
            "power": 3,
            "method": "box-cox",
            "output_columns": ["A_pow", "B_pow"],
        }
        transformer.__setstate__(state)

        assert transformer.columns == ["A", "B"]
        assert transformer.power == 3
        assert transformer.method == "box-cox"
        assert transformer.output_columns == ["A_pow", "B_pow"]


class TestHashingVectorizerBackwardsCompatibility:
    """Test backwards compatibility for HashingVectorizer preprocessor."""

    def test_hashing_vectorizer_deserialize_backward_compat_missing_required(self):
        """Deserialization fails when required field 'num_features' is missing."""
        vectorizer = HashingVectorizer(columns=["text"], num_features=100)
        delattr(vectorizer, "_num_features")

        data = vectorizer.serialize()
        with pytest.raises(ValueError, match="missing required field 'num_features'"):
            HashingVectorizer.deserialize(data)

    def test_hashing_vectorizer_old_public_field_names(self):
        """Migration from old public field names to new private fields."""
        vectorizer = HashingVectorizer.__new__(HashingVectorizer)
        state = {
            "columns": ["text"],
            "num_features": 200,
            "tokenization_fn": lambda s: s.split(),
            "output_columns": ["text_vec"],
        }
        vectorizer.__setstate__(state)

        assert vectorizer.columns == ["text"]
        assert vectorizer.num_features == 200
        assert callable(vectorizer.tokenization_fn)
        assert vectorizer.output_columns == ["text_vec"]


class TestCountVectorizerBackwardsCompatibility:
    """Test backwards compatibility for CountVectorizer preprocessor."""

    def test_count_vectorizer_deserialize_backward_compat(self):
        """Deserialize with missing private fields applies defaults."""
        vectorizer = CountVectorizer(columns=["text"], max_features=50)
        delattr(vectorizer, "_columns")
        delattr(vectorizer, "_tokenization_fn")
        delattr(vectorizer, "_max_features")
        delattr(vectorizer, "_output_columns")

        data = vectorizer.serialize()
        restored = CountVectorizer.deserialize(data)

        assert isinstance(restored, CountVectorizer)
        assert restored.columns == []
        assert callable(restored.tokenization_fn)
        assert restored.max_features is None
        assert restored.output_columns == []

    def test_count_vectorizer_old_public_field_names(self):
        """Migration from old public field names to new private fields."""
        vectorizer = CountVectorizer.__new__(CountVectorizer)
        state = {
            "columns": ["text"],
            "tokenization_fn": lambda s: s.split(),
            "max_features": 100,
            "output_columns": ["text_count"],
        }
        vectorizer.__setstate__(state)

        assert vectorizer.columns == ["text"]
        assert callable(vectorizer.tokenization_fn)
        assert vectorizer.max_features == 100
        assert vectorizer.output_columns == ["text_count"]


class TestFeatureHasherBackwardsCompatibility:
    """Test backwards compatibility for FeatureHasher preprocessor."""

    def test_feature_hasher_deserialize_backward_compat_missing_required(self):
        """Deserialization fails when required field 'num_features' is missing."""
        hasher = FeatureHasher(
            columns=["A", "B"], num_features=10, output_column="hashed"
        )
        delattr(hasher, "_num_features")

        data = hasher.serialize()
        with pytest.raises(ValueError, match="missing required field 'num_features'"):
            FeatureHasher.deserialize(data)

    def test_feature_hasher_deserialize_backward_compat_missing_output_column(self):
        """Deserialization fails when required field 'output_column' is missing."""
        hasher = FeatureHasher(
            columns=["A", "B"], num_features=10, output_column="hashed"
        )
        delattr(hasher, "_output_column")

        data = hasher.serialize()
        with pytest.raises(ValueError, match="missing required field 'output_column'"):
            FeatureHasher.deserialize(data)

    def test_feature_hasher_old_public_field_names(self):
        """Migration from old public field names to new private fields."""
        hasher = FeatureHasher.__new__(FeatureHasher)
        state = {
            "columns": ["A", "B"],
            "num_features": 20,
            "output_column": "features",
        }
        hasher.__setstate__(state)

        assert hasher.columns == ["A", "B"]
        assert hasher.num_features == 20
        assert hasher.output_column == "features"


class TestChainBackwardsCompatibility:
    """Test backwards compatibility for Chain preprocessor."""

    def test_chain_deserialize_backward_compat_missing_required(self):
        """Deserialization fails when required field 'preprocessors' is missing."""
        scaler = StandardScaler(columns=["A"])
        chain = Chain(scaler)
        delattr(chain, "_preprocessors")

        data = chain.serialize()
        with pytest.raises(ValueError, match="missing required field 'preprocessors'"):
            Chain.deserialize(data)

    def test_chain_old_public_field_names(self):
        """Migration from old public field names to new private fields."""
        scaler1 = StandardScaler(columns=["A"])
        scaler2 = StandardScaler(columns=["B"])
        chain = Chain.__new__(Chain)

        state = {
            "preprocessors": (scaler1, scaler2),
        }
        chain.__setstate__(state)

        assert len(chain.preprocessors) == 2
        assert chain.preprocessors[0] == scaler1
        assert chain.preprocessors[1] == scaler2


class TestFunctionalBackwardsCompatibility:
    """End-to-end tests: deserialized preprocessors work correctly."""

    def test_tokenizer_functional_after_deserialization(self):
        """Tokenizer transforms data correctly after old format deserialization."""
        df = pd.DataFrame({"text": ["hello world", "foo bar"]})
        ds = ray.data.from_pandas(df)

        tokenizer = Tokenizer(columns=["text"])

        # Simulate old format by renaming private fields to public
        state = tokenizer.__dict__.copy()
        state["columns"] = state.pop("_columns")
        state["tokenization_fn"] = state.pop("_tokenization_fn")
        state["output_columns"] = state.pop("_output_columns")

        new_tokenizer = Tokenizer.__new__(Tokenizer)
        new_tokenizer.__setstate__(state)

        result = new_tokenizer.transform(ds).to_pandas()
        assert result["text"][0] == ["hello", "world"]
        assert result["text"][1] == ["foo", "bar"]

    def test_normalizer_functional_after_deserialization(self):
        """Normalizer transforms data correctly after old format deserialization."""
        df = pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0]})
        ds = ray.data.from_pandas(df)

        normalizer = Normalizer(columns=["A", "B"], norm="l2")

        # Simulate old format by renaming private fields to public
        state = normalizer.__dict__.copy()
        state["columns"] = state.pop("_columns")
        state["norm"] = state.pop("_norm")
        state["output_columns"] = state.pop("_output_columns")

        new_normalizer = Normalizer.__new__(Normalizer)
        new_normalizer.__setstate__(state)

        result = new_normalizer.transform(ds).to_pandas()
        assert len(result) == 2
        assert "A" in result.columns
        assert "B" in result.columns

    def test_concatenator_functional_after_deserialization(self):
        """Concatenator transforms data correctly after old format deserialization."""
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        ds = ray.data.from_pandas(df)

        concatenator = Concatenator(columns=["A", "B"], output_column_name="C")

        # Simulate old format by renaming private fields to public
        state = concatenator.__dict__.copy()
        state["columns"] = state.pop("_columns")
        state["output_column_name"] = state.pop("_output_column_name")
        state["dtype"] = state.pop("_dtype")
        state["raise_if_missing"] = state.pop("_raise_if_missing")
        state["flatten"] = state.pop("_flatten")

        new_concatenator = Concatenator.__new__(Concatenator)
        new_concatenator.__setstate__(state)

        result = new_concatenator.transform(ds).to_pandas()
        assert "C" in result.columns
        assert len(result) == 2
        assert np.array_equal(result["C"][0], np.array([1, 3]))
        assert np.array_equal(result["C"][1], np.array([2, 4]))

    def test_feature_hasher_functional_after_deserialization(self):
        """FeatureHasher transforms data correctly after old format deserialization."""
        df = pd.DataFrame({"token_a": [1, 2], "token_b": [3, 4]})
        ds = ray.data.from_pandas(df)

        hasher = FeatureHasher(
            columns=["token_a", "token_b"], num_features=5, output_column="hashed"
        )

        # Simulate old format by renaming private fields to public
        state = hasher.__dict__.copy()
        state["columns"] = state.pop("_columns")
        state["num_features"] = state.pop("_num_features")
        state["output_column"] = state.pop("_output_column")

        new_hasher = FeatureHasher.__new__(FeatureHasher)
        new_hasher.__setstate__(state)

        result = new_hasher.transform(ds).to_pandas()
        assert "hashed" in result.columns
        assert len(result) == 2
        assert len(result["hashed"][0]) == 5

    def test_chain_functional_after_deserialization(self):
        """Chain transforms data correctly after old format deserialization."""
        df = pd.DataFrame({"A": [1.0, 2.0, 3.0]})
        ds = ray.data.from_pandas(df)

        scaler = StandardScaler(columns=["A"])
        normalizer = Normalizer(columns=["A"])
        chain = Chain(scaler, normalizer)
        chain = chain.fit(ds)

        # Simulate old format by renaming private fields to public
        state = chain.__dict__.copy()
        state["preprocessors"] = state.pop("_preprocessors")

        new_chain = Chain.__new__(Chain)
        new_chain.__setstate__(state)

        result = new_chain.transform(ds)
        assert result.count() == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
