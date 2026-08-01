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
    Categorizer,
    Chain,
    Concatenator,
    CountVectorizer,
    CustomKBinsDiscretizer,
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
    TorchVisionPreprocessor,
    UniformKBinsDiscretizer,
)

# =============================================================================
# Field Migration Tests
# =============================================================================


@pytest.mark.parametrize(
    "preprocessor_class,old_state,expected_attrs",
    [
        (
            Concatenator,
            {
                "columns": ["A", "B"],
                "output_column_name": "concat",
                "dtype": np.float32,
                "raise_if_missing": True,
                "flatten": True,
            },
            {
                "columns": ["A", "B"],
                "output_column_name": "concat",
                "dtype": np.float32,
                "raise_if_missing": True,
                "flatten": True,
            },
        ),
        (
            Normalizer,
            {
                "columns": ["A", "B"],
                "norm": "l1",
                "output_columns": ["A_norm", "B_norm"],
            },
            {
                "columns": ["A", "B"],
                "norm": "l1",
                "output_columns": ["A_norm", "B_norm"],
            },
        ),
        (
            Tokenizer,
            {
                "columns": ["text"],
                "tokenization_fn": lambda s: s.split(),
                "output_columns": ["text_tokens"],
            },
            {
                "columns": ["text"],
                "tokenization_fn": "callable",  # Special marker
                "output_columns": ["text_tokens"],
            },
        ),
        (
            PowerTransformer,
            {
                "columns": ["A", "B"],
                "power": 3,
                "method": "box-cox",
                "output_columns": ["A_pow", "B_pow"],
            },
            {
                "columns": ["A", "B"],
                "power": 3,
                "method": "box-cox",
                "output_columns": ["A_pow", "B_pow"],
            },
        ),
        (
            HashingVectorizer,
            {
                "columns": ["text"],
                "num_features": 200,
                "tokenization_fn": lambda s: s.split(),
                "output_columns": ["text_vec"],
            },
            {
                "columns": ["text"],
                "num_features": 200,
                "tokenization_fn": "callable",
                "output_columns": ["text_vec"],
            },
        ),
        (
            CountVectorizer,
            {
                "columns": ["text"],
                "tokenization_fn": lambda s: s.split(),
                "max_features": 100,
                "output_columns": ["text_count"],
            },
            {
                "columns": ["text"],
                "tokenization_fn": "callable",
                "max_features": 100,
                "output_columns": ["text_count"],
            },
        ),
        (
            FeatureHasher,
            {"columns": ["A", "B"], "num_features": 20, "output_column": "features"},
            {"columns": ["A", "B"], "num_features": 20, "output_column": "features"},
        ),
        (
            OrdinalEncoder,
            {
                "columns": ["color"],
                "output_columns": ["color_encoded"],
                "encode_lists": False,
            },
            {
                "columns": ["color"],
                "output_columns": ["color_encoded"],
                "encode_lists": False,
            },
        ),
        (
            OneHotEncoder,
            {
                "columns": ["color"],
                "output_columns": ["color_encoded"],
                "max_categories": {"color": 5},
            },
            {
                "columns": ["color"],
                "output_columns": ["color_encoded"],
                "max_categories": {"color": 5},
            },
        ),
        (
            MultiHotEncoder,
            {
                "columns": ["tags"],
                "output_columns": ["tags_encoded"],
                "max_categories": {},
            },
            {"columns": ["tags"], "output_columns": ["tags_encoded"]},
        ),
        (
            LabelEncoder,
            {"label_column": "label", "output_column": "label_id"},
            {"label_column": "label", "output_column": "label_id"},
        ),
        (
            Categorizer,
            {"columns": ["sex"], "output_columns": ["sex_cat"], "dtypes": {}},
            {"columns": ["sex"], "output_columns": ["sex_cat"]},
        ),
        (
            StandardScaler,
            {"columns": ["A", "B"], "output_columns": ["A_scaled", "B_scaled"]},
            {"columns": ["A", "B"], "output_columns": ["A_scaled", "B_scaled"]},
        ),
        (
            MinMaxScaler,
            {"columns": ["A"], "output_columns": ["A_scaled"]},
            {"columns": ["A"], "output_columns": ["A_scaled"]},
        ),
        (
            MaxAbsScaler,
            {"columns": ["A"], "output_columns": ["A_scaled"]},
            {"columns": ["A"], "output_columns": ["A_scaled"]},
        ),
        (
            RobustScaler,
            {
                "columns": ["A"],
                "output_columns": ["A_scaled"],
                "quantile_range": (0.1, 0.9),
                "quantile_precision": 1000,
            },
            {
                "columns": ["A"],
                "output_columns": ["A_scaled"],
                "quantile_range": (0.1, 0.9),
                "quantile_precision": 1000,
            },
        ),
        (
            SimpleImputer,
            {
                "columns": ["A", "B"],
                "output_columns": ["A_imputed", "B_imputed"],
                "strategy": "median",
                "fill_value": 99.0,
            },
            {
                "columns": ["A", "B"],
                "output_columns": ["A_imputed", "B_imputed"],
                "strategy": "median",
                "fill_value": 99.0,
            },
        ),
        (
            CustomKBinsDiscretizer,
            {
                "columns": ["A", "B"],
                "bins": [0, 1, 2, 3],
                "right": False,
                "include_lowest": True,
                "duplicates": "drop",
                "dtypes": None,
                "output_columns": ["A_binned", "B_binned"],
            },
            {
                "columns": ["A", "B"],
                "bins": [0, 1, 2, 3],
                "right": False,
                "include_lowest": True,
                "duplicates": "drop",
                "dtypes": None,
                "output_columns": ["A_binned", "B_binned"],
            },
        ),
        (
            UniformKBinsDiscretizer,
            {
                "columns": ["A", "B"],
                "bins": 4,
                "right": False,
                "include_lowest": True,
                "duplicates": "drop",
                "dtypes": None,
                "output_columns": ["A_binned", "B_binned"],
            },
            {
                "columns": ["A", "B"],
                "bins": 4,
                "right": False,
                "include_lowest": True,
                "duplicates": "drop",
                "dtypes": None,
                "output_columns": ["A_binned", "B_binned"],
            },
        ),
    ],
    ids=lambda x: x.__name__ if hasattr(x, "__name__") else str(x)[:20],
)
def test_field_migration_from_old_public_names(
    preprocessor_class, old_state, expected_attrs
):
    """Verify old public field names are migrated to new private fields."""
    preprocessor = preprocessor_class.__new__(preprocessor_class)
    preprocessor.__setstate__(old_state)

    for attr_name, expected_value in expected_attrs.items():
        actual_value = getattr(preprocessor, attr_name)
        if expected_value == "callable":
            assert callable(actual_value), f"{attr_name} should be callable"
        else:
            assert actual_value == expected_value, f"Mismatch in {attr_name}"


@pytest.mark.parametrize(
    "preprocessor_class,minimal_state,expected_defaults",
    [
        # Callable default: tokenization_fn must be stored as the function itself,
        # not called. This would have failed with the old callable() check.
        (
            Tokenizer,
            {"columns": ["text"]},
            {"tokenization_fn": "callable", "output_columns": ["text"]},
        ),
        (
            HashingVectorizer,
            {"columns": ["text"], "num_features": 100},
            {"tokenization_fn": "callable", "output_columns": ["text"]},
        ),
        (
            CountVectorizer,
            {"columns": ["text"]},
            {"tokenization_fn": "callable", "output_columns": ["text"]},
        ),
        # _Computed default: output_columns derives from _columns.
        (
            StandardScaler,
            {"columns": ["A", "B"]},
            {"output_columns": ["A", "B"]},
        ),
        # _Computed default deriving from a different source field.
        (
            LabelEncoder,
            {"label_column": "label"},
            {"output_column": "label"},
        ),
        # Plain value default alongside a _Computed default.
        (
            Normalizer,
            {"columns": ["A", "B"]},
            {"norm": "l2", "output_columns": ["A", "B"]},
        ),
    ],
    ids=[
        "Tokenizer",
        "HashingVectorizer",
        "CountVectorizer",
        "StandardScaler",
        "LabelEncoder",
        "Normalizer",
    ],
)
def test_missing_optional_fields_use_defaults(
    preprocessor_class, minimal_state, expected_defaults
):
    """
    Verify that absent optional fields are filled with their correct defaults.

    This exercises the default-fallback branch of migrate_private_fields. The minimal_state
    deliberately omits optional fields to force the default path.
    """
    preprocessor = preprocessor_class.__new__(preprocessor_class)
    preprocessor.__setstate__(minimal_state)

    for attr_name, expected_value in expected_defaults.items():
        actual_value = getattr(preprocessor, attr_name)
        if expected_value == "callable":
            assert callable(
                actual_value
            ), f"{attr_name} should be a stored callable, not the result of calling it"
        else:
            assert (
                actual_value == expected_value
            ), f"Mismatch in {attr_name}: {actual_value!r} != {expected_value!r}"


def test_torchvision_preprocessor_field_migration():
    try:
        from torchvision import transforms
    except ImportError:
        pytest.skip("torchvision not installed")

    transform = transforms.Lambda(lambda x: x)
    preprocessor = TorchVisionPreprocessor.__new__(TorchVisionPreprocessor)
    state = {
        "columns": ["image"],
        "output_columns": ["image_out"],
        "torchvision_transform": transform,
        "batched": True,
    }
    preprocessor.__setstate__(state)

    assert preprocessor.columns == ["image"]
    assert preprocessor.output_columns == ["image_out"]
    assert preprocessor.torchvision_transform == transform
    assert preprocessor.batched is True


def test_chain_field_migration():
    scaler1 = StandardScaler(columns=["A"])
    scaler2 = StandardScaler(columns=["B"])
    chain = Chain.__new__(Chain)

    state = {"preprocessors": (scaler1, scaler2)}
    chain.__setstate__(state)

    assert len(chain.preprocessors) == 2
    assert chain.preprocessors[0] == scaler1
    assert chain.preprocessors[1] == scaler2


# =============================================================================
# Functional Test Helpers
# =============================================================================


def _simulate_old_format_deserialization(preprocessor, field_mapping):
    """Simulate deserialization from old format by renaming private->public fields."""
    state = preprocessor.__dict__.copy()
    for public_name, private_name in field_mapping.items():
        if private_name in state:
            state[public_name] = state.pop(private_name)

    new_preprocessor = preprocessor.__class__.__new__(preprocessor.__class__)
    new_preprocessor.__setstate__(state)
    return new_preprocessor


def _test_functional_backwards_compat(preprocessor, test_ds, field_mapping):
    """Generic functional test: verify deserialized preprocessor produces same output."""
    expected_result = preprocessor.transform(test_ds).to_pandas()
    new_preprocessor = _simulate_old_format_deserialization(preprocessor, field_mapping)
    result = new_preprocessor.transform(test_ds).to_pandas()
    pd.testing.assert_frame_equal(result, expected_result)


# =============================================================================
# Functional Tests - Simple Preprocessors (No Fitting Required)
# =============================================================================


@pytest.mark.parametrize(
    "setup_func,field_mapping",
    [
        (
            lambda: (
                Concatenator(columns=["A", "B"], output_column_name="C"),
                pd.DataFrame({"A": [1, 2], "B": [3, 4]}),
                {
                    "columns": "_columns",
                    "output_column_name": "_output_column_name",
                    "dtype": "_dtype",
                    "raise_if_missing": "_raise_if_missing",
                    "flatten": "_flatten",
                },
            ),
            None,
        ),
        (
            lambda: (
                Normalizer(columns=["A", "B"], norm="l2"),
                pd.DataFrame({"A": [1.0, 2.0], "B": [3.0, 4.0]}),
                {
                    "columns": "_columns",
                    "norm": "_norm",
                    "output_columns": "_output_columns",
                },
            ),
            None,
        ),
        (
            lambda: (
                Tokenizer(columns=["text"]),
                pd.DataFrame({"text": ["hello world", "foo bar"]}),
                {
                    "columns": "_columns",
                    "tokenization_fn": "_tokenization_fn",
                    "output_columns": "_output_columns",
                },
            ),
            None,
        ),
        (
            lambda: (
                PowerTransformer(columns=["A", "B"], power=2),
                pd.DataFrame({"A": [1.0, 2.0, 3.0], "B": [4.0, 5.0, 6.0]}),
                {
                    "columns": "_columns",
                    "power": "_power",
                    "method": "_method",
                    "output_columns": "_output_columns",
                },
            ),
            None,
        ),
        (
            lambda: (
                HashingVectorizer(columns=["text"], num_features=10),
                pd.DataFrame({"text": ["hello world", "foo bar"]}),
                {
                    "columns": "_columns",
                    "num_features": "_num_features",
                    "tokenization_fn": "_tokenization_fn",
                    "output_columns": "_output_columns",
                },
            ),
            None,
        ),
        (
            lambda: (
                FeatureHasher(
                    columns=["token_a", "token_b"],
                    num_features=5,
                    output_column="hashed",
                ),
                pd.DataFrame({"token_a": [1, 2], "token_b": [3, 4]}),
                {
                    "columns": "_columns",
                    "num_features": "_num_features",
                    "output_column": "_output_column",
                },
            ),
            None,
        ),
        (
            lambda: (
                CustomKBinsDiscretizer(
                    columns=["A", "B"],
                    bins=[0, 1, 2, 3, 4],
                    output_columns=["A_binned", "B_binned"],
                ),
                pd.DataFrame({"A": [0.5, 1.5, 2.5, 3.5], "B": [0.2, 1.2, 2.2, 3.2]}),
                {
                    "columns": "_columns",
                    "bins": "_bins",
                    "right": "_right",
                    "include_lowest": "_include_lowest",
                    "duplicates": "_duplicates",
                    "dtypes": "_dtypes",
                    "output_columns": "_output_columns",
                },
            ),
            None,
        ),
    ],
    ids=[
        "Concatenator",
        "Normalizer",
        "Tokenizer",
        "PowerTransformer",
        "HashingVectorizer",
        "FeatureHasher",
        "CustomKBinsDiscretizer",
    ],
)
def test_simple_functional_backwards_compat(setup_func, field_mapping):
    """Verify preprocessors that don't need fitting work after deserialization."""
    preprocessor, test_data, field_mapping = setup_func()
    test_ds = ray.data.from_pandas(test_data)
    _test_functional_backwards_compat(preprocessor, test_ds, field_mapping)


# =============================================================================
# Functional Tests - Stateful Preprocessors (Require Fitting)
# =============================================================================


@pytest.mark.parametrize(
    "setup_func",
    [
        lambda: (
            OrdinalEncoder(columns=["color"]),
            pd.DataFrame({"color": ["red", "green", "blue", "red", "green"]}),
            {
                "columns": "_columns",
                "output_columns": "_output_columns",
                "encode_lists": "_encode_lists",
            },
        ),
        lambda: (
            OneHotEncoder(columns=["color"]),
            pd.DataFrame({"color": ["red", "green", "blue", "red", "green", "blue"]}),
            {
                "columns": "_columns",
                "output_columns": "_output_columns",
                "max_categories": "_max_categories",
            },
        ),
        lambda: (
            LabelEncoder(label_column="label"),
            pd.DataFrame(
                {
                    "feature": [1.0, 2.0, 3.0, 4.0],
                    "label": ["cat", "dog", "cat", "bird"],
                }
            ),
            {"label_column": "_label_column", "output_column": "_output_column"},
        ),
        lambda: (
            StandardScaler(columns=["A", "B"]),
            pd.DataFrame(
                {"A": [1.0, 2.0, 3.0, 4.0, 5.0], "B": [10.0, 20.0, 30.0, 40.0, 50.0]}
            ),
            {"columns": "_columns", "output_columns": "_output_columns"},
        ),
        lambda: (
            MinMaxScaler(columns=["A", "B"]),
            pd.DataFrame(
                {"A": [1.0, 2.0, 3.0, 4.0, 5.0], "B": [10.0, 20.0, 30.0, 40.0, 50.0]}
            ),
            {"columns": "_columns", "output_columns": "_output_columns"},
        ),
        lambda: (
            RobustScaler(columns=["A"]),
            pd.DataFrame({"A": [1.0, 2.0, 3.0, 4.0, 5.0, 100.0]}),
            {
                "columns": "_columns",
                "output_columns": "_output_columns",
                "quantile_range": "_quantile_range",
                "quantile_precision": "_quantile_precision",
            },
        ),
        lambda: (
            SimpleImputer(columns=["A", "B"], strategy="mean"),
            pd.DataFrame(
                {"A": [1.0, 2.0, None, 4.0, 5.0], "B": [10.0, None, 30.0, 40.0, 50.0]}
            ),
            {
                "columns": "_columns",
                "output_columns": "_output_columns",
                "strategy": "_strategy",
                "fill_value": "_fill_value",
            },
        ),
        lambda: (
            CountVectorizer(columns=["text"]),
            pd.DataFrame({"text": ["hello world", "foo bar", "hello foo"]}),
            {
                "columns": "_columns",
                "tokenization_fn": "_tokenization_fn",
                "max_features": "_max_features",
                "output_columns": "_output_columns",
            },
        ),
        lambda: (
            UniformKBinsDiscretizer(
                columns=["A", "B"], bins=3, output_columns=["A_binned", "B_binned"]
            ),
            pd.DataFrame(
                {"A": [1.0, 2.0, 3.0, 4.0, 5.0], "B": [10.0, 20.0, 30.0, 40.0, 50.0]}
            ),
            {
                "columns": "_columns",
                "bins": "_bins",
                "right": "_right",
                "include_lowest": "_include_lowest",
                "duplicates": "_duplicates",
                "dtypes": "_dtypes",
                "output_columns": "_output_columns",
            },
        ),
        lambda: (
            MultiHotEncoder(columns=["genre"]),
            pd.DataFrame(
                {
                    "genre": [
                        ["comedy", "action"],
                        ["drama", "action"],
                        ["comedy", "drama"],
                    ]
                }
            ),
            {
                "columns": "_columns",
                "output_columns": "_output_columns",
                "max_categories": "_max_categories",
            },
        ),
        lambda: (
            MaxAbsScaler(columns=["A", "B"]),
            pd.DataFrame({"A": [-6.0, 3.0, -3.0], "B": [2.0, -4.0, 1.0]}),
            {"columns": "_columns", "output_columns": "_output_columns"},
        ),
        lambda: (
            Categorizer(columns=["color"]),
            pd.DataFrame({"color": ["red", "green", "blue", "red", "green"]}),
            {
                "columns": "_columns",
                "output_columns": "_output_columns",
                "dtypes": "_dtypes",
            },
        ),
    ],
    ids=[
        "OrdinalEncoder",
        "OneHotEncoder",
        "LabelEncoder",
        "StandardScaler",
        "MinMaxScaler",
        "RobustScaler",
        "SimpleImputer",
        "CountVectorizer",
        "UniformKBinsDiscretizer",
        "MultiHotEncoder",
        "MaxAbsScaler",
        "Categorizer",
    ],
)
def test_stateful_functional_backwards_compat(setup_func):
    """Verify fitted preprocessors work after deserialization."""
    preprocessor, test_data, field_mapping = setup_func()
    test_ds = ray.data.from_pandas(test_data)
    preprocessor = preprocessor.fit(test_ds)
    _test_functional_backwards_compat(preprocessor, test_ds, field_mapping)


def test_chain_functional_backwards_compat():
    df = pd.DataFrame({"A": [1.0, 2.0, 3.0]})
    ds = ray.data.from_pandas(df)

    scaler = StandardScaler(columns=["A"])
    normalizer = Normalizer(columns=["A"])
    chain = Chain(scaler, normalizer)
    chain = chain.fit(ds)

    expected_result = chain.transform(ds).to_pandas()

    state = chain.__dict__.copy()
    state["preprocessors"] = state.pop("_preprocessors")

    new_chain = Chain.__new__(Chain)
    new_chain.__setstate__(state)

    result = new_chain.transform(ds).to_pandas()
    pd.testing.assert_frame_equal(result, expected_result)


def test_torchvision_functional_backwards_compat():
    try:
        import torch
        from torchvision import transforms
    except ImportError:
        pytest.skip("torchvision not installed")

    transform = transforms.Lambda(lambda x: torch.as_tensor(x, dtype=torch.float32))
    df = pd.DataFrame(
        {
            "image": [
                np.array([[1, 2], [3, 4]], dtype=np.uint8),
                np.array([[5, 6], [7, 8]], dtype=np.uint8),
            ]
        }
    )
    ds = ray.data.from_pandas(df)

    preprocessor = TorchVisionPreprocessor(
        columns=["image"], transform=transform, batched=False
    )
    expected_result = preprocessor.transform(ds).to_pandas()

    state = preprocessor.__dict__.copy()
    state["columns"] = state.pop("_columns")
    state["output_columns"] = state.pop("_output_columns")
    state["torchvision_transform"] = state.pop("_torchvision_transform")
    state["batched"] = state.pop("_batched")

    new_preprocessor = TorchVisionPreprocessor.__new__(TorchVisionPreprocessor)
    new_preprocessor.__setstate__(state)

    result = new_preprocessor.transform(ds).to_pandas()
    assert len(result) == len(expected_result)
    assert "image" in result.columns


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
