import pandas as pd
import pytest

import ray
from ray.data._internal.util import rows_same
from ray.data.preprocessors import CustomKBinsDiscretizer, UniformKBinsDiscretizer


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
    col_b = col_a.copy()
    col_c = col_a.copy()
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
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

    # Create expected dataframe with transformed columns
    expected_df = in_df.copy()
    expected_df["A"] = pd.cut(
        in_df["A"],
        bins_A,
        labels=labels_A,
        ordered=ordered_A,
        right=right,
        include_lowest=include_lowest,
    )
    expected_df["B"] = pd.cut(
        in_df["B"],
        bins_B,
        labels=labels_B,
        ordered=ordered_B,
        right=right,
        include_lowest=include_lowest,
    )

    # Use rows_same to compare regardless of row ordering
    assert rows_same(out_df, expected_df)

    # append mode
    expected_message = "The length of columns and output_columns must match."
    with pytest.raises(ValueError, match=expected_message):
        UniformKBinsDiscretizer(["A", "B"], bins=bins, output_columns=["A_discretized"])

    discretizer = UniformKBinsDiscretizer(
        ["A", "B"],
        bins=bins,
        dtypes=dtypes,
        right=right,
        include_lowest=include_lowest,
        output_columns=["A_discretized", "B_discretized"],
    )

    transformed = discretizer.fit_transform(ds)
    out_df = transformed.to_pandas()

    # Create expected dataframe with appended columns
    expected_df = in_df.copy()
    expected_df["A_discretized"] = pd.cut(
        in_df["A"],
        bins_A,
        labels=labels_A,
        ordered=ordered_A,
        right=right,
        include_lowest=include_lowest,
    )
    expected_df["B_discretized"] = pd.cut(
        in_df["B"],
        bins_B,
        labels=labels_B,
        ordered=ordered_B,
        right=right,
        include_lowest=include_lowest,
    )

    # Use rows_same to compare regardless of row ordering
    assert rows_same(out_df, expected_df)


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
    col_b = col_a.copy()
    col_c = col_a.copy()
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b, "C": col_c})
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

    # Create expected dataframe with transformed columns
    expected_df = in_df.copy()
    expected_df["A"] = pd.cut(
        in_df["A"],
        bins_A,
        labels=labels_A,
        ordered=ordered_A,
        right=right,
        include_lowest=include_lowest,
    )
    expected_df["B"] = pd.cut(
        in_df["B"],
        bins_B,
        labels=labels_B,
        ordered=ordered_B,
        right=right,
        include_lowest=include_lowest,
    )

    # Use rows_same to compare regardless of row ordering
    assert rows_same(out_df, expected_df)

    # append mode
    expected_message = "The length of columns and output_columns must match."
    with pytest.raises(ValueError, match=expected_message):
        CustomKBinsDiscretizer(["A", "B"], bins=bins, output_columns=["A_discretized"])

    discretizer = CustomKBinsDiscretizer(
        ["A", "B"],
        bins=bins,
        dtypes=dtypes,
        right=right,
        include_lowest=include_lowest,
        output_columns=["A_discretized", "B_discretized"],
    )

    transformed = discretizer.fit_transform(ds)
    out_df = transformed.to_pandas()

    # Create expected dataframe with appended columns
    expected_df = in_df.copy()
    expected_df["A_discretized"] = pd.cut(
        in_df["A"],
        bins_A,
        labels=labels_A,
        ordered=ordered_A,
        right=right,
        include_lowest=include_lowest,
    )
    expected_df["B_discretized"] = pd.cut(
        in_df["B"],
        bins_B,
        labels=labels_B,
        ordered=ordered_B,
        right=right,
        include_lowest=include_lowest,
    )

    # Use rows_same to compare regardless of row ordering
    assert rows_same(out_df, expected_df)


def test_custom_kbins_discretizer_serialization():
    """Test CustomKBinsDiscretizer serialization and deserialization functionality."""
    from ray.data.preprocessor import SerializablePreprocessorBase

    # Create discretizer
    discretizer = CustomKBinsDiscretizer(
        columns=["A"], bins={"A": [0, 1, 2, 3]}, right=True
    )

    # Serialize using CloudPickle
    serialized = discretizer.serialize()

    # Verify it's binary CloudPickle format
    assert isinstance(serialized, bytes)
    assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

    # Deserialize
    deserialized = CustomKBinsDiscretizer.deserialize(serialized)

    # Verify type and field values
    assert isinstance(deserialized, CustomKBinsDiscretizer)
    assert deserialized.columns == ["A"]
    assert deserialized.bins == {"A": [0, 1, 2, 3]}
    assert deserialized.right is True

    # Verify it works correctly
    df = pd.DataFrame({"A": [0.5, 1.5, 2.5]})
    result = deserialized.transform_batch(df)

    # Verify discretization was applied correctly
    assert "A" in result.columns
    assert len(result) == 3


def test_uniform_kbins_discretizer_serialization():
    """Test UniformKBinsDiscretizer serialization and deserialization functionality."""
    import ray
    from ray.data.preprocessor import SerializablePreprocessorBase

    # Create and fit discretizer
    discretizer = UniformKBinsDiscretizer(columns=["A"], bins=3)
    df = pd.DataFrame({"A": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]})
    ds = ray.data.from_pandas(df)
    fitted_discretizer = discretizer.fit(ds)

    # Serialize using CloudPickle
    serialized = fitted_discretizer.serialize()

    # Verify it's binary CloudPickle format
    assert isinstance(serialized, bytes)
    assert serialized.startswith(SerializablePreprocessorBase.MAGIC_CLOUDPICKLE)

    # Deserialize
    deserialized = UniformKBinsDiscretizer.deserialize(serialized)

    # Verify type and field values
    assert isinstance(deserialized, UniformKBinsDiscretizer)
    assert deserialized._fitted
    assert deserialized.columns == ["A"]
    assert deserialized.bins == 3

    # Verify stats are preserved: bin edges for 3 bins = 4 edge values
    assert "A" in deserialized.stats_
    assert len(deserialized.stats_["A"]) == 4

    # Verify it works correctly
    test_df = pd.DataFrame({"A": [1.5, 3.5, 5.5]})
    result = deserialized.transform_batch(test_df)

    # Verify discretization was applied correctly
    assert "A" in result.columns
    assert len(result) == 3


@pytest.mark.parametrize("bins", (0, -3, {"A": 0}))
def test_uniform_kbins_discretizer_rejects_non_positive_bins(bins):
    # A non-positive bin count has no meaning; without this check `bins=0`
    # produced an all-null column instead of an error.
    with pytest.raises(ValueError, match="bins must be a positive integer"):
        UniformKBinsDiscretizer(["A"], bins=bins)


@pytest.mark.parametrize("bins", ([1, 2], "abc", 3.5))
def test_uniform_kbins_discretizer_defers_non_integer_bins_to_fit(bins):
    # Non-integer `bins` keeps reaching the type check in `_fit`, which names
    # the parameter, rather than tripping the positivity comparison.
    discretizer = UniformKBinsDiscretizer(["A"], bins=bins)
    ds = ray.data.from_pandas(pd.DataFrame({"A": [1.0, 2.0]}))
    with pytest.raises(TypeError, match="`bins` must be an integer"):
        discretizer.fit(ds)


def test_kbins_discretizer_transform_pandas_preserves_unselected_columns():
    df = pd.DataFrame(
        {
            "A": [0.5, 1.5, 2.5],
            "B": [10.0, 20.0, 30.0],
            "passthrough_str": ["x", "y", "z"],
            "passthrough_obj": [{"k": 1}, {"k": 2}, {"k": 3}],
        }
    )
    discretizer = CustomKBinsDiscretizer(
        columns=["A", "B"],
        bins={"A": [0.0, 1.0, 2.0, 3.0], "B": [0.0, 15.0, 35.0]},
        output_columns=["A_binned", "B_binned"],
    )
    out = discretizer._transform_pandas(df)
    assert "A_binned" not in df.columns
    assert "B_binned" not in df.columns
    assert list(out["A_binned"]) == [0, 1, 2]
    assert list(out["B_binned"]) == [0, 1, 1]
    assert list(out["passthrough_str"]) == ["x", "y", "z"]
    assert list(out["passthrough_obj"]) == [{"k": 1}, {"k": 2}, {"k": 3}]


def test_kbins_discretizer_transform_pandas_overlapping_output_columns():
    # Overlapping output_columns and columns (output_columns[0] == columns[1])
    # should still bin the original input column B rather than the overwritten output.
    df = pd.DataFrame(
        {
            "A": [0.5, 1.5, 2.5],
            "B": [10.0, 20.0, 30.0],
        }
    )
    overlapping = CustomKBinsDiscretizer(
        columns=["A", "B"],
        bins={"A": [0.0, 1.0, 2.0, 3.0], "B": [0.0, 15.0, 35.0]},
        output_columns=["B", "C"],
    )
    overlap_out = overlapping._transform_pandas(df)
    assert list(df["B"]) == [10.0, 20.0, 30.0]
    assert "C" not in df.columns
    assert list(overlap_out["B"]) == [0, 1, 2]
    assert list(overlap_out["C"]) == [0, 1, 1]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
