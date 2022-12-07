import pandas as pd
import pytest

import ray
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
    col_b = [0.2, 1.4, 2.5, 6.2, 9.7, 2.1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
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

    assert out_df["A"].equals(
        pd.cut(
            in_df["A"],
            bins_A,
            labels=labels_A,
            ordered=ordered_A,
            right=right,
            include_lowest=include_lowest,
        )
    )
    assert out_df["B"].equals(
        pd.cut(
            in_df["B"],
            bins_B,
            labels=labels_B,
            ordered=ordered_B,
            right=right,
            include_lowest=include_lowest,
        )
    )


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
    col_b = [0.2, 1.4, 2.5, 6.2, 9.7, 2.1]
    in_df = pd.DataFrame.from_dict({"A": col_a, "B": col_b})
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

    assert out_df["A"].equals(
        pd.cut(
            in_df["A"],
            bins_A,
            labels=labels_A,
            ordered=ordered_A,
            right=right,
            include_lowest=include_lowest,
        )
    )
    assert out_df["B"].equals(
        pd.cut(
            in_df["B"],
            bins_B,
            labels=labels_B,
            ordered=ordered_B,
            right=right,
            include_lowest=include_lowest,
        )
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-sv", __file__]))
