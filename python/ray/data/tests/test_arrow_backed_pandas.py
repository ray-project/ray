"""Coverage for Arrow-backed pandas conversion (Ray 2.56+).

Since Ray 2.56, ``BlockAccessor.to_pandas`` maps Arrow types to pandas
Arrow-backed dtypes (``pd.ArrowDtype``) instead of copying into NumPy arrays. That
avoids a copy, but changes two things code downstream relied on:

* a missing value is ``pd.NA`` rather than ``np.nan``, and ``pd.NA`` propagates
  through comparisons as a third "unknown" value instead of ``False``;
* pandas' Arrow backend does not implement every operator its NumPy backend does
  (``%``, ``divmod``; see https://github.com/pandas-dev/pandas/issues/58723), and
  ``to_numpy()`` degrades to ``dtype=object``.

Two groups of tests live here. The first covers preprocessors, which handle
``pd.NA`` explicitly because ``transform_batch`` hands them a user-supplied frame
without conversion. The second covers the batch-format contract: pandas batches
handed to a transform function are converted back to NumPy-backed pandas, because
that code cannot be audited for either problem above. ``Dataset.to_pandas`` is
the one caller that keeps Arrow-backed dtypes.

Every test builds its input with ``from_items``/``range`` rather than
``from_pandas``. This matters: ``from_pandas`` blocks never go through the
Arrow-to-pandas conversion, so the same test written that way would pass while
covering nothing.
"""

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.tensor_extensions.arrow import (
    get_arrow_extension_fixed_shape_tensor_types,
)
from ray.data._internal.util import is_null, to_numpy_backed
from ray.data.block import BlockAccessor
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors import (
    Categorizer,
    Concatenator,
    FeatureHasher,
    PowerTransformer,
    SimpleImputer,
)
from ray.data.tests.conftest import *  # noqa: F401, F403


def _assert_block_is_arrow_backed(ds, *columns):
    """Assert ``ds``'s single block is Arrow and ``columns`` are Arrow-backed with nulls.

    This guards the premise of every preprocessor test below: without an
    Arrow-backed column holding ``pd.NA``, the test would keep passing while no
    longer exercising the bug it was written for.

    The check goes through the block rather than ``ds.to_pandas()``, because
    ``to_pandas()`` is user-facing and returns NumPy-backed columns. What matters
    here is what the preprocessor receives.
    """
    (block,) = ray.get(ds.to_arrow_refs())
    df = BlockAccessor.for_block(block).to_pandas()
    for column in columns:
        assert isinstance(df.dtypes[column], pd.ArrowDtype), df.dtypes[column]
        assert df[column].isna().any()


# ---------------------------------------------------------------------------
# `is_null`
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(None, id="none"),
        pytest.param(np.nan, id="np_nan"),
        pytest.param(float("nan"), id="float_nan"),
        # `pd.NA` and `pd.NaT` are the sentinels used by Arrow-backed pandas
        # columns. Unlike `np.nan` they are not floats, so they are not caught by
        # an `isinstance(value, float)` check.
        pytest.param(pd.NA, id="pd_na"),
        pytest.param(pd.NaT, id="pd_nat"),
    ],
)
def test_is_null_recognizes_all_missing_value_sentinels(value):
    assert is_null(value)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(0, id="zero"),
        pytest.param(0.0, id="zero_float"),
        pytest.param("", id="empty_string"),
        pytest.param(False, id="false"),
        pytest.param([], id="empty_list"),
        # `is_null` is called with list values by
        # `ray.data.preprocessors.encoder.unique_post_fn`, so it must return a
        # plain bool rather than an elementwise result.
        pytest.param([1, None], id="list_containing_null"),
    ],
)
def test_is_null_does_not_treat_non_null_values_as_null(value):
    assert not is_null(value)


# ---------------------------------------------------------------------------
# `to_numpy_backed`
# ---------------------------------------------------------------------------


# Each case is an Arrow array plus the NumPy-backed dtype it must convert to.
# The expected dtypes are the representation pandas produces when Arrow-backing
# is disabled, so this pins `to_numpy_backed` to the pre-2.56 behaviour.
@pytest.mark.parametrize(
    "arrow_array,expected_dtype",
    [
        pytest.param(pa.array([1.0, None, 5.0]), np.float64, id="float_with_null"),
        pytest.param(pa.array([1.0, 2.0, 5.0]), np.float64, id="float_no_null"),
        # An integer column containing nulls has to widen, because NumPy integer
        # arrays cannot hold a missing value.
        pytest.param(pa.array([1, None, 5]), np.float64, id="int_with_null"),
        pytest.param(pa.array([1, 2, 5]), np.int64, id="int_no_null"),
        pytest.param(pa.array(["a", None, "c"]), object, id="string_with_null"),
        pytest.param(pa.array([True, None, False]), object, id="bool_with_null"),
    ],
)
def test_to_numpy_backed_matches_non_arrow_conversion(arrow_array, expected_dtype):
    table = pa.table({"x": arrow_array})
    arrow_backed = table.to_pandas(types_mapper=pd.ArrowDtype)["x"]
    # Premise: the input must really be Arrow-backed, otherwise this test would
    # pass without exercising the conversion at all.
    assert isinstance(arrow_backed.dtype, pd.ArrowDtype)

    converted = to_numpy_backed(arrow_backed)

    assert converted.dtype == expected_dtype
    # The conversion pandas performs with Arrow-backing disabled is the
    # reference: matching it means missing values are represented the way
    # NumPy-style preprocessor code expects.
    pd.testing.assert_series_equal(converted, table.to_pandas()["x"])


def test_to_numpy_backed_replaces_pd_na_with_nan():
    arrow_backed = pa.table({"x": pa.array([1.0, None, 5.0])}).to_pandas(
        types_mapper=pd.ArrowDtype
    )["x"]
    assert arrow_backed[1] is pd.NA

    converted = to_numpy_backed(arrow_backed)

    assert np.isnan(converted[1])
    # `pd.NA` yields a three-valued mask that cannot index a NumPy array; the
    # converted column must produce a plain two-valued one.
    assert (converted >= 0).dtype == np.bool_
    assert (converted >= 0).tolist() == [True, False, True]


def test_to_numpy_backed_preserves_index_and_name():
    arrow_backed = pd.Series(
        pd.array([1.0, None], dtype=pd.ArrowDtype(pa.float64())),
        index=[10, 20],
        name="feature",
    )

    converted = to_numpy_backed(arrow_backed)

    assert converted.index.tolist() == [10, 20]
    assert converted.name == "feature"


def test_to_numpy_backed_is_a_no_op_for_numpy_backed_input():
    series = pd.Series([1.0, np.nan, 5.0])
    assert to_numpy_backed(series) is series

    # The frame must come back as the *same object*, not an equal copy.
    # `_format_batch` calls this on every pandas batch, and rebuilding a
    # `DataFrame` from its columns duplicates the whole batch.
    df = pd.DataFrame({"a": [1.0, 2.0], "b": ["x", "y"]})
    assert to_numpy_backed(df) is df


def test_to_numpy_backed_converts_a_partially_arrow_backed_frame():
    """The all-NumPy early return must not skip frames with a mix of dtypes."""
    df = pd.DataFrame(
        {
            "arrow": pd.array([1.0, None], dtype=pd.ArrowDtype(pa.float64())),
            "numpy": np.array([1.5, 2.5]),
        }
    )

    converted = to_numpy_backed(df)

    assert converted["arrow"].dtype == np.float64
    assert np.isnan(converted["arrow"][1])
    # The untouched column keeps its values and dtype.
    pd.testing.assert_series_equal(converted["numpy"], df["numpy"])


def test_to_numpy_backed_handles_duplicate_column_names():
    """Columns are indexed positionally, so repeated names don't recurse.

    ``df[name]`` returns a ``DataFrame`` rather than a ``Series`` when the name
    is duplicated, which would make the recursion in ``to_numpy_backed`` never
    bottom out.
    """
    df = pa.table([pa.array([1, None]), pa.array([3, 4])], names=["a", "a"]).to_pandas(
        types_mapper=pd.ArrowDtype
    )

    converted = to_numpy_backed(df)

    assert converted.columns.tolist() == ["a", "a"]
    # The first column widens (int64 with a null), the second stays integral.
    assert converted.iloc[:, 0].tolist()[0] == 1.0
    assert np.isnan(converted.iloc[:, 0][1])
    assert converted.iloc[:, 1].tolist() == [3, 4]


def test_to_numpy_backed_dataframe_converts_every_column():
    df = pa.table(
        {"a": pa.array([1.0, None]), "b": pa.array([1, 2]), "c": pa.array(["x", None])}
    ).to_pandas(types_mapper=pd.ArrowDtype)
    assert all(isinstance(dtype, pd.ArrowDtype) for dtype in df.dtypes)

    converted = to_numpy_backed(df)

    assert not any(isinstance(dtype, pd.ArrowDtype) for dtype in converted.dtypes)
    assert converted.columns.tolist() == ["a", "b", "c"]
    # Without this, `to_numpy()` would fall back to `dtype=object`.
    assert converted[["a", "b"]].to_numpy().dtype == np.float64


# ---------------------------------------------------------------------------
# `PowerTransformer`
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("power", [0.5, 0, 2])
@pytest.mark.parametrize("values", [[1.0, None, -3.0], [1, None, -3]])
def test_power_transformer_yeo_johnson_with_nulls(power, values):
    """Nulls in an Arrow-backed column must not break the sign mask.

    `series >= 0` on an Arrow-backed column returns a three-valued
    `bool[pyarrow]` mask, which cannot index the NumPy result array.
    """
    ds = ray.data.from_items([{"v": v} for v in values], override_num_blocks=1)
    _assert_block_is_arrow_backed(ds, "v")

    transformed = PowerTransformer(columns=["v"], power=power).fit_transform(ds)
    out_df = transformed.to_pandas()

    assert transformed.schema().types == [pa.float64()]
    # The null must stay null rather than being silently filled with 0.0.
    assert pd.isna(out_df["v"][1])
    assert not pd.isna(out_df["v"][0])
    assert not pd.isna(out_df["v"][2])


def test_power_transformer_yeo_johnson_with_nulls_matches_non_null_math():
    """A null row must not perturb the values computed for other rows."""
    ds = ray.data.from_items(
        [{"v": 1.0}, {"v": None}, {"v": -3.0}], override_num_blocks=1
    )
    _assert_block_is_arrow_backed(ds, "v")

    out = PowerTransformer(columns=["v"], power=0.5).fit_transform(ds).to_pandas()["v"]

    assert out[0] == pytest.approx((np.power(1.0 + 1, 0.5) - 1) / 0.5)
    assert out[2] == pytest.approx(-(np.power(3.0 + 1, 2 - 0.5) - 1) / (2 - 0.5))


def test_power_transformer_with_nulls_and_output_columns():
    ds = ray.data.from_items(
        [{"v": 1.0}, {"v": None}, {"v": -3.0}], override_num_blocks=1
    )
    _assert_block_is_arrow_backed(ds, "v")

    out_df = (
        PowerTransformer(columns=["v"], power=0.5, output_columns=["v_transformed"])
        .fit_transform(ds)
        .to_pandas()
    )

    assert out_df.columns.tolist() == ["v", "v_transformed"]
    assert pd.isna(out_df["v_transformed"][1])
    # The input column must be left untouched.
    assert out_df["v"].tolist()[0] == 1.0


# ---------------------------------------------------------------------------
# `Categorizer`
# ---------------------------------------------------------------------------


class TestCategorizerWithArrowBackedNulls:
    """Nulls in Arrow-backed columns must not break fitting or transforming.

    An Arrow-backed column represents missing values with `pd.NA`. `pd.NA` has no
    truth value, so sorting the fitted categories raises
    `TypeError: boolean value of NA is ambiguous` unless it is filtered out.
    """

    @staticmethod
    def _dataset(values):
        ds = ray.data.from_items([{"g": v} for v in values], override_num_blocks=1)
        _assert_block_is_arrow_backed(ds, "g")
        return ds

    @pytest.mark.parametrize(
        "values",
        [
            pytest.param(["male", None, "female"], id="string"),
            pytest.param([1, None, 2], id="int"),
        ],
    )
    def test_fit_transform_with_nulls(self, values):
        ds = self._dataset(values)

        transformed = Categorizer(columns=["g"]).fit_transform(ds)

        out = transformed.to_pandas()["g"]
        assert isinstance(out.dtype, pd.CategoricalDtype)
        # The null must not become a category of its own.
        non_null = [v for v in values if v is not None]
        assert sorted(out.dtype.categories) == sorted(non_null)
        assert pd.isna(out[1])
        assert not pd.isna(out[0])

    def test_nulls_with_output_columns(self):
        ds = self._dataset(["male", None])

        transformed = Categorizer(
            columns=["g"], output_columns=["g_cat"]
        ).fit_transform(ds)

        out_df = transformed.to_pandas()
        assert out_df.columns.tolist() == ["g", "g_cat"]
        assert isinstance(out_df["g_cat"].dtype, pd.CategoricalDtype)
        assert pd.isna(out_df["g_cat"][1])

    def test_nulls_with_explicit_dtype(self):
        ds = self._dataset(["male", None, "female"])

        transformed = Categorizer(
            columns=["g"], dtypes={"g": pd.CategoricalDtype(["male", "female"])}
        ).fit_transform(ds)

        out = transformed.to_pandas()["g"]
        assert list(out.dtype.categories) == ["male", "female"]
        assert pd.isna(out[1])

    def test_column_null_typed_in_every_block(self):
        """A column with no values at all is Arrow-typed `null`."""
        ds = ray.data.from_items([{"g": None}, {"g": None}], override_num_blocks=1)
        (block,) = ray.get(ds.to_arrow_refs())
        assert pa.types.is_null(block.schema.field("g").type)

        transformed = Categorizer(
            columns=["g"], dtypes={"g": pd.CategoricalDtype(["male", "female"])}
        ).fit_transform(ds)

        out = transformed.to_pandas()["g"]
        assert out.isna().all()
        # The categorical dtype is not asserted here: an all-null categorical
        # column converts back to an Arrow `null`-typed block, so the round trip
        # yields `object`. That is pre-existing behavior, identical with
        # `DataContext.enable_arrow_backed_pandas_conversion` disabled.


# ---------------------------------------------------------------------------
# `Concatenator`
# ---------------------------------------------------------------------------


class TestConcatenatorWithArrowBackedNulls:
    """Nulls in Arrow-backed columns must not degrade the tensor output.

    An Arrow-backed column represents missing values with `pd.NA`, which cannot
    be stored in a numeric NumPy array. Without an explicit conversion,
    `to_numpy()` silently returns `dtype=object` and Ray falls back to storing
    the column as pickled Python objects instead of a tensor -- with no error
    raised. These tests therefore assert on the resulting Arrow type, not just
    on the values.
    """

    @staticmethod
    def _dataset_with_null():
        ds = ray.data.from_items(
            [
                {"x": 1.0, "y": 2.0},
                {"x": None, "y": 4.0},
                {"x": 5.0, "y": 6.0},
            ],
            override_num_blocks=1,
        )
        _assert_block_is_arrow_backed(ds, "x")
        return ds

    def test_output_is_a_tensor_column_not_pickled_objects(self):
        ds = self._dataset_with_null()

        transformed = Concatenator(
            columns=["x", "y"], output_column_name="f"
        ).fit_transform(ds)

        (field,) = transformed.schema().types
        assert isinstance(field, get_arrow_extension_fixed_shape_tensor_types()), field
        assert field.value_type == pa.float64()
        assert field.shape == (2,)

    def test_null_becomes_nan_within_the_tensor(self):
        ds = self._dataset_with_null()

        rows = (
            Concatenator(columns=["x", "y"], output_column_name="f")
            .fit_transform(ds)
            .take_all()
        )

        tensors = [row["f"] for row in rows]
        assert all(t.dtype == np.float64 for t in tensors)
        np.testing.assert_array_equal(tensors[0], np.array([1.0, 2.0]))
        np.testing.assert_array_equal(tensors[2], np.array([5.0, 6.0]))
        # The missing value must survive as NaN rather than becoming 0.0.
        assert np.isnan(tensors[1][0])
        assert tensors[1][1] == 4.0

    def test_explicit_dtype_is_honoured(self):
        ds = self._dataset_with_null()

        transformed = Concatenator(
            columns=["x", "y"], output_column_name="f", dtype=np.dtype(np.float32)
        ).fit_transform(ds)

        (field,) = transformed.schema().types
        assert isinstance(field, get_arrow_extension_fixed_shape_tensor_types()), field
        assert field.value_type == pa.float32()

    def test_flatten_with_nulls(self):
        ds = self._dataset_with_null()

        rows = (
            Concatenator(columns=["x", "y"], output_column_name="f", flatten=True)
            .fit_transform(ds)
            .take_all()
        )

        assert rows[0]["f"].dtype == np.float64
        assert np.isnan(rows[1]["f"][0])

    @pytest.mark.parametrize("dtype", [np.int64, np.int32], ids=["int64", "int32"])
    def test_integer_dtype_with_nulls(self, dtype):
        """An integer `dtype` raised `TypeError: int() argument must be ... not
        'NAType'`, because `pd.NA` cannot be coerced to an integer. This is a
        separate failure from the default-dtype case: `to_numpy()` without a
        `dtype` degrades silently, but an integer `dtype` crashes outright."""
        ds = self._dataset_with_null()

        transformed = Concatenator(
            columns=["x", "y"], output_column_name="f", dtype=dtype
        ).fit_transform(ds)

        (field,) = transformed.schema().types
        assert isinstance(field, get_arrow_extension_fixed_shape_tensor_types()), field
        rows = transformed.take_all()
        # An integer tensor cannot represent the missing value, so it is cast
        # rather than preserved. What matters is that the rows with values are
        # exact and the column is still a tensor -- the same outcome as the
        # pre-Arrow-backed conversion.
        assert np.array_equal(rows[0]["f"], np.array([1, 2], dtype=dtype))
        assert np.array_equal(rows[2]["f"], np.array([5, 6], dtype=dtype))
        assert rows[1]["f"][1] == 4

    def test_flatten_with_explicit_dtype_and_nulls(self):
        """`flatten=True` casts each element with `np.atleast_1d(elem).astype()`,
        so a `pd.NA` element raised `TypeError: float() argument must be a
        string or a real number, not 'NAType'` even for a float dtype -- unlike
        the non-flattened path, which tolerated `dtype=np.float32`."""
        ds = self._dataset_with_null()

        rows = (
            Concatenator(
                columns=["x", "y"],
                output_column_name="f",
                flatten=True,
                dtype=np.dtype(np.float32),
            )
            .fit_transform(ds)
            .take_all()
        )

        assert rows[0]["f"].dtype == np.float32
        assert np.isnan(rows[1]["f"][0])

    def test_int_column_with_nulls_widens_to_float(self):
        """NumPy integer arrays cannot hold a missing value, so widening is
        required -- and is what the pre-Arrow-backed conversion also did."""
        ds = ray.data.from_items(
            [{"x": 1, "y": 2}, {"x": None, "y": 4}], override_num_blocks=1
        )
        _assert_block_is_arrow_backed(ds, "x")

        transformed = Concatenator(
            columns=["x", "y"], output_column_name="f"
        ).fit_transform(ds)

        (field,) = transformed.schema().types
        assert isinstance(field, get_arrow_extension_fixed_shape_tensor_types()), field
        assert field.value_type == pa.float64()
        assert field.shape == (2,)


# ---------------------------------------------------------------------------
# `FeatureHasher`
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "values", [[1.5, None], [1, None]], ids=["float_column", "int_column"]
)
def test_feature_hasher_with_arrow_backed_nulls(values):
    """Nulls in an Arrow-backed column must not degrade the tensor output.

    The hash accumulator sums the column values, and `0 + pd.NA` is `pd.NA`, so
    an unconverted null poisons every hash bucket and the output silently
    becomes a column of pickled Python objects instead of a tensor.
    """
    ds = ray.data.from_items(
        [{"a": values[0], "b": 2.5}, {"a": values[1], "b": 4.5}],
        override_num_blocks=1,
    )
    _assert_block_is_arrow_backed(ds, "a")

    transformed = FeatureHasher(
        columns=["a", "b"], num_features=4, output_column="h"
    ).fit_transform(ds)

    hashed_type = transformed.schema().types[transformed.schema().names.index("h")]
    assert isinstance(
        hashed_type, get_arrow_extension_fixed_shape_tensor_types()
    ), hashed_type
    assert hashed_type.value_type == pa.float64()

    rows = transformed.take_all()
    assert all(row["h"].dtype == np.float64 for row in rows)
    # The row without a null must have finite hash counts.
    assert np.isfinite(rows[0]["h"]).all()
    # The null must propagate as NaN rather than becoming 0.0.
    assert np.isnan(rows[1]["h"]).any()


# ---------------------------------------------------------------------------
# `SimpleImputer` with a null-typed column
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "imputer,expected_rows",
    [
        (
            SimpleImputer(columns=["a", "b"]),
            [{"a": 1.0, "b": 2.0}, {"a": 3.0, "b": 3.0}, {"a": 2.0, "b": 4.0}],
        ),
        (
            SimpleImputer(
                columns=["a", "b"],
                strategy="constant",
                # `fill_value` is annotated `numbers.Number`, which `float` only
                # satisfies at runtime (via ABC registration), not statically.
                fill_value=-1.0,  # pyrefly: ignore[bad-argument-type]
            ),
            [{"a": 1.0, "b": 2.0}, {"a": 3.0, "b": -1.0}, {"a": -1.0, "b": 4.0}],
        ),
    ],
    ids=["mean", "constant"],
)
def test_simple_imputer_column_all_null_within_a_block(imputer, expected_rows):
    # https://github.com/ray-project/ray/issues/64765: a column is typed per
    # block, so a column with values overall can still be entirely null in one
    # block -- typed pa.null() there. Arrow's null type carries no type
    # information, so mapping it to null[pyarrow] on the way to pandas made
    # _transform_pandas's fillna raise ArrowInvalid("Invalid null value").
    #
    # This needs Arrow blocks (from_pandas would never call to_pandas), and the
    # dtype assertions below must not be derived from the output -- coercing
    # expectations with astype(out_df.dtypes) hides exactly this regression.
    ds = ray.data.from_items(
        [
            {"a": 1.0, "b": 2.0},
            {"a": 3.0, "b": None},
            {"a": None, "b": 4.0},
        ],
        override_num_blocks=3,
    )

    # Guard the premise: without a null-typed column in some block, this test
    # would pass without exercising the bug at all.
    null_typed = [
        field.name
        for block in ray.get(ds.to_arrow_refs())
        for field in block.schema
        if pa.types.is_null(field.type)
    ]
    assert sorted(null_typed) == ["a", "b"]

    transformed = imputer.fit_transform(ds)

    assert transformed.take_all() == expected_rows
    # The imputed columns hold real values, so neither the Arrow type nor the
    # pandas dtype may be null-typed.
    assert transformed.schema().types == [pa.float64(), pa.float64()]
    assert transformed.to_pandas().dtypes.to_dict() == {
        "a": pd.ArrowDtype(pa.float64()),
        "b": pd.ArrowDtype(pa.float64()),
    }


def test_simple_imputer_output_columns_with_null_typed_block():
    # Same failure, but writing to separate output columns: the copy taken from
    # the null-typed source column has to be fillable too.
    ds = ray.data.from_items(
        [{"a": 1.0}, {"a": None}],
        override_num_blocks=2,
    )

    transformed = SimpleImputer(
        columns=["a"],
        strategy="constant",
        fill_value=0.0,  # pyrefly: ignore[bad-argument-type]
        output_columns=["a_imputed"],
    ).fit_transform(ds)

    assert transformed.take_all() == [
        {"a": 1.0, "a_imputed": 1.0},
        {"a": None, "a_imputed": 0.0},
    ]
    assert transformed.schema().names == ["a", "a_imputed"]
    assert transformed.schema().types == [pa.float64(), pa.float64()]


def test_simple_imputer_column_null_typed_in_every_block():
    # The degenerate case: the column is null-typed for the whole dataset, so
    # there is no type to fall back on. "constant" still has a fill value, so
    # the fill decides the output type. ("mean" has none -- see
    # test_imputer_all_nan_raise_error in test_imputer.py.)
    ds = ray.data.from_arrow(
        [
            pa.table({"a": pa.array([None, None], type=pa.null())}),
            pa.table({"a": pa.array([None], type=pa.null())}),
        ]
    )
    assert ds.schema().types == [pa.null()]

    transformed = SimpleImputer(
        columns=["a"],
        strategy="constant",
        fill_value=-1.0,  # pyrefly: ignore[bad-argument-type]
    ).fit_transform(ds)

    assert transformed.take_all() == [{"a": -1.0}, {"a": -1.0}, {"a": -1.0}]
    assert transformed.schema().types == [pa.float64()]


# ---------------------------------------------------------------------------
# Batch-format contract: user code gets NumPy-backed pandas
# ---------------------------------------------------------------------------


def _dtype_probe(df):
    """Report, from inside the worker, what the user function was handed.

    The dtype has to be observed where the UDF runs, not on the driver, so it is
    returned as data.
    """
    return pd.DataFrame(
        {
            "dtype": [str(df.dtypes["id"])],
            "numpy_dtype": [str(df[["id"]].to_numpy().dtype)],
        }
    )


def test_map_batches_pandas_batch_is_numpy_backed():
    """A pandas batch handed to user code must be the conventional kind.

    Were it Arrow-backed, ``dtype`` would be ``int64[pyarrow]`` and
    ``to_numpy()`` would degrade to ``object``.
    """
    rows = (
        ray.data.range(4, override_num_blocks=1)
        .map_batches(_dtype_probe, batch_format="pandas")
        .take_all()
    )

    assert rows == [{"dtype": "int64", "numpy_dtype": "int64"}]


def test_map_batches_pandas_supports_operators_the_arrow_backend_lacks():
    """``%`` and ``divmod`` are unimplemented on Arrow-backed columns.

    See https://github.com/pandas-dev/pandas/issues/58723; on an Arrow-backed
    batch this raises ``NotImplementedError: mod not implemented.``
    """

    def modulo(df):
        return df.assign(m=df["id"] % 2)

    rows = (
        ray.data.range(4, override_num_blocks=1)
        .map_batches(modulo, batch_format="pandas")
        .take_all()
    )

    assert [row["m"] for row in rows] == [0, 1, 0, 1]


def test_map_batches_pandas_missing_value_is_nan():
    """Nulls must arrive as ``np.nan``, whose comparisons are two-valued.

    ``pd.NA >= 0`` is ``pd.NA``, making the mask ``bool[pyarrow]`` and unusable
    as a NumPy index.
    """

    def probe(df):
        return pd.DataFrame(
            {
                "is_nan": [bool(np.isnan(value)) for value in df["v"]],
                "mask_dtype": [str((df["v"] >= 0).dtype)] * len(df),
            }
        )

    rows = (
        ray.data.from_items([{"v": 1.0}, {"v": None}], override_num_blocks=1)
        .map_batches(probe, batch_format="pandas")
        .take_all()
    )

    assert [row["is_nan"] for row in rows] == [False, True]
    assert {row["mask_dtype"] for row in rows} == {"bool"}


def test_iter_batches_pandas_is_numpy_backed():
    ds = ray.data.range(4, override_num_blocks=2)

    batches = list(ds.iter_batches(batch_format="pandas"))

    assert batches
    for batch in batches:
        assert isinstance(batch, pd.DataFrame)
        assert batch.dtypes["id"] == np.int64
        assert batch[["id"]].to_numpy().dtype == np.int64


def test_iter_batches_default_format_is_unaffected():
    """``batch_format="default"`` on an Arrow block yields numpy, not pandas."""
    batches = list(ray.data.range(2, override_num_blocks=1).iter_batches())

    assert all(isinstance(batch, dict) for batch in batches)


class TestToPandasStaysArrowBacked:
    """``Dataset.to_pandas`` deliberately keeps Arrow-backed dtypes.

    It returns a result to the driver rather than feeding a user function, and
    the Arrow-backed dtypes preserve the dataset's types.
    """

    def test_dtype_is_arrow_backed(self):
        df = ray.data.range(4).to_pandas()

        assert df.dtypes["id"] == pd.ArrowDtype(pa.int64())

    def test_integer_column_with_nulls_stays_integral(self):
        ds = ray.data.from_items([{"v": 1}, {"v": None}], override_num_blocks=1)

        df = ds.to_pandas()

        # NumPy-backed pandas would have to widen this to float64.
        assert df.dtypes["v"] == pd.ArrowDtype(pa.int64())
        assert df["v"][0] == 1
        assert pd.isna(df["v"][1])


class _BatchDtypeRecorder(Preprocessor):
    """Reports the dtype of the pandas batch that Ray hands a preprocessor."""

    _is_fittable = False

    def _transform_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"dtype": [str(df.dtypes["id"])] * len(df)})


def test_preprocessors_receive_numpy_backed_pandas():
    """``_transform_pandas`` gets the same batch any ``map_batches`` function does.

    ``Preprocessor`` is public, so a subclass's ``_transform_pandas`` is user code
    as often as it is Ray's; it must not be handed ``pd.NA``. This recorder is
    itself a user-defined subclass, which is the case that matters.
    """
    ds = ray.data.range(2, override_num_blocks=1)

    rows = _BatchDtypeRecorder().transform(ds).take_all()

    assert rows == [{"dtype": "int64"}, {"dtype": "int64"}]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
