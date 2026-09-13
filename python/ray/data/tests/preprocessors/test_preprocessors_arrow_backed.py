"""Every preprocessor, against Arrow-backed and NumPy-backed pandas blocks.

Since Ray 2.56 (#63017), a block is converted to an *Arrow-backed* pandas
DataFrame -- columns typed ``double[pyarrow]`` rather than ``float64``, with
missing values represented by ``pd.NA`` rather than ``np.nan``. Preprocessors
receive that frame directly, so each one has to cope with it.

``pd.NA`` breaks NumPy-style code in three distinct ways, and this file exercises
all three:

* ``series >= 0`` yields a *three-valued* mask (``True``/``False``/``pd.NA``)
  that cannot index a NumPy array -- an ``IndexError``.
* ``pd.NA`` is not orderable, so ``sorted()`` over values containing it raises
  ``TypeError: boolean value of NA is ambiguous``.
* ``df[["a", "b"]].to_numpy()`` silently returns ``dtype=object`` for *any*
  multi-column selection of Arrow-backed columns, even with no nulls present.
  Nothing raises; the output is simply no longer numeric.

The third is the reason several assertions here check dtypes rather than values.
A preprocessor that emits an ``object`` tensor column has not failed loudly --
it has shipped a non-numeric feature into training.

Each preprocessor is run over four input shapes:

``no_nulls``    Arrow-backed, complete data. Catches failures that have nothing
                to do with missing values, such as the ``to_numpy()`` fallback.
``with_nulls``  Arrow-backed, one ``pd.NA`` per column.
``all_null``    Arrow-backed, one column entirely null.
``numpy_nan``   A pandas block using ``np.nan`` -- the pre-2.56 representation,
                which must keep working.

Every case asserts one of two outcomes, and which one applies is a property of
the preprocessor rather than of the input:

* **Propagates** -- a missing input gives a missing output and no other row is
  affected. Required whenever the output type can represent "missing" and the
  computation is per-row.
* **Refuses legibly** -- raises an error naming the column and saying what to do
  about it. Required when no defensible output exists: an ordinal encoding has no
  integer meaning "absent", and there is no way to bin a column with no range.

Crashing is not refusing, which is why the refusal assertions check the message
and not just the exception type.
"""

from typing import Any, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data.preprocessors import (
    Categorizer,
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
    UniformKBinsDiscretizer,
)

# The null lands in the last row of every shape, so tests can look it up by
# index rather than searching for it.
NULL_ROW = 3


def _table(num, num2, cnt, txt, tokens) -> pa.Table:
    return pa.table(
        {
            "num": pa.array(num, pa.float64()),
            "num2": pa.array(num2, pa.float64()),
            "cnt": pa.array(cnt, pa.int64()),
            "txt": pa.array(txt, pa.string()),
            "tokens": pa.array(tokens, pa.list_(pa.string())),
        }
    )


SHAPES = {
    "no_nulls": lambda: _table(
        [1.0, -2.0, 3.0, 4.0],
        [3.0, 4.0, 5.0, 6.0],
        [1, 2, 3, 4],
        ["x", "y", "x", "y"],
        [["a"], ["b"], ["a", "b"], ["b"]],
    ),
    "with_nulls": lambda: _table(
        [1.0, -2.0, 3.0, None],
        [3.0, 4.0, 5.0, None],
        [1, 2, 3, None],
        ["x", "y", "x", None],
        [["a"], ["b"], ["a", "b"], None],
    ),
    "all_null": lambda: _table(
        [None] * 4,
        [3.0, 4.0, 5.0, 6.0],
        [None] * 4,
        [None] * 4,
        [None] * 4,
    ),
}


@pytest.fixture(params=list(SHAPES) + ["numpy_nan"])
def dataset(request):
    """A dataset per input shape, plus its name and whether it has nulls."""
    name = request.param
    if name == "numpy_nan":
        # A *pandas* block built from NumPy-backed columns: `np.nan` for the
        # numeric nulls and `None` for the object ones, exactly what Ray
        # produced before 2.56.
        #
        # `tokens` is dropped for this shape. A pandas block cannot carry a
        # Python-list column at all: with a null present `from_pandas` rejects it
        # while casting to Ray's tensor extension type, and without one the
        # block fails later, converting back to Arrow
        # (`ArrowNotImplementedError: Unsupported numpy type 17`, i.e. object).
        # That is a pandas-block limitation, not something these tests measure,
        # so `test_multi_hot_encoder` skips this shape.
        table = SHAPES["with_nulls"]().drop_columns(["tokens"])
        ds = ray.data.from_pandas(table.to_pandas())
    else:
        ds = ray.data.from_arrow(SHAPES[name]())
    return name, ds


# ---------------------------------------------------------------------------
# assertions
# ---------------------------------------------------------------------------


def _column(rows, name):
    return [row[name] for row in rows]


def _is_missing(value) -> bool:
    """All three spellings of "missing" count.

    Which one a row carries depends on the path the column took, not on anything
    the preprocessor decided: ``None`` from an Arrow column, ``np.nan`` from a
    NumPy-backed one, and ``pd.NA`` when the output column never made it back to
    Arrow -- a column of Python lists keeps the block in pandas, so ``take_all``
    hands back the pandas value untouched.

    ``pd.isna`` is not used because it returns an *array* for a list or ndarray
    value, which cannot be treated as a single truth value.
    """
    if value is None or value is pd.NA:
        return True
    return isinstance(value, float) and np.isnan(value)


def assert_numeric(rows, name):
    """The column must be numeric, not a box of Python objects.

    This is the assertion that catches the *silent* regression: a tensor column
    of ``dtype=object`` looks fine in a `take_all()` dump and is rejected only
    much later, by scikit-learn or PyTorch.

    Missing values are excluded before the check. ``take_all`` renders a null as
    ``None``, and a single ``None`` forces ``np.asarray`` to ``dtype=object``,
    which would make this assertion fire on correct output.
    """
    values = [v for v in _column(rows, name) if not _is_missing(v)]
    if not values:
        return  # nothing but nulls; `assert_null_preserved` covers this case
    flat = np.concatenate([np.atleast_1d(np.asarray(v)).ravel() for v in values])
    assert flat.dtype.kind in "fiub", (
        f"column {name!r} came back as {flat.dtype}, not a numeric dtype; "
        f"values={values}"
    )


def assert_null_preserved(rows, name):
    """A missing input must stay missing -- not become 0, and not vanish."""
    value = _column(rows, name)[NULL_ROW]
    assert _is_missing(
        value
    ), f"column {name!r} row {NULL_ROW} should still be null, got {value!r}"


def assert_no_nulls(rows, name):
    for i, value in enumerate(_column(rows, name)):
        assert not _is_missing(
            value
        ), f"column {name!r} row {i} should have been imputed, got {value!r}"


def assert_refuses_legibly(excinfo, column, *, mentions):
    """A refusal has to name the column and say what to do about it.

    Crashing is not the same as refusing. These preprocessors used to fail with
    an ``IndexError``, a ``TypeError`` on ``None + float``, or an
    ``AttributeError`` about ``'DataFrame' object has no attribute 'name'`` --
    none of which tell the user which column is empty or that emptiness is the
    problem.
    """
    message = str(excinfo.value)
    assert column in message, f"error should name column {column!r}: {message}"
    assert mentions in message, f"error should mention {mentions!r}: {message}"


# ---------------------------------------------------------------------------
# numeric preprocessors: nulls propagate, output stays numeric
# ---------------------------------------------------------------------------

NUMERIC = {
    "PowerTransformer": lambda: PowerTransformer(columns=["num"], power=0.5),
    "StandardScaler": lambda: StandardScaler(columns=["num"]),
    "MinMaxScaler": lambda: MinMaxScaler(columns=["num"]),
    "MaxAbsScaler": lambda: MaxAbsScaler(columns=["num"]),
    "Normalizer": lambda: Normalizer(columns=["num", "num2"]),
    "UniformKBinsDiscretizer": lambda: UniformKBinsDiscretizer(columns=["num"], bins=2),
    "CustomKBinsDiscretizer": lambda: CustomKBinsDiscretizer(
        columns=["num"], bins=[-10, 0, 10]
    ),
}


@pytest.mark.parametrize("name", list(NUMERIC))
def test_numeric_preprocessor(name, dataset):
    """Runs, keeps the column numeric, and leaves missing values missing."""
    shape, ds = dataset

    if shape == "all_null" and name == "UniformKBinsDiscretizer":
        # Binning is the one case here that cannot propagate: with no observed
        # values there is no range to divide into `bins` intervals, so there is
        # no output to produce. It refuses instead, naming the column.
        #
        # `CustomKBinsDiscretizer` is not in the same position and stays above:
        # its edges come from the user, so an all-null column just bins to nulls.
        with pytest.raises(ValueError) as excinfo:
            NUMERIC[name]().fit_transform(ds).take_all()
        assert_refuses_legibly(excinfo, "num", mentions="no values to bin")
        return

    rows = NUMERIC[name]().fit_transform(ds).take_all()

    assert len(rows) == 4, "preprocessors must preserve the row count"
    if shape == "all_null":
        # Every value is missing; there is nothing to assert about dtype beyond
        # the column still existing.
        assert_null_preserved(rows, "num")
        return

    assert_numeric(rows, "num")
    if shape != "no_nulls":
        assert_null_preserved(rows, "num")


SCALERS = {
    "StandardScaler": lambda: StandardScaler(columns=["num"]),
    "MinMaxScaler": lambda: MinMaxScaler(columns=["num"]),
    "MaxAbsScaler": lambda: MaxAbsScaler(columns=["num"]),
    "RobustScaler": lambda: RobustScaler(columns=["num"]),
}


@pytest.mark.parametrize("name", list(SCALERS))
def test_scaler_propagates_nulls_from_an_all_null_column(name):
    """An uncomputable statistic yields nulls, on either kind of block.

    Fitting on a column with no observed values leaves nothing to scale by, so
    ``stats_`` holds None -- no mean, no range, no absolute maximum, no
    quantiles. Every one of them answers with a column of nulls, which is what
    ``StandardScaler`` has done since #51281 and what the other three now do too.

    Both block types are checked because they used to disagree, and the
    disagreement was the tell that the behaviour was accidental rather than
    chosen. ``MaxAbsScaler`` divides a Series by the statistic, so an
    Arrow-backed column already yielded nulls (pandas treats None as a null
    scalar there) while a NumPy-backed one raised ``TypeError``. ``MinMaxScaler``
    and ``RobustScaler`` compute ``max - min`` and ``high - low`` *before*
    touching the column, which fails on two Nones whatever the backing. Stating
    the case explicitly makes all four agree on both block types, instead of
    leaving the right answer resting on a pandas arithmetic rule.
    """
    if name == "RobustScaler":
        pytest.importorskip("datasketches")

    table = pa.table({"num": pa.array([None] * 4, pa.float64())})
    for label, ds in [
        ("arrow block", ray.data.from_arrow(table)),
        ("pandas block", ray.data.from_pandas(table.to_pandas())),
    ]:
        rows = SCALERS[name]().fit_transform(ds).take_all()
        assert len(rows) == 4, f"{label}: row count changed"
        values = _column(rows, "num")
        assert all(_is_missing(v) for v in values), f"{label}: got {values}"


# ---------------------------------------------------------------------------
# tensor-producing preprocessors: the silent `dtype=object` case
# ---------------------------------------------------------------------------

TENSOR = {
    "Concatenator": (lambda: Concatenator(columns=["num", "num2"]), "concat_out"),
    "FeatureHasher": (
        lambda: FeatureHasher(
            columns=["num", "num2"], num_features=4, output_column="h"
        ),
        "h",
    ),
}


@pytest.mark.parametrize("name", list(TENSOR))
def test_tensor_preprocessor(name, dataset):
    """The packed column must be a numeric tensor, not pickled Python objects.

    ``Concatenator`` selects several columns at once, and a multi-column
    ``to_numpy()`` over Arrow-backed columns degrades to ``dtype=object`` even
    when no value is missing -- so ``no_nulls`` is a real case here, not a
    control.
    """
    shape, ds = dataset
    make, output = TENSOR[name]
    rows = make().fit_transform(ds).take_all()

    assert len(rows) == 4
    # An all-null input is covered too: hashing missing counts puts ``np.nan`` in
    # the buckets the column names hash to and leaves the rest at zero, which is
    # still a float tensor. That only holds because a missing count is mapped to
    # ``np.nan`` rather than left as ``pd.NA``, which would take the whole column
    # to ``dtype=object``.
    assert_numeric(rows, output)


# ---------------------------------------------------------------------------
# imputation: nulls are filled, not propagated
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("strategy", ["mean", "most_frequent", "constant"])
def test_simple_imputer(strategy, dataset):
    shape, ds = dataset
    kwargs: Dict[str, Any] = {"fill_value": 0.0} if strategy == "constant" else {}
    imputer = SimpleImputer(columns=["num"], strategy=strategy, **kwargs)

    if shape == "all_null" and strategy in ("mean", "most_frequent"):
        # There is nothing to impute *from*: no mean and no most frequent value.
        # Both strategies refuse with the same message, which `"mean"` has raised
        # since f4c4c81390 -- `"most_frequent"` used to die earlier, in `fit`,
        # with `IndexError: list index out of range`, so it never reached the
        # guard that message comes from.
        #
        # `"constant"` is unaffected: its fill value comes from the user, so an
        # all-null column is filled like any other.
        with pytest.raises(Exception) as excinfo:
            imputer.fit_transform(ds).take_all()
        assert_refuses_legibly(excinfo, "num", mentions="has no fill value")
        return

    rows = imputer.fit_transform(ds).take_all()
    assert len(rows) == 4
    assert_numeric(rows, "num")
    assert_no_nulls(rows, "num")


# ---------------------------------------------------------------------------
# categorical encoders: a null must raise the *documented* error
# ---------------------------------------------------------------------------

ENCODERS = {
    "OrdinalEncoder": lambda: OrdinalEncoder(columns=["txt"]),
    "OneHotEncoder": lambda: OneHotEncoder(columns=["txt"]),
    "LabelEncoder": lambda: LabelEncoder(label_column="txt"),
}


@pytest.mark.parametrize("name", list(ENCODERS))
def test_encoder_rejects_nulls_with_a_clear_error(name, dataset):
    """Encoders refuse null categories -- but with which error?

    Refusing is deliberate: a missing value is not a category, and the user is
    told to impute first. What matters is that the failure is that *documented*
    ``ValueError`` and not an incidental ``TypeError: boolean value of NA is
    ambiguous`` leaking out of ``sorted()``, which is what happens when
    ``is_null`` does not recognise ``pd.NA``.
    """
    shape, ds = dataset
    encoder = ENCODERS[name]()

    if shape == "no_nulls":
        rows = encoder.fit_transform(ds).take_all()
        assert len(rows) == 4
        return

    with pytest.raises(ValueError) as excinfo:
        encoder.fit_transform(ds).take_all()
    assert "null" in str(excinfo.value).lower(), (
        f"{name} should explain that the column contains nulls; "
        f"got {excinfo.value!r}"
    )


def test_categorizer_passes_nulls_through(dataset):
    """``Categorizer`` keeps nulls rather than refusing them.

    Unlike the encoders above it does not build an integer mapping that a null
    would corrupt -- it converts the column to pandas' ``category`` dtype, in
    which a missing value is simply not a category. So the null survives, on
    both backings.
    """
    shape, ds = dataset
    rows = Categorizer(columns=["txt"]).fit_transform(ds).take_all()

    assert len(rows) == 4
    if shape != "no_nulls":
        assert _column(rows, "txt")[NULL_ROW] is None


def test_multi_hot_encoder(dataset):
    """``MultiHotEncoder`` consumes list columns, so it gets its own case.

    The list column is what makes this a distinct case: an Arrow-backed list
    column is typed ``list<item: string>[pyarrow]`` rather than ``object``, so a
    dtype check written for NumPy-backed pandas does not recognise it as a list
    column and routes it to ``value_counts``, which pyarrow cannot compute for
    list types.
    """
    shape, ds = dataset
    if shape == "numpy_nan":
        pytest.skip("a pandas block cannot carry a list column; see the fixture")

    encoder = MultiHotEncoder(columns=["tokens"])
    if shape == "no_nulls":
        rows = encoder.fit_transform(ds).take_all()
        assert len(rows) == 4
        assert_numeric(rows, "tokens")
        return

    # Like the other encoders, it refuses null categories -- but from
    # `_validate_df` during *transform* rather than during fit, and only once the
    # list column is recognised at all. Before that it failed in fit with
    # `ArrowNotImplementedError`, which told the user nothing useful.
    #
    # The exception type is not `ValueError` here: a transform runs inside a Ray
    # task, so the `ValueError` arrives wrapped as
    # `RayTaskError(UserCodeException)`. The other encoders raise during fit, on
    # the driver, where the type survives. The message is what matters.
    with pytest.raises(Exception) as excinfo:
        encoder.fit_transform(ds).take_all()
    assert "null values" in str(excinfo.value)


# Encoders that treat a whole list as one category, rather than exploding it.
# This is the ``encode_lists=False`` branch of `compute_unique_value_indices`,
# which is reached separately from the ``True`` branch `MultiHotEncoder` uses.
WHOLE_LIST_ENCODERS = {
    "OneHotEncoder": lambda: OneHotEncoder(columns=["tokens"]),
    "OrdinalEncoder": lambda: OrdinalEncoder(columns=["tokens"], encode_lists=False),
}


@pytest.mark.parametrize("name", list(WHOLE_LIST_ENCODERS))
def test_whole_list_encoder_rejects_nulls_with_a_clear_error(name, dataset):
    """A list column encoded whole refuses nulls with the *documented* error.

    The ``encode_lists=False`` branch makes each list hashable with ``tuple(x)``.
    A missing row has no list to convert, so that raises ``TypeError: 'NAType'
    object is not iterable`` inside ``fit`` -- and because the fit runs in a Ray
    task, the user sees it wrapped as ``UDF failed to process a data block``,
    which names neither the column nor the nulls. Carrying the null through to
    ``unique_post_fn`` instead reaches the same "consider imputing missing values
    first" ``ValueError`` the scalar encoders raise.

    ``MultiHotEncoder`` does not cover this: it takes the ``encode_lists=True``
    branch, which was already guarded.
    """
    shape, ds = dataset
    if shape == "numpy_nan":
        pytest.skip("a pandas block cannot carry a list column; see the fixture")

    encoder = WHOLE_LIST_ENCODERS[name]()

    if shape == "no_nulls":
        rows = encoder.fit_transform(ds).take_all()
        assert len(rows) == 4
        return

    with pytest.raises(ValueError) as excinfo:
        encoder.fit_transform(ds).take_all()
    assert "null" in str(excinfo.value).lower(), (
        f"{name} should explain that the column contains nulls; "
        f"got {excinfo.value!r}"
    )


# ---------------------------------------------------------------------------
# text preprocessors: a null document propagates
# ---------------------------------------------------------------------------

TEXT = {
    "Tokenizer": lambda: Tokenizer(columns=["txt"]),
    "CountVectorizer": lambda: CountVectorizer(columns=["txt"]),
    "HashingVectorizer": lambda: HashingVectorizer(columns=["txt"], num_features=4),
}


@pytest.mark.parametrize("name", list(TEXT))
def test_text_preprocessor(name, dataset):
    """A missing document propagates; the other rows are unaffected.

    Unlike the encoders, these three fit no vocabulary that a null could corrupt
    -- ``Tokenizer`` and ``HashingVectorizer`` map each row independently, and
    ``CountVectorizer``'s vocabulary is still valid when built from the documents
    that are present -- so the gap is carried through rather than refused.

    A null document is *not* an empty one. ``Tokenizer`` gives it no token list
    and the vectorizers give it no count vector, instead of a zero vector that
    would be indistinguishable from a real document containing none of the
    vocabulary.

    They used to hand the missing value to the tokenization function, which
    called ``.split()`` on it and raised ``AttributeError`` -- killing the job
    over one empty cell in a text column.
    """
    shape, ds = dataset
    rows = TEXT[name]().fit_transform(ds).take_all()

    assert len(rows) == 4
    if shape == "all_null":
        assert all(_is_missing(v) for v in _column(rows, "txt"))
        return

    if shape != "no_nulls":
        assert_null_preserved(rows, "txt")

    present = [v for v in _column(rows, "txt") if not _is_missing(v)]
    assert len(present) == (4 if shape == "no_nulls" else 3), (
        "only the missing document should be missing; " f"got {_column(rows, 'txt')}"
    )
    if name != "Tokenizer":
        # The vectorizers emit counts, so the surviving rows must still be
        # numbers -- a null row must not drag the column to `dtype=object`.
        assert_numeric(rows, "txt")


# ---------------------------------------------------------------------------
# RobustScaler needs an optional dependency
# ---------------------------------------------------------------------------


def test_robust_scaler(dataset):
    pytest.importorskip("datasketches")
    shape, ds = dataset
    rows = RobustScaler(columns=["num"]).fit_transform(ds).take_all()
    assert len(rows) == 4
    if shape == "all_null":
        # No quantiles to centre or scale by, so the column comes back null.
        # `test_scaler_propagates_nulls_from_an_all_null_column` pins this on
        # both block types.
        assert_null_preserved(rows, "num")
        return

    assert_numeric(rows, "num")
    if shape != "no_nulls":
        assert_null_preserved(rows, "num")


# ---------------------------------------------------------------------------
# integer columns: nulls force a representation choice
# ---------------------------------------------------------------------------


def test_integer_column_with_nulls_stays_usable(dataset):
    """An integer column containing nulls is the awkward case.

    NumPy has no integer value meaning "missing", so the pre-2.56 behaviour was
    to widen to ``float64`` and use ``np.nan``. Arrow-backed columns keep the
    column integral. Either is acceptable; producing an ``object`` column is
    not.
    """
    shape, ds = dataset
    rows = StandardScaler(columns=["cnt"]).fit_transform(ds).take_all()
    assert len(rows) == 4
    if shape != "all_null":
        assert_numeric(rows, "cnt")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
