"""Property-based tests for ``BlockColumnAccessor`` backend agreement.

``BlockColumnAccessor`` (``ray/data/block.py``) is an abstract interface with
exactly two implementations:

    ArrowBlockColumnAccessor   (_internal/arrow_block.py)
    PandasBlockColumnAccessor  (_internal/pandas_block.py)

``BlockColumnAccessor.for_column`` dispatches on the runtime type, and Ray Data
mixes both backends within one pipeline (``to_pandas``/``to_arrow``,
``batch_format``, pandas-returning UDFs). An aggregation's result must therefore
not depend on which backend a block happens to use, since block type is not
something the user controls per block.

Runs are derandomized, so a failure is reproducible on a given commit.
"""
import math
from typing import Any, Dict

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from ray.data.block import BlockAccessor, BlockColumnAccessor

settings.register_profile(
    "ray_data_ci",
    max_examples=200,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("ray_data_ci")

_NAN = object()  # sentinel: NaN is a distinct value but `nan != nan`


# The pandas dtypes Ray Data actually produces:
#   "pyarrow"  -> what BlockAccessor.to_pandas() yields (e.g. int64[pyarrow])
#   "nullable" -> pandas nullable extension dtypes (Int64/Float64)
#   "numpy"    -> plain numpy dtype with NaN for missing (pandas-returning UDFs)
PANDAS_DTYPES = ["pyarrow", "nullable", "numpy"]

string_values = st.lists(
    st.one_of(st.none(), st.sampled_from(["a", "b", "c"])), min_size=1, max_size=10
)
float_values = st.lists(
    st.one_of(
        st.none(),
        st.floats(allow_nan=False, allow_infinity=False, min_value=-1e9, max_value=1e9),
    ),
    min_size=1,
    max_size=10,
)


def _pandas_series(values, pa_type, pandas_dtype):
    arr = pa.array(values, type=pa_type)
    if pandas_dtype == "pyarrow":
        return arr.to_pandas(types_mapper=pd.ArrowDtype)
    if pandas_dtype == "nullable":
        dtype = "Float64" if pa_type == pa.float64() else "string"
        return pd.Series(values, dtype="object").astype(dtype)
    if pa_type == pa.float64():
        return pd.Series([np.nan if v is None else v for v in values], dtype="float64")
    return pd.Series(values, dtype="object")


def _accessors(values, pa_type, pandas_dtype):
    return (
        BlockColumnAccessor.for_column(pa.array(values, type=pa_type)),
        BlockColumnAccessor.for_column(_pandas_series(values, pa_type, pandas_dtype)),
    )


def _counts(result):
    """Normalize a value_counts result into a comparable {value: count} dict.

    NaN is keyed by a sentinel: it is a legitimate distinct value for
    Arrow-backed columns, but `nan != nan` makes it uncomparable directly.
    """
    if result is None:
        return None
    out: Dict[Any, int] = {}
    seen_none = 0
    for value, count in zip(result["values"], result["counts"]):
        if isinstance(value, float) and math.isnan(value):
            key = _NAN
        elif value == 0 and isinstance(value, float):
            # Arrow keeps 0.0 and -0.0 as separate entries while numpy and
            # nullable pandas merge them. That predates this code and converges
            # anyway, since `ValueCounter.combine` keys a Python dict where
            # 0.0 == -0.0. Canonicalize so it does not mask the real property.
            key = 0.0
        else:
            key = value
        if key is None:
            seen_none += 1
        out[key] = out.get(key, 0) + count
    assert seen_none <= 1, (
        f"missing values must collapse to exactly one `None` entry, "
        f"got {seen_none} in {result!r}"
    )
    return out


def _as_arrow_column(series):
    """The same block, converted to Arrow.

    This is the correct oracle: whether NaN survives as a value or becomes
    null is a property of the pandas dtype, so comparing against a fixed Arrow
    array would flag legitimate dtype-level differences.
    """
    table = BlockAccessor.for_block(pd.DataFrame({"a": series})).to_arrow()
    return table.column("a").combine_chunks()


@given(
    values=st.lists(
        st.one_of(st.none(), st.sampled_from(["a", "b", "c"])), min_size=1, max_size=10
    ),
    pandas_dtype=st.sampled_from(PANDAS_DTYPES),
)
def test_value_counts_agrees_across_backends_strings(values, pandas_dtype):
    arrow, pandas = _accessors(values, pa.string(), pandas_dtype)
    assert _counts(arrow.value_counts()) == _counts(pandas.value_counts())


@given(
    values=st.lists(
        st.one_of(
            st.none(),
            st.just(float("nan")),
            st.floats(allow_nan=False, allow_infinity=False, width=32),
        ),
        min_size=1,
        max_size=10,
    ),
    pandas_dtype=st.sampled_from(PANDAS_DTYPES),
)
def test_value_counts_agrees_across_backends_floats(values, pandas_dtype):
    """Floats are the hard case: NaN and null are distinct for Arrow-backed
    columns but not for numpy/nullable ones, so the two must be told apart
    rather than both folded into `None`.
    """
    series = _pandas_series(values, pa.float64(), pandas_dtype)
    pandas = BlockColumnAccessor.for_column(series)
    arrow = BlockColumnAccessor.for_column(_as_arrow_column(series))
    assert _counts(pandas.value_counts()) == _counts(arrow.value_counts()), (
        f"value_counts disagrees for {pandas_dtype} dtype\n"
        f"  values: {values}\n"
        f"  pandas: {pandas.value_counts()}\n"
        f"  arrow : {arrow.value_counts()}"
    )


@given(
    values=st.lists(
        st.one_of(st.none(), st.just(float("nan")), st.sampled_from([1.0, 2.0])),
        min_size=1,
        max_size=10,
    ),
    pandas_dtype=st.sampled_from(PANDAS_DTYPES),
)
def test_value_counts_accounts_for_every_row(values, pandas_dtype):
    """Counting nulls as values means the counts must sum to the row count."""
    series = _pandas_series(values, pa.float64(), pandas_dtype)
    counts = BlockColumnAccessor.for_column(series).value_counts()
    if counts is not None:
        assert sum(counts["counts"]) == len(values)


def _same_scalar(a, b, *, rel=1e-9):
    def missing(x):
        return x is None or x is pd.NA or (isinstance(x, float) and math.isnan(x))

    if missing(a) or missing(b):
        return missing(a) and missing(b)
    if isinstance(a, float) or isinstance(b, float):
        return math.isclose(float(a), float(b), rel_tol=rel, abs_tol=1e-9)
    return a == b


@given(
    values=float_values,
    method=st.sampled_from(["sum", "min", "max", "mean", "count"]),
    ignore_nulls=st.booleans(),
    pandas_dtype=st.sampled_from(PANDAS_DTYPES),
)
def test_aggregations_agree_across_backends(values, method, ignore_nulls, pandas_dtype):
    """Regression guard for the rest of the aggregation surface."""
    arrow, pandas = _accessors(values, pa.float64(), pandas_dtype)

    def call(accessor):
        try:
            return "ok", getattr(accessor, method)(
                ignore_nulls=ignore_nulls, as_py=True
            )
        except Exception as e:  # noqa: BLE001
            return "raised", type(e).__name__

    a_status, a = call(arrow)
    p_status, p = call(pandas)
    assert a_status == p_status, (
        f"{method}(ignore_nulls={ignore_nulls}) on {values} ({pandas_dtype}): "
        f"arrow={a_status}:{a} pandas={p_status}:{p}"
    )
    if a_status == "ok":
        assert _same_scalar(a, p), (
            f"{method}(ignore_nulls={ignore_nulls}) on {values} ({pandas_dtype}): "
            f"arrow={a!r} pandas={p!r}"
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
