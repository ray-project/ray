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

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from ray.data.block import BlockColumnAccessor

settings.register_profile(
    "ray_data_ci",
    max_examples=200,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("ray_data_ci")


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
    """Normalize a value_counts result into a ``{value: count}`` dict."""
    if result is None:
        return None
    return dict(zip(result["values"], result["counts"]))


def _same_scalar(a, b, *, rel=1e-9):
    def missing(x):
        return x is None or x is pd.NA or (isinstance(x, float) and math.isnan(x))

    if missing(a) or missing(b):
        return missing(a) and missing(b)
    if isinstance(a, float) or isinstance(b, float):
        return math.isclose(float(a), float(b), rel_tol=rel, abs_tol=1e-9)
    return a == b


@given(values=string_values, pandas_dtype=st.sampled_from(PANDAS_DTYPES))
def test_value_counts_agrees_across_backends(values, pandas_dtype):
    """Arrow counts nulls as values of their own; pandas must do the same.

    Nulls are normalized to a single canonical ``None`` so that accumulators
    from the two backends merge on the same key in ``ValueCounter.combine``.
    """
    arrow, pandas = _accessors(values, pa.string(), pandas_dtype)
    assert _counts(arrow.value_counts()) == _counts(pandas.value_counts()), (
        f"value_counts disagrees by backend\n"
        f"  values ({pandas_dtype}): {values}\n"
        f"  arrow : {_counts(arrow.value_counts())}\n"
        f"  pandas: {_counts(pandas.value_counts())}"
    )


@given(values=string_values, pandas_dtype=st.sampled_from(PANDAS_DTYPES))
def test_value_counts_accounts_for_every_row(values, pandas_dtype):
    """Counting nulls as values means the counts must sum to the row count."""
    for accessor in _accessors(values, pa.string(), pandas_dtype):
        counts = accessor.value_counts()
        if counts is None:
            continue
        assert sum(counts["counts"]) == len(values)


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
