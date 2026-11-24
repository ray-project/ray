"""Tests for ``_add_statistics_bounds`` -- the statistics-bound rewrite.

The rewrite turns the leaf forms PyArrow cannot evaluate against Parquet
statistics into comparisons it can. Because it is a semantics-preserving
transform over a large, structured input space, the suite is property-based
(Hypothesis) around three invariants the hand-picked cases only sampled:

1. the rewrite preserves semantics, for a leaf and under ``~``;
2. ``FooterReader`` pruning keeps every group holding a matching row, and
   claims ``fully_matched`` only where every row really matches;
3. the rewrite is idempotent: re-entry on its own output changes nothing.

The prefix and overflow boundaries the ``@example`` cases pin are always
tried, and the generator sweeps the space around them. The narrow-column
case reads end to end through ``read_parquet`` on a local cluster.
"""

import operator
import os
import tempfile
from typing import TYPE_CHECKING, NamedTuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st
from pyarrow.fs import LocalFileSystem

import ray
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import FooterReader
from ray.data._internal.datasource_v2.formats.parquet.statistics_bounds import (
    _add_statistics_bounds,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data.expressions import col

if TYPE_CHECKING:
    from ray.data.expressions import Expr

_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1

_INTEGER_TYPES = [
    pa.int8(),
    pa.int16(),
    pa.int32(),
    pa.int64(),
    pa.uint8(),
    pa.uint16(),
    pa.uint32(),
]
_STRING_TYPES = [pa.string(), pa.large_string()]

# Values every valid string can hold, spanning the boundaries the prefix
# bound reasons over: the codepoint below the surrogate block, the first
# one above it, and the Unicode maximum.
_STRING_VALUES = [
    "",
    "a",
    "b",
    "z",
    "zz",
    "s000",
    "s001",
    "s080",
    "s081",
    "\ud7ff",
    "\ud7ffz",
    "\ue000",
    chr(0x10FFFF),
    chr(0x10FFFF) + "a",
]

# Prefixes spanning the successor space: the empty prefix, one ending at
# U+D7FF (whose successor jumps the surrogate block), one ending at the
# Unicode maximum (which has none), and plain ASCII. Surrogates are absent
# on purpose -- a filter holding one does not evaluate, so it cannot run
# through the oracle; its rejection is pinned by a dedicated test below.
_PREFIXES = [
    "",
    "a",
    "s0",
    "s08",
    "\ud7ff",
    "\ud7ffz",
    "a" + chr(0x10FFFF),
    chr(0x10FFFF),
]


def _reader(**kwargs):
    return FooterReader(filesystem=LocalFileSystem(), io_concurrency=2, **kwargs)


def _table(column_type, values):
    return pa.table({"x": pa.array(values, type=column_type)})


class _Case(NamedTuple):
    """A drawn rewrite input: column type, raw column values, predicate."""

    column_type: pa.DataType
    values: list
    expr: "Expr"
    row_group_size: int = 1


def _literals():
    """Integer literals the comparisons can hold.

    Python ints biased to the int64 extremes the bound construction cares
    about, plus NumPy scalars of every width the rewrite accepts. The one
    exclusion is np.uint64: a literal outside int64 makes the filter
    itself unevaluable, so the oracle cannot run on it.
    """
    return st.one_of(
        st.sampled_from(
            [
                _INT64_MIN,
                _INT64_MIN + 1,
                _INT64_MIN + 126,
                _INT64_MIN + 127,
                -1,
                0,
                1,
                74,
                75,
                76,
                77,
                126,
                127,
                128,
                255,
                256,
                _INT64_MAX - 1,
                _INT64_MAX,
            ]
        ),
        st.integers(min_value=-320, max_value=320),
        st.builds(np.int8, st.integers(min_value=-128, max_value=127)),
        st.builds(np.int16, st.integers(min_value=-(1 << 15), max_value=(1 << 15) - 1)),
        st.builds(np.int32, st.integers(min_value=-(1 << 31), max_value=(1 << 31) - 1)),
        st.builds(np.int64, st.integers(min_value=_INT64_MIN, max_value=_INT64_MAX)),
        st.builds(np.uint8, st.integers(min_value=0, max_value=255)),
        st.builds(np.uint16, st.integers(min_value=0, max_value=(1 << 16) - 1)),
        st.builds(np.uint32, st.integers(min_value=0, max_value=(1 << 32) - 1)),
    )


def _prefixes():
    return st.one_of(
        st.sampled_from(_PREFIXES),
        st.text(alphabet="abz08", max_size=3),
        st.text(alphabet="\ud7ff\ue000" + chr(0x10FFFF) + "a", max_size=2),
    )


def _values(column_type):
    """A column's raw values: nulls mixed with in-range extrema."""
    if pa.types.is_integer(column_type):
        info = np.iinfo(column_type.to_pandas_dtype())
        element = st.one_of(
            st.none(),
            st.integers(min_value=int(info.min), max_value=int(info.max)),
            st.sampled_from([int(info.min), int(info.max)]),
        )
        return st.lists(element, min_size=1, max_size=16)
    element = st.one_of(
        st.none(),
        st.sampled_from(_STRING_VALUES),
        st.text(alphabet="abz08" + "\ud7ff" + "\ue000" + chr(0x10FFFF), max_size=3),
    )
    return st.lists(element, min_size=1, max_size=16)


@st.composite
def _addition_comparisons(draw):
    """``(x + c) >=/> t`` and ``c + x >=/> t`` -- the modeled leaf shape."""
    offset = draw(_literals())
    target = draw(_literals())
    column = col("x")
    addition = column + offset if draw(st.booleans()) else offset + column
    if draw(st.booleans()):
        return addition >= target
    return addition > target


@st.composite
def _subtraction_comparisons(draw):
    """``(x - c) >=/> t`` -- an arithmetic shape without a bound."""
    offset = draw(_literals())
    target = draw(_literals())
    column = col("x")
    if draw(st.booleans()):
        return column - offset >= target
    return column - offset > target


@st.composite
def _plain_comparisons(draw, string=False):
    """``x <op> t`` -- a comparison PyArrow prunes by itself."""
    column = col("x")
    op = draw(st.sampled_from([operator.ge, operator.gt, operator.lt, operator.le]))
    target = draw(st.sampled_from(_STRING_VALUES) if string else _literals())
    return op(column, target)


@st.composite
def _starts_with(draw):
    return col("x").str.starts_with(draw(_prefixes()))


def _leaves(column_type):
    """One predicate leaf over the schema's single column."""
    if pa.types.is_integer(column_type):
        return st.one_of(
            _addition_comparisons(),
            _subtraction_comparisons(),
            _plain_comparisons(),
        )
    return st.one_of(
        _starts_with(),
        _plain_comparisons(string=True),
    )


def _combiner(child):
    return st.one_of(
        child.map(operator.invert),
        st.builds(operator.and_, child, child),
        st.builds(operator.or_, child, child),
    )


def _predicates(column_type):
    """Leaves combined with ``&``/``|``/``~`` to depth three."""
    return st.recursive(
        _leaves(column_type),
        _combiner,
        max_leaves=3,
    )


@st.composite
def _semantics_cases(draw):
    column_type = draw(st.sampled_from(_INTEGER_TYPES + _STRING_TYPES))
    return _Case(
        column_type=column_type,
        values=draw(_values(column_type)),
        expr=draw(_predicates(column_type)),
    )


@st.composite
def _pruning_predicates(draw, column_type, values):
    """A predicate pinned near the drawn values, so groups actually match.

    The arithmetic threshold sits on a drawn value (+/- one), and the
    prefix is a prefix of a drawn value, so pruning has real work to do
    and the fully-matched claims are not vacuous.
    """
    if pa.types.is_integer(column_type):
        present = [v for v in values if v is not None] or [0]
        offset = draw(st.sampled_from([1, 2, 3, 5]))
        base = draw(st.sampled_from(present))
        jitter = draw(st.sampled_from([-1, 0, 1]))
        column = col("x")
        addition = column + offset if draw(st.booleans()) else offset + column
        # Keep the literal inside int64: beyond it, the filter itself does
        # not evaluate and the oracle cannot run.
        threshold = min(base + offset + jitter, _INT64_MAX)
        leaf = addition > threshold if draw(st.booleans()) else addition >= threshold
    else:
        present = [v for v in values if v] or ["a"]
        value = draw(st.sampled_from(present))
        end = draw(st.integers(min_value=1, max_value=len(value)))
        leaf = col("x").str.starts_with(value[:end])
    return draw(st.recursive(st.just(leaf), _combiner, max_leaves=1))


@st.composite
def _pruning_cases(draw):
    column_type = draw(st.sampled_from(_INTEGER_TYPES + _STRING_TYPES))
    values = draw(_values(column_type))
    return _Case(
        column_type=column_type,
        values=values,
        expr=draw(_pruning_predicates(column_type=column_type, values=values)),
        row_group_size=draw(st.sampled_from([1, 2, 3, 5, 8])),
    )


@settings(max_examples=500)
@given(case=_semantics_cases())
@example(
    case=_Case(
        column_type=pa.string(),
        values=[
            "zz",
            "\ud7ff",
            "\ud7ffz",
            "\ue000",
            chr(0x10FFFF),
            chr(0x10FFFF) + "a",
        ],
        expr=col("x").str.starts_with("\ud7ff"),
    )
)
@example(
    case=_Case(
        column_type=pa.string(),
        values=["zz", "a" + chr(0x10FFFF), "b"],
        expr=~(col("x").str.starts_with("a" + chr(0x10FFFF))),
    )
)
@example(
    case=_Case(
        column_type=pa.string(),
        values=["", "a", None],
        expr=col("x").str.starts_with(""),
    )
)
@example(
    case=_Case(
        column_type=pa.int64(),
        values=[None, _INT64_MIN, -1, 0, 1, _INT64_MAX],
        expr=(col("x") + 127) >= _INT64_MIN,
    )
)
@example(
    case=_Case(
        column_type=pa.int64(),
        values=[None, _INT64_MIN, _INT64_MAX],
        expr=(col("x") + 127) > _INT64_MIN,
    )
)
@example(
    case=_Case(
        column_type=pa.uint8(),
        values=[None, 0, 1, 74, 75, 255],
        expr=(col("x") + 255) >= 76,
    )
)
@example(
    case=_Case(
        column_type=pa.int8(),
        values=[127, 127, 75, 126],
        expr=(col("x") + 1) >= 76,
    )
)
def test_rewrite_preserves_semantics(case):
    """The rewrite accepts exactly the rows its predicate accepts.

    Each drawn case checks the predicate and its negation: the bound must
    keep every row the leaf keeps, and under ``~`` must not turn into a
    filter that drops rows the negation needs. The examples pin the
    boundaries so they are always tried: the prefix successor jump over the
    surrogate block, the prefix with no successor, the empty prefix, the
    int64 bound constant that does not fit a scalar, and the signed wrap
    region.
    """
    table = _table(column_type=case.column_type, values=case.values)
    for e in (case.expr, ~case.expr):
        derived = _add_statistics_bounds(expr=e, schema=table.schema)
        assert table.filter(derived.to_pyarrow()).equals(table.filter(e.to_pyarrow()))


@settings(max_examples=30, deadline=None)
@given(case=_pruning_cases())
def test_derived_pruning_soundness_and_flag_exactness(case):
    """Pruning loses no group a matching row lives in, and over-claims nothing.

    Through ``FooterReader``: every group holding a matching row must
    survive pruning, and a ``fully_matched`` group's ``num_rows`` must be
    the exact survivor count. The invariants replace pinned expected
    outputs, which pass a group the rewrite prunes by mistake entirely
    when it is not a key in the expectation.
    """
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "pruning.parquet")
        pq.write_table(
            _table(column_type=case.column_type, values=case.values),
            path,
            row_group_size=case.row_group_size,
        )
        size = os.path.getsize(path)
        chunks = _reader(filter_expr=case.expr)._read_and_chunk(
            FileInfo(path=path, size=size)
        )

        parquet_file = pq.ParquetFile(path)
        predicate = case.expr.to_pyarrow()
        required = {
            i
            for i in range(parquet_file.num_row_groups)
            if parquet_file.read_row_group(i).filter(predicate).num_rows
        }
        by_idx = {rg.unit_ids[0]: rg.fully_matched for rg in chunks.row_groups}
        # Soundness: a group holding a matching row is never pruned outright.
        assert required <= set(by_idx)
        # Flag exactness: a fully-matched group's num_rows is the exact
        # survivor count, so limit push-down cannot stop early.
        for idx, fully in by_idx.items():
            if not fully:
                continue
            group = parquet_file.read_row_group(idx)
            assert group.filter(predicate).num_rows == group.num_rows


@settings(max_examples=50, deadline=None)
@given(case=_semantics_cases())
def test_rewrite_is_idempotent(case):
    """Re-entry on the rewrite's own output is a fixed point.

    The signed bound keeps the original leaf (``lower & (expr | upper)``),
    so the rewrite meets its own output on every re-entry: a double wrap
    must not change what the filter returns.
    """
    table = _table(column_type=case.column_type, values=case.values)
    once = _add_statistics_bounds(expr=case.expr, schema=table.schema)
    twice = _add_statistics_bounds(expr=once, schema=table.schema)
    assert table.filter(twice.to_pyarrow()).equals(table.filter(once.to_pyarrow()))


@pytest.mark.parametrize("prefix", ["\ud800", "a\udfff"])
def test_prefix_with_surrogate_stays_untouched(prefix):
    """A prefix no valid string can hold has no range to bound with.

    The property suite cannot draw this prefix: its filter does not
    evaluate, because PyArrow rejects the surrogate scalar -- which is
    exactly why the leaf must pass through untouched.
    """
    predicate = col("x").str.starts_with(prefix)
    schema = pa.schema([("x", pa.string())])
    assert _add_statistics_bounds(expr=predicate, schema=schema) is predicate


def test_narrow_file_column_keeps_its_rows(tmp_path, ray_start_regular_shared):
    """A file column narrower than the dataset's is not pruned by its range.

    An int8 file next to an int64 file is evaluated at the unified int64
    schema, where 124 + 5 does not wrap -- the two-sided range alone dropped
    the int8 file's rows. The original leaf stays in the rewritten filter, so
    the group survives and the evaluation decides.
    """
    pq.write_table(
        pa.table({"x": pa.array([124, 127], type=pa.int8())}),
        str(tmp_path / "narrow.parquet"),
        row_group_size=2,
    )
    pq.write_table(
        pa.table({"x": pa.array([0, 1], type=pa.int64())}),
        str(tmp_path / "wide.parquet"),
    )

    ds = ray.data.read_parquet(str(tmp_path))
    rows = ds.filter(expr=(col("x") + 5) >= 76).take_all()
    assert sorted(row["x"] for row in rows) == [124, 127]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
