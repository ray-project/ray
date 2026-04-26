"""Tests for DeltaDatasource predicate pushdown.

These tests exercise the pushdown contract that ray-project/ray#61547 asks for:
when a filter on a partition column is applied to a Delta-Lake-backed dataset,
the Filter operator should disappear from the optimized logical plan and the
underlying ``DeltaTable.file_uris`` call should receive a ``partition_filters``
DNF that excludes the pruned partitions.

The tests are written before the ``DeltaDatasource`` implementation lands; until
then most cases are expected to fail (Filter still present in the plan, file_uris
called without partition_filters, or the new datasource module not importable).
That is the intended TDD state.

Stats-based / non-partition file skipping (delta-io/delta-rs#3014) is out of
scope for this file -- it is deferred to a follow-up PR.
"""

import os
from unittest import mock

import pyarrow as pa
import pytest

import ray
from ray.data._internal.logical.operators import Filter
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import (
    get_operator_types as _get_operator_types,
    plan_has_operator as _has_operator_type,
)
from ray.tests.conftest import *  # noqa

deltalake = pytest.importorskip("deltalake")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _filter_op_removed(ds) -> bool:
    """True if the optimized logical plan no longer contains a Filter operator.

    Mirrors the Iceberg pushdown check in test_iceberg.py:329-364.
    """
    optimized = LogicalOptimizer().optimize(ds._plan._logical_plan)
    return not _has_operator_type(optimized, Filter)


def _operator_types(ds):
    """Return the operator types in the optimized plan, for diagnostics."""
    optimized = LogicalOptimizer().optimize(ds._plan._logical_plan)
    return _get_operator_types(optimized)


def _write_partitioned(path, *, partitions=("a", "b", "c"), rows_per_partition=10):
    """Write a Hive-partitioned Delta table with a string partition column ``part``
    and an integer data column ``x``. One physical parquet file per partition."""
    import pandas as pd
    from deltalake import write_deltalake

    parts, xs = [], []
    for i, p in enumerate(partitions):
        parts.extend([p] * rows_per_partition)
        xs.extend(range(i * 100, i * 100 + rows_per_partition))
    df = pd.DataFrame({"part": parts, "x": xs})
    write_deltalake(
        path, pa.Table.from_pandas(df, preserve_index=False), partition_by=["part"]
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def partitioned_delta_table(tmp_path):
    """Hive-partitioned Delta table at ``tmp_path/delta``: 3 partitions {a,b,c},
    10 rows each, one parquet file per partition. Returns the path."""
    path = os.path.join(tmp_path, "delta")
    _write_partitioned(path)
    return path


@pytest.fixture
def file_uris_spy():
    """Spy on ``deltalake.DeltaTable.file_uris`` while preserving real behavior.

    Yields the Mock so tests can assert on ``.call_args_list``. We use
    ``autospec=True`` so the spy enforces the real signature
    ``file_uris(self, partition_filters=None)``.
    """
    original = deltalake.DeltaTable.file_uris

    def _wrap(self, *args, **kwargs):
        return original(self, *args, **kwargs)

    with mock.patch.object(
        deltalake.DeltaTable, "file_uris", autospec=True, side_effect=_wrap
    ) as spy:
        yield spy


def _partition_filters_seen(spy):
    """Return the list of ``partition_filters`` values that were passed to
    every ``file_uris`` invocation captured by the spy. ``None`` covers both
    the omitted-kwarg case and explicit ``partition_filters=None``."""
    seen = []
    for call in spy.call_args_list:
        # autospec=True puts ``self`` first in args
        args, kwargs = call.args, call.kwargs
        if "partition_filters" in kwargs:
            seen.append(kwargs["partition_filters"])
        elif len(args) >= 2:
            seen.append(args[1])
        else:
            seen.append(None)
    return seen


# ---------------------------------------------------------------------------
# A. Pushdown contract (datasource-level)
# ---------------------------------------------------------------------------


def test_delta_datasource_supports_pushdown(partitioned_delta_table):
    """The new DeltaDatasource must opt into the predicate pushdown mixin."""
    from ray.data._internal.datasource.delta_datasource import DeltaDatasource

    ds = DeltaDatasource(partitioned_delta_table)
    assert ds.supports_predicate_pushdown() is True
    assert ds.get_current_predicate() is None


def test_delta_datasource_apply_predicate_combines_with_and(partitioned_delta_table):
    """Two successive ``apply_predicate`` calls must AND-combine, per the
    _DatasourcePredicatePushdownMixin contract (datasource.py:184-221)."""
    from ray.data._internal.datasource.delta_datasource import DeltaDatasource

    ds = DeltaDatasource(partitioned_delta_table)
    p1 = col("part") == "a"
    p2 = col("x") > 5

    ds1 = ds.apply_predicate(p1)
    ds2 = ds1.apply_predicate(p2)

    assert ds.get_current_predicate() is None  # original untouched
    assert ds1.get_current_predicate() == p1
    assert ds2.get_current_predicate() == (p1 & p2)


# ---------------------------------------------------------------------------
# B. Partition pruning -- logical plan + correctness
# ---------------------------------------------------------------------------


def test_delta_partition_pushdown_logical_plan(partitioned_delta_table):
    """Filter on a partition column must be removed from the optimized plan
    (i.e., pushed into the Read). Same pattern as test_iceberg.py:329-364."""
    ds = ray.data.read_delta(partitioned_delta_table)
    filtered = ds.filter(expr=col("part") == "a")

    assert _filter_op_removed(filtered), (
        f"Filter should be pushed down to read, got operators: "
        f"{_operator_types(filtered)}"
    )


def test_delta_partition_pushdown_row_count(partitioned_delta_table):
    """End-to-end: only the matching partition's rows are returned."""
    ds = ray.data.read_delta(partitioned_delta_table)
    filtered = ds.filter(expr=col("part") == "a")
    assert filtered.count() == 10  # one partition, 10 rows


def test_delta_partition_pushdown_in_clause(partitioned_delta_table):
    """``is_in`` translates to a DNF ``in`` op that delta-rs accepts."""
    ds = ray.data.read_delta(partitioned_delta_table)
    filtered = ds.filter(expr=col("part").is_in(["a", "b"]))

    assert _filter_op_removed(filtered), (
        f"Filter (is_in) should be pushed down, got operators: "
        f"{_operator_types(filtered)}"
    )
    assert filtered.count() == 20


def test_delta_partition_pushdown_not_equal(partitioned_delta_table):
    """``!=`` translates to DNF, supported by delta-rs."""
    ds = ray.data.read_delta(partitioned_delta_table)
    filtered = ds.filter(expr=col("part") != "c")

    assert _filter_op_removed(filtered), (
        f"Filter (!=) should be pushed down, got operators: "
        f"{_operator_types(filtered)}"
    )
    assert filtered.count() == 20


# ---------------------------------------------------------------------------
# C. File spy -- proves files were actually skipped (the headline test)
# ---------------------------------------------------------------------------


def test_delta_partition_pushdown_calls_file_uris_with_filter(
    partitioned_delta_table, file_uris_spy
):
    """The new datasource must hand a non-empty ``partition_filters`` DNF to
    ``DeltaTable.file_uris`` -- otherwise we are still reading every file and
    relying on PyArrow to throw rows away, which defeats the whole point of #61547.
    """
    ds = ray.data.read_delta(partitioned_delta_table)
    filtered = ds.filter(expr=col("part") == "a")
    filtered.count()  # force execution

    seen = _partition_filters_seen(file_uris_spy)
    assert any(f is not None and len(f) > 0 for f in seen), (
        f"Expected DeltaTable.file_uris to be called with a non-empty "
        f"partition_filters DNF, got call history: {seen}"
    )


def test_delta_no_filter_does_not_pass_partition_filters(
    partitioned_delta_table, file_uris_spy
):
    """Control: an unfiltered read must NOT invent a partition_filters value."""
    ray.data.read_delta(partitioned_delta_table).count()

    seen = _partition_filters_seen(file_uris_spy)
    assert seen, "Expected DeltaTable.file_uris to be called at least once"
    assert all(f is None for f in seen), (
        f"Expected partition_filters to be None for an unfiltered read, got: {seen}"
    )


# ---------------------------------------------------------------------------
# D. Data-column predicates -- graceful degradation (no stats skipping in this PR)
# ---------------------------------------------------------------------------


def test_delta_data_predicate_correctness(partitioned_delta_table):
    """Mixed partition + data predicate: result must be correct regardless of
    whether the data half got stats-skipped (it won't in this PR -- phase 2)."""
    ds = ray.data.read_delta(partitioned_delta_table)
    filtered = ds.filter(expr=(col("part") == "a") & (col("x") > 5))

    rows = filtered.take_all()
    assert {r["part"] for r in rows} == {"a"}
    assert all(r["x"] > 5 for r in rows)
    # partition "a" has x in [0..9], so x>5 leaves 4 rows (6,7,8,9)
    assert len(rows) == 4


def test_delta_unpushable_predicate_falls_back(partitioned_delta_table):
    """A predicate the datasource can't translate to a Delta partition DNF
    (here: a data-column-only predicate) must still produce correct results,
    and the Filter operator must remain in the plan so Ray's filter step
    handles what Delta couldn't."""
    ds = ray.data.read_delta(partitioned_delta_table)
    filtered = ds.filter(expr=col("x") > 105)

    # Filter is allowed to stay -- this is the graceful-fallback contract.
    # We don't assert it must stay (the implementation might legitimately push
    # it via stats in the future), only that the result is correct.
    rows = filtered.take_all()
    assert all(r["x"] > 105 for r in rows)
    # partitions a/b/c have x ranges [0..9], [100..109], [200..209] -> 14 rows
    assert len(rows) == 14


# ---------------------------------------------------------------------------
# E. Existing-behavior regression (these must keep passing on the new path)
# ---------------------------------------------------------------------------


def test_delta_read_with_version(tmp_path):
    """Time travel via ``version=`` must survive the new datasource code path."""
    import pandas as pd
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "delta_versioned")
    t1 = pa.Table.from_pandas(
        pd.DataFrame({"x": [1, 2, 3]}), preserve_index=False
    )
    t2 = pa.Table.from_pandas(
        pd.DataFrame({"x": [4, 5, 6]}), preserve_index=False
    )
    write_deltalake(path, t1, mode="append")
    write_deltalake(path, t2, mode="append")

    assert ray.data.read_delta(path).count() == 6
    assert ray.data.read_delta(path, version=0).count() == 3


def test_delta_read_with_columns(partitioned_delta_table):
    """Column projection must still work end-to-end."""
    ds = ray.data.read_delta(partitioned_delta_table, columns=["x"])
    assert ds.schema().names == ["x"]
    sample = ds.take(1)[0]
    assert set(sample.keys()) == {"x"}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
