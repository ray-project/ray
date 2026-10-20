"""Tests that logical optimizer rules do not duplicate, reorder, or merge
non-idempotent expressions (``random``/``uuid``/``monotonically_increasing_id``).

Covers the shared ``is_idempotent`` contract as consumed by CommonSubExprElimination,
ProjectionPushdown, PredicatePushdown, and LimitPushdown.
"""

import pytest

import ray
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data.expressions import (
    col,
    monotonically_increasing_id,
    random,
    uuid,
)


def _optimized_plan_str(ds) -> str:
    return LogicalOptimizer().optimize(ds._logical_plan).dag.dag_str


# ---------------------------------------------------------------------------
# CommonSubExprElimination: independent non-idempotent draws must not merge.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("make_expr", [uuid, random])
def test_cse_does_not_merge_independent_nondeterministic_columns(
    ray_start_regular_shared, make_expr
):
    ds = ray.data.range(8).with_column("a", make_expr()).with_column("b", make_expr())
    rows = ds.take_all()
    assert all(r["a"] != r["b"] for r in rows)


def test_cse_does_not_merge_reused_seeded_random_object(ray_start_regular_shared):
    # The same seeded RandomExpr object reused in two columns must not be merged:
    # the runtime advances a per-instance block counter between evaluations, so the
    # two columns legitimately differ. (Under the old bug, CSE collapsed them.)
    shared = random(seed=42)
    ds = ray.data.range(8).with_column("a", shared).with_column("b", shared)
    rows = ds.take_all()
    assert any(r["a"] != r["b"] for r in rows)


def test_cse_still_merges_deterministic_subexpression(ray_start_regular_shared):
    # A repeated deterministic sub-expression must still be eligible for CSE.
    ds = ray.data.range(5).with_column("y", (col("id") + 1) * (col("id") + 1))
    assert ds.take_all() == [{"id": i, "y": (i + 1) * (i + 1)} for i in range(5)]


# ---------------------------------------------------------------------------
# ProjectionPushdown: a non-idempotent column referenced multiple times must
# not be inlined into multiple independent evaluations.
# ---------------------------------------------------------------------------


def test_projection_does_not_inline_nonidempotent_into_multiple_refs(
    ray_start_regular_shared,
):
    ds = (
        ray.data.range(6)
        .with_column("x", random())
        .with_column("y", col("x") + col("x"))
    )
    rows = ds.take_all()
    # If ``x`` were inlined as two independent draws, ``y`` would not equal ``2*x``.
    assert all(abs(r["y"] - 2 * r["x"]) < 1e-12 for r in rows)


def test_projection_still_fuses_deterministic_chain(ray_start_regular_shared):
    ds = (
        ray.data.range(6)
        .with_column("x", col("id") + 1)
        .with_column("y", col("x") + col("x"))
    )
    assert ds.take_all() == [{"id": i, "x": i + 1, "y": 2 * (i + 1)} for i in range(6)]


# ---------------------------------------------------------------------------
# PredicatePushdown: a filter must not be pushed below a projection that
# produces a non-idempotent column.
# ---------------------------------------------------------------------------


def test_predicate_not_pushed_below_nonidempotent_projection(ray_start_regular_shared):
    ds = (
        ray.data.range(10)
        .with_column("rid", monotonically_increasing_id())
        .filter(expr=col("id") > 4)
    )
    plan = _optimized_plan_str(ds)
    # Filter remains downstream of (above) the Project.
    assert plan.rfind("Filter") > plan.rfind("Project"), plan


def test_predicate_still_pushed_below_deterministic_projection(
    ray_start_regular_shared,
):
    ds = ray.data.range(10).with_column("k", col("id") + 1).filter(expr=col("id") > 4)
    plan = _optimized_plan_str(ds)
    assert plan.rfind("Filter") < plan.rfind("Project"), plan


def test_filter_with_nonidempotent_predicate_not_pushed_into_union_branches(
    ray_start_regular_shared,
):
    # A filter whose own predicate is non-idempotent must not be duplicated into
    # each Union branch (each branch would compute a separate local id).
    union = ray.data.range(10).union(ray.data.range(10))
    ds = union.filter(expr=monotonically_increasing_id() < 5)
    plan = _optimized_plan_str(ds)
    assert plan.rfind("Filter") > plan.rfind("Union"), plan


def test_idempotent_filter_still_pushed_into_union_branches(ray_start_regular_shared):
    union = ray.data.range(10).union(ray.data.range(10))
    ds = union.filter(expr=col("id") > 2)
    plan = _optimized_plan_str(ds)
    # An idempotent predicate is still pushed below the Union, into the branches.
    assert plan.rfind("Union") > plan.rfind("Filter"), plan


def test_filter_with_nonidempotent_predicate_not_fused(ray_start_regular_shared):
    # Fusing would move where monotonically_increasing_id() is evaluated.
    ds = (
        ray.data.range(20)
        .filter(expr=monotonically_increasing_id() < 15)
        .filter(expr=col("id") > 2)
    )
    plan = _optimized_plan_str(ds)
    assert plan.count("Filter[") == 2, plan


# ---------------------------------------------------------------------------
# LimitPushdown: a limit must not be pushed past a projection that produces a
# non-idempotent column.
# ---------------------------------------------------------------------------


def test_limit_not_pushed_past_nonidempotent_projection(ray_start_regular_shared):
    ds = ray.data.range(100).with_column("rid", monotonically_increasing_id()).limit(10)
    plan = _optimized_plan_str(ds)
    assert plan.rfind("Limit") > plan.rfind("Project"), plan


def test_limit_still_pushed_past_deterministic_projection(ray_start_regular_shared):
    ds = ray.data.range(100).with_column("k", col("id") + 1).limit(10)
    plan = _optimized_plan_str(ds)
    assert plan.rfind("Limit") < plan.rfind("Project"), plan


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
