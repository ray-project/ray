import pytest

import ray
from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_map_operator import (  # noqa: E501
    ExternalHashShuffleMapOp,
)
from ray.data._internal.execution.operators.shuffle_operators.external_shuffle_reduce_operator import (  # noqa: E501
    ExternalHashShuffleReduceOp,
)
from ray.data.aggregate import AbsMax, Count, Mean, Std, Sum
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _use_shuffle_v2(use_external: bool) -> None:
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ctx.use_external_hash_shuffle = use_external


def _collect_ops(dag):
    ops, stack = [], [dag]
    while stack:
        op = stack.pop()
        ops.append(op)
        stack.extend(op.input_dependencies)
    return ops


def _rows_match(actual, expected):
    assert len(actual) == len(expected)
    for a, e in zip(actual, expected):
        assert a.keys() == e.keys()
        for col in a:
            if isinstance(e[col], float):
                assert a[col] == pytest.approx(e[col]), col
            else:
                assert a[col] == e[col], col


def test_external_aggregate_planner_routing(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Flag-on routes Aggregate to the external op pair, with the map-side
    combiner installed and empty-partition placeholders disabled."""
    from ray.data._internal.logical.optimizers import get_execution_plan

    _use_shuffle_v2(True)

    dag = get_execution_plan(
        ray.data.range(10).groupby("id", num_partitions=2).sum("id")._logical_plan
    )[0].dag
    ops = _collect_ops(dag)

    reduce_ops = [op for op in ops if isinstance(op, ExternalHashShuffleReduceOp)]
    assert len(reduce_ops) == 1
    reduce_op = reduce_ops[0]
    assert reduce_op._emit_empty_partitions is False
    assert "ExternalHashAggregateReduce" in reduce_op.name

    map_op = reduce_op.input_dependencies[0]
    assert isinstance(map_op, ExternalHashShuffleMapOp)
    assert map_op._block_transformer is not None
    assert "ExternalHashAggregateMap" in map_op.name


def _grouped_agg_rows(use_external: bool):
    _use_shuffle_v2(use_external)
    ds = ray.data.range(1000, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 25, "v": row["id"] - 500}
    )
    # AbsMax routes the query through the reduce that merge-sorts
    # individually key-sorted mapper shards — catches transports that
    # coalesce shards (a concat of sorted shards is not sorted).
    rows = (
        ds.groupby("k", num_partitions=8)
        .aggregate(Count(), Sum("v"), Mean("v"), Std("v"), AbsMax("v"))
        .take_all()
    )
    return sorted(rows, key=lambda r: r["k"])


def test_external_groupby_aggregate_matches_object_store(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    external = _grouped_agg_rows(True)
    expected = _grouped_agg_rows(False)
    assert len(external) == 25
    _rows_match(external, expected)


def test_external_global_aggregate(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Global (keyless) aggregation reduces to a single partition."""
    _use_shuffle_v2(True)
    ds = ray.data.range(1000, override_num_blocks=10)
    assert ds.sum("id") == sum(range(1000))
    assert ds.mean("id") == pytest.approx(sum(range(1000)) / 1000)


def test_external_global_aggregate_empty_input_matches_object_store(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """Blocks exist but hold zero rows: the map-side combiner must still run
    on the empty input so the global aggregation emits its identity row."""

    def _run(use_external: bool):
        _use_shuffle_v2(use_external)
        ds = ray.data.range(100, override_num_blocks=4).filter(lambda row: False)
        return ds.sum("id"), ds.min("id")

    assert _run(True) == _run(False)


def test_external_aggregate_drops_empty_partitions(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
):
    """More partitions than groups: empty partitions emit nothing (no
    partial-schema placeholder blocks that would conflict with finalized
    partitions)."""
    _use_shuffle_v2(True)
    ds = ray.data.range(600, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 3, "v": row["id"]}
    )
    out = ds.groupby("k", num_partitions=50).sum("v").materialize()

    assert out.count() == 3
    assert out.num_blocks() <= 3

    rows = sorted(out.take_all(), key=lambda r: r["k"])
    assert [r["k"] for r in rows] == [0, 1, 2]
    assert rows[0]["sum(v)"] == sum(v for v in range(600) if v % 3 == 0)


def test_external_aggregate_into_write(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    disable_fallback_to_object_extension,
    tmp_path,
):
    """Aggregate feeding a Write (fusable into the reduce): empty partitions
    must not surface partial-schema blocks to the writer."""
    _use_shuffle_v2(True)
    ds = ray.data.range(600, override_num_blocks=10).map(
        lambda row: {"k": row["id"] % 3, "v": row["id"]}
    )
    ds.groupby("k", num_partitions=50).sum("v").write_parquet(str(tmp_path))

    out = ray.data.read_parquet(str(tmp_path))
    rows = sorted(out.take_all(), key=lambda r: r["k"])
    assert [r["k"] for r in rows] == [0, 1, 2]
    assert sum(r["sum(v)"] for r in rows) == sum(range(600))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
