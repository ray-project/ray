from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator, TaskContext
from ray.data._internal.execution.operators.hash_aggregate import (
    HashAggregateOperator,
)
from ray.data._internal.execution.operators.hash_aggregate_v2 import (
    _make_complete_aggregating_block_fn,
)
from ray.data._internal.execution.operators.shuffle_operators.disk_shuffle_reduce_operator import (  # noqa: E501
    DiskHashShuffleReduceOp,
)
from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator import (  # noqa: E501
    ShuffleReduceOp,
)
from ray.data._internal.logical.interfaces import (
    ClusteredDistribution,
    HashPartitioning,
)
from ray.data._internal.logical.operators import Project
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data.aggregate import AbsMax, AggregateFnV2, Count, CountDistinct, Max, Sum
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.dataset import Dataset
from ray.data.expressions import col
from ray.data.grouped_data import GroupedData
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403

NUM_PARTITIONS = 4
NUM_ROWS = 120
# Two-sided, so PredicatePushdown keeps a filter on it above the join.
TWO_SIDED_PREDICATE = (col("lval") + col("rval")) > 100


@pytest.fixture(autouse=True)
def shuffle_v2(restore_data_context: DataContext) -> DataContext:
    ctx = restore_data_context
    ctx.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    # One map task per bundle, so each reduce task sees several shards.
    ctx.shuffle_input_batch_bytes = 0
    return ctx


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _from_columns(**columns: np.ndarray) -> Dataset:
    """Six Arrow blocks with a plan-time-known schema, as a Parquet read has."""
    table = pa.table(columns)
    chunks = np.array_split(np.arange(table.num_rows), 6)
    return ray.data.from_arrow([table.take(idx) for idx in chunks])


def _left() -> Dataset:
    ids = np.arange(NUM_ROWS)
    return _from_columns(id=ids, grp=ids % 4, lval=ids * 2)


def _right() -> Dataset:
    # Two rows per key, so every group has several rows to aggregate.
    ids = np.arange(2 * NUM_ROWS)
    return _from_columns(rid=ids % NUM_ROWS, rval=ids * 3)


def _join(right: Optional[Dataset] = None, join_type: str = "inner") -> Dataset:
    """``_left()`` joined to ``right`` (default ``_right()``) on ``id = rid``."""
    return _left().join(
        _right() if right is None else right,
        on=("id",),
        right_on=("rid",),
        join_type=join_type,
        num_partitions=NUM_PARTITIONS,
    )


def _physical_dag(ds: Dataset) -> PhysicalOperator:
    return get_execution_plan(ds._logical_plan)[0].dag


def _op_names(dag: PhysicalOperator) -> List[str]:
    return [op.name for op in dag.post_order_iter()]


def _has_aggregate_shuffle(dag: PhysicalOperator) -> bool:
    return any("HashAggregateMap" in name for name in _op_names(dag))


def _pandas_groupby(
    ds: Dataset, keys: List[str], **aggs: Tuple[str, str]
) -> pd.DataFrame:
    """Reference result: ``ds`` materialized, then aggregated in pandas."""
    return ds.to_pandas().groupby(keys).agg(**aggs).reset_index()


def _assert_same_rows(
    ds: Dataset, expected: pd.DataFrame, sort_keys: List[str]
) -> None:
    actual = ds.to_pandas()
    assert set(actual.columns) == set(expected.columns), (
        actual.columns,
        expected.columns,
    )
    actual = actual[expected.columns].sort_values(sort_keys).reset_index(drop=True)
    expected = expected.sort_values(sort_keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)


def _assert_fused_aggregate(
    dag: PhysicalOperator,
    *aggregate_key_columns: Tuple[str, ...],
    contains: Tuple[str, ...] = (),
) -> None:
    """Assert the aggregation(s) run inside the join's reduce, the DAG's root.

    Ops in between are only checked via ``contains``; pushdown may add a Project.
    """
    reduce_ops = [
        op
        for op in dag.post_order_iter()
        if isinstance(op, (ShuffleReduceOp, DiskHashShuffleReduceOp))
    ]
    assert len(reduce_ops) == 1 and reduce_ops[0] is dag, _op_names(dag)
    expected_tail = "".join(
        f"->HashAggregate(key_columns={keys}, mode=complete)"
        for keys in aggregate_key_columns
    )
    assert dag.name.endswith(expected_tail), dag.name
    for part in contains:
        assert part in dag.name, dag.name


# ---------------------------------------------------------------------------
# Partitioning / distribution properties
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "partitioning_columns,required_columns,satisfied",
    [
        (("a",), ("a",), True),
        (("a",), ("a", "b"), True),
        (("b", "a"), ("a", "b", "c"), True),
        (("a", "b"), ("a",), False),
        (("a",), ("b",), False),
        (None, ("a",), False),
    ],
)
def test_clustered_distribution_satisfaction(
    partitioning_columns: Optional[Tuple[str, ...]],
    required_columns: Tuple[str, ...],
    satisfied: bool,
):
    partitioning = (
        HashPartitioning(partitioning_columns, 8) if partitioning_columns else None
    )
    assert ClusteredDistribution(required_columns).is_satisfied_by(partitioning) == (
        satisfied
    )


@pytest.mark.parametrize(
    "join_type,expected_columns",
    [
        ("inner", ("id",)),
        ("left_outer", ("id",)),
        ("full_outer", ("id",)),
        ("left_semi", ("id",)),
        ("left_anti", ("id",)),
        # Right-preserving joins coalesce into the right keys; not tracked.
        ("right_outer", None),
        ("right_semi", None),
        ("right_anti", None),
    ],
)
def test_join_output_partitioning(
    ray_start_regular_shared_2_cpus,
    join_type: str,
    expected_columns: Optional[Tuple[str, ...]],
):
    ds = _join(join_type=join_type)
    expected = (
        HashPartitioning(expected_columns, NUM_PARTITIONS) if expected_columns else None
    )
    assert _physical_dag(ds).output_partitioning == expected
    # The partitioning columns are real output columns.
    assert set(expected_columns or ()) <= set(ds.schema().names)


def test_aggregate_output_partitioning(ray_start_regular_shared_2_cpus):
    ds = _left().groupby(["grp", "id"]).aggregate(Sum(on="lval", alias_name="s"))
    assert ds._logical_plan.dag.required_input_distribution == (
        ClusteredDistribution(("grp", "id"))
    )
    assert _physical_dag(ds).output_partitioning == HashPartitioning(
        ("grp", "id"), DataContext.get_current().default_hash_shuffle_parallelism
    )


@pytest.mark.parametrize(
    "transform,expected_columns",
    [
        pytest.param(
            lambda ds: ds.select_columns(["id", "lval"]), ("id",), id="select"
        ),
        # Renames aren't tracked.
        pytest.param(lambda ds: ds.rename_columns({"id": "key"}), None, id="rename"),
        pytest.param(
            lambda ds: ds.with_column("x", col("lval") + 1), ("id",), id="add_column"
        ),
        pytest.param(
            lambda ds: ds.with_columns({"key": col("id"), "y": col("lval")}),
            ("id",),
            id="aliased_copy_keeps_identity",
        ),
        pytest.param(
            lambda ds: ds.filter(expr=TWO_SIDED_PREDICATE), ("id",), id="filter"
        ),
        pytest.param(
            lambda ds: ds.filter(lambda row: row["lval"] > 10), ("id",), id="udf_filter"
        ),
        pytest.param(
            lambda ds: ds.with_column("id", col("id") + 1), None, id="overwrite_key"
        ),
        pytest.param(lambda ds: ds.select_columns(["lval"]), None, id="drop_key"),
        pytest.param(
            lambda ds: ds.with_columns({"id": col("lval")}),
            None,
            id="other_column_takes_key_name",
        ),
        pytest.param(lambda ds: ds.map_batches(lambda b: b), None, id="map_batches"),
        pytest.param(lambda ds: ds.map(lambda r: r), None, id="map_rows"),
    ],
)
def test_map_output_partitioning(
    ray_start_regular_shared_2_cpus,
    transform: Callable[[Dataset], Dataset],
    expected_columns: Optional[Tuple[str, ...]],
):
    """Only filters and key-preserving projections keep the partitioning."""
    expected = (
        HashPartitioning(expected_columns, NUM_PARTITIONS) if expected_columns else None
    )
    assert _physical_dag(transform(_join())).output_partitioning == expected


def test_project_preserves_columns_without_schema(
    ray_start_regular_shared_2_cpus,
):
    """With an unknown input schema, columns pass through ``star()``."""
    opaque = _left().map_batches(lambda b: b)
    assert opaque.schema(fetch_if_missing=False) is None

    project = opaque.with_column("x", col("lval") + 1)._logical_plan.dag
    assert isinstance(project, Project)
    assert project.preserves_columns(["id", "grp"])

    project = opaque.with_column("id", col("id") + 1)._logical_plan.dag
    assert not project.preserves_columns(["id"])

    project = opaque.rename_columns({"id": "key"})._logical_plan.dag
    assert not project.preserves_columns(["id"])
    assert project.preserves_columns(["grp"])


# ---------------------------------------------------------------------------
# Complete (one-phase) aggregation of a partition
# ---------------------------------------------------------------------------


def _partition_blocks() -> List[Block]:
    """One partition's blocks: mixed formats, one empty, groups 1 and 2 split
    across blocks."""
    return [
        pa.table({"k": [1, 2], "v": [1, 2]}),
        pa.table({"k": pa.array([], pa.int64()), "v": pa.array([], pa.int64())}),
        pd.DataFrame({"k": [2, 3], "v": [10, 20]}),
        pa.table({"k": [1], "v": [100]}),
    ]


@pytest.mark.parametrize(
    "aggs,expected",
    [
        pytest.param(
            (Sum(on="v", alias_name="s"), Count(alias_name="c")),
            {"s": [101, 12, 20], "c": [2, 2, 1]},
            id="vectorized",
        ),
        pytest.param(
            (CountDistinct(on="v", alias_name="d"),),
            {"d": [2, 2, 1]},
            id="vectorized_collection",
        ),
        # ``AbsMax`` has no Arrow kernel, forcing the Python engine.
        pytest.param(
            (Sum(on="v", alias_name="s"), AbsMax(on="v", alias_name="m")),
            {"s": [101, 12, 20], "m": [100, 10, 20]},
            id="python",
        ),
    ],
)
def test_complete_aggregation_of_a_partition(
    aggs: Tuple[AggregateFnV2, ...], expected: Dict[str, List[int]]
):
    """One row per group, from all of the group's blocks."""
    aggregate = _make_complete_aggregating_block_fn(("k",), aggs)
    out = pa.concat_tables(
        aggregate(iter(_partition_blocks()), TaskContext(task_idx=0, op_name="test"))
    )
    assert out.sort_by("k").to_pydict() == {"k": [1, 2, 3], **expected}
    # No rows means no groups and no output block.
    assert list(aggregate(iter(_partition_blocks()[1:2]), TaskContext(0, "test"))) == []


class _SumThatCannotCombine(AggregateFnV2):
    """A sum that fails if two partial aggregates are ever combined."""

    def __init__(self):
        super().__init__("s", zero_factory=lambda: 0, on="v", ignore_nulls=True)

    def aggregate_block(self, block: Block) -> int:
        return pc.sum(BlockAccessor.for_block(block).to_arrow()["v"]).as_py()

    def combine(self, current_accumulator: int, new: int) -> int:
        raise AssertionError("combined partial aggregates of one group")


def test_complete_aggregation_aggregates_each_group_once():
    """A group split across blocks is aggregated once, not per block."""
    aggregate = _make_complete_aggregating_block_fn(("k",), (_SumThatCannotCombine(),))
    out = pa.concat_tables(
        aggregate(iter(_partition_blocks()), TaskContext(task_idx=0, op_name="test"))
    )
    assert out.sort_by("k").to_pydict() == {"k": [1, 2, 3], "s": [101, 12, 20]}


# ---------------------------------------------------------------------------
# Shuffle elimination
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("use_disk", [False, True])
def test_groupby_after_join_skips_shuffle(
    ray_start_regular_shared_2_cpus, shuffle_v2: DataContext, use_disk: bool
):
    """TPC-H q3 shape: join, derive a column, group by a superset of the key."""
    shuffle_v2.use_disk_based_hash_shuffle = use_disk
    before_agg = _join().with_column("rev", col("lval") * col("rval"))
    ds = before_agg.groupby(["id", "grp"]).aggregate(
        Sum(on="rev", alias_name="rev"),
        Max(on="rval", alias_name="m"),
        Count(alias_name="c"),
    )

    dag = _physical_dag(ds)
    _assert_fused_aggregate(dag, ("id", "grp"), contains=("->Project->",))
    assert isinstance(dag, DiskHashShuffleReduceOp) == use_disk
    # The aggregated output keeps the join's layout.
    assert dag.output_partitioning == HashPartitioning(("id",), NUM_PARTITIONS)
    assert dag.num_output_rows_total() is None

    out = ds.materialize()
    # One output block per non-empty partition, with no second shuffle.
    assert out.num_blocks() <= NUM_PARTITIONS
    expected = _pandas_groupby(
        before_agg,
        ["id", "grp"],
        rev=("rev", "sum"),
        m=("rval", "max"),
        c=("rval", "size"),
    )
    _assert_same_rows(out, expected, ["id", "grp"])


@pytest.mark.parametrize(
    "transform,skips_shuffle",
    [
        pytest.param(lambda ds: ds.groupby("id"), True, id="join_key"),
        pytest.param(
            lambda ds: ds.groupby("id", num_partitions=NUM_PARTITIONS),
            True,
            id="same_num_partitions",
        ),
        pytest.param(lambda ds: ds.groupby("grp"), False, id="other_key"),
        pytest.param(
            lambda ds: ds.with_column("id", col("id") % 3).groupby("id"),
            False,
            id="key_overwritten",
        ),
        pytest.param(
            lambda ds: ds.groupby("id", num_partitions=NUM_PARTITIONS + 1),
            False,
            id="different_num_partitions",
        ),
        pytest.param(
            lambda ds: ds.filter(expr=TWO_SIDED_PREDICATE, num_cpus=0.5).groupby("id"),
            False,
            id="map_with_incompatible_remote_args",
        ),
        pytest.param(
            lambda ds: ds.filter(expr=TWO_SIDED_PREDICATE, concurrency=1).groupby("id"),
            False,
            id="map_with_concurrency_cap",
        ),
    ],
)
def test_aggregate_shuffle_elimination(
    ray_start_regular_shared_2_cpus,
    transform: Callable[[Dataset], GroupedData],
    skips_shuffle: bool,
):
    grouped = transform(_join())
    ds = grouped.aggregate(Sum(on="rval", alias_name="s"))
    assert _has_aggregate_shuffle(_physical_dag(ds)) != skips_shuffle

    # Reference: the same pipeline minus the aggregation, aggregated in pandas.
    keys = grouped._key if isinstance(grouped._key, list) else [grouped._key]
    expected = _pandas_groupby(grouped._dataset, keys, s=("rval", "sum"))
    _assert_same_rows(ds, expected, keys)


def test_left_outer_join_then_count_skips_shuffle(ray_start_regular_shared_2_cpus):
    """TPC-H q13 shape: left join then count; unmatched keys must count 0."""
    joined = _join(_right().filter(expr=col("rid") < 60), join_type="left_outer")
    ds = joined.groupby("id").aggregate(
        Count(on="rval", ignore_nulls=True, alias_name="c")
    )
    assert not _has_aggregate_shuffle(_physical_dag(ds))

    expected = _pandas_groupby(joined, ["id"], c=("rval", "count"))
    assert (expected["c"] == 0).sum() == 60
    _assert_same_rows(ds, expected, ["id"])


def test_chained_groupby_skips_both_shuffles(ray_start_regular_shared_2_cpus):
    """Both groupbys, and the maps before them, fold into the join's reduce."""
    before_agg = (
        _join().filter(expr=TWO_SIDED_PREDICATE).with_column("w", col("rval") * 2)
    )
    ds = (
        before_agg.groupby("id")
        .aggregate(Sum(on="w", alias_name="s"))
        .groupby(["id", "s"])
        .aggregate(Count(alias_name="c"))
    )
    _assert_fused_aggregate(
        _physical_dag(ds),
        ("id",),
        ("id", "s"),
        contains=("->Filter(", "->Project->"),
    )

    expected = _pandas_groupby(before_agg, ["id"], s=("w", "sum"))
    expected["c"] = 1
    _assert_same_rows(ds, expected, ["id"])


def test_non_vectorized_aggregation_skips_shuffle(ray_start_regular_shared_2_cpus):
    """``AbsMax`` has no Arrow kernel, forcing the Python fallback path."""
    joined = _join()
    ds = joined.groupby("id").aggregate(
        Sum(on="rval", alias_name="s"), AbsMax(on="rval", alias_name="am")
    )
    assert not _has_aggregate_shuffle(_physical_dag(ds))

    expected = _pandas_groupby(joined, ["id"], s=("rval", "sum"), am=("rval", "max"))
    _assert_same_rows(ds, expected, ["id"])


def test_empty_join_output(ray_start_regular_shared_2_cpus):
    joined = _join(_right().filter(expr=col("rid") > 10_000))
    ds = joined.groupby("id").aggregate(Count(alias_name="c"))
    assert not _has_aggregate_shuffle(_physical_dag(ds))
    assert ds.take_all() == []


def test_downstream_map_after_eliminated_shuffle(ray_start_regular_shared_2_cpus):
    joined = _join()
    ds = (
        joined.groupby("id")
        .aggregate(Sum(on="rval", alias_name="s"))
        .with_column("s2", col("s") * 2)
    )
    assert not _has_aggregate_shuffle(_physical_dag(ds))

    expected = _pandas_groupby(joined, ["id"], s=("rval", "sum"))
    expected["s2"] = expected["s"] * 2
    _assert_same_rows(ds, expected, ["id"])


def test_global_aggregation_keeps_shuffle(ray_start_regular_shared_2_cpus):
    joined = _join()
    ds = joined.groupby(None).aggregate(Sum(on="rval", alias_name="s"))
    # Needs all rows in one place, whatever the input's layout.
    assert ds._logical_plan.dag.required_input_distribution is None
    dag = _physical_dag(ds)
    assert _has_aggregate_shuffle(dag)
    assert dag.output_partitioning is None
    assert ds.take_all() == [{"s": joined.to_pandas()["rval"].sum()}]


def test_legacy_hash_shuffle_strategy_is_untouched(
    ray_start_regular_shared_2_cpus, shuffle_v2: DataContext
):
    shuffle_v2.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    joined = _join()
    ds = joined.groupby("id").aggregate(Sum(on="rval", alias_name="s"))
    assert isinstance(_physical_dag(ds), HashAggregateOperator)
    _assert_same_rows(ds, _pandas_groupby(joined, ["id"], s=("rval", "sum")), ["id"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
