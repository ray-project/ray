from typing import Callable, List, Optional, Tuple
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator, TaskContext
from ray.data._internal.execution.interfaces.op_runtime_metrics import (
    OpRuntimeMetrics,
)
from ray.data._internal.execution.operators.hash_aggregate_v2 import (
    COMPLETE_AGGREGATION_PEAK_MEMORY_MULTIPLIER,
    _make_complete_aggregating_block_fn,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical import optimizers
from ray.data._internal.logical.interfaces import (
    ClusteredDistribution,
    HashPartitioning,
    LogicalPlan,
)
from ray.data._internal.logical.operators import Aggregate, Project
from ray.data._internal.logical.optimizers import (
    LogicalOptimizer,
    get_execution_plan,
    get_physical_ruleset,
)
from ray.data._internal.logical.rules import (
    EliminateAggregateShuffle,
    EnsureLocalAggregateReadsWholePartitions,
)
from ray.data._internal.logical.ruleset import Ruleset
from ray.data._internal.planner import create_planner
from ray.data.aggregate import (
    AbsMax,
    AggregateFn,
    Count,
    CountDistinct,
    Max,
    Mean,
    Sum,
    Unique,
)
from ray.data.context import DataContext, ShuffleStrategy
from ray.data.dataset import Dataset
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa: F401, F403
from ray.tests.conftest import *  # noqa: F401, F403

NUM_PARTITIONS = 4
NUM_ROWS = 120
# Reads both sides, so PredicatePushdown can't push it below the join.
TWO_SIDED_PREDICATE = (col("lval") + col("rval")) > 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _from_columns(**columns: np.ndarray) -> Dataset:
    """Six Arrow blocks whose schema is known at plan time, like a Parquet read."""
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


def _planned_dag(ds: Dataset) -> PhysicalOperator:
    """The physical DAG before ``FuseOperators``, which drops the partitioning."""
    logical_plan = LogicalOptimizer().optimize(ds._logical_plan)
    assert isinstance(logical_plan, LogicalPlan)
    physical_plan, _ = create_planner().plan(logical_plan)
    return physical_plan.dag


def _physical_dag(ds: Dataset) -> PhysicalOperator:
    """The physical DAG after every physical rule."""
    physical_plan, _ = get_execution_plan(ds._logical_plan)
    return physical_plan.dag


def _skips_aggregate_shuffle(ds: Dataset) -> bool:
    return any("mode=complete" in op.name for op in _physical_dag(ds).post_order_iter())


def _sum_every_column(
    ds: Dataset, keys: List[str], num_partitions: Optional[int] = None
) -> Tuple[Dataset, pd.DataFrame]:
    """Groups ``ds`` by ``keys``, sums every other column, and returns the
    expected rows too. Reading every column keeps projection pushdown from
    adding a Project."""
    values = [c for c in ds.schema().names if c not in keys]
    out = ds.groupby(keys, num_partitions=num_partitions).aggregate(
        *[Sum(on=c, alias_name=c) for c in values]
    )
    expected = ds.to_pandas().groupby(keys, dropna=False)[values].sum().reset_index()
    return out, expected


def _assert_same_rows(ds: Dataset, expected: pd.DataFrame, keys: List[str]) -> None:
    actual = ds.to_pandas()
    assert sorted(actual.columns) == sorted(expected.columns)
    actual = actual[expected.columns].sort_values(keys).reset_index(drop=True)
    expected = expected.sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(actual, expected, check_dtype=False)


def _expected_partitioning(
    shuffle_strategy: ShuffleStrategy,
    columns: Optional[Tuple[str, ...]],
    num_partitions: int,
) -> Optional[HashPartitioning]:
    """Only SHUFFLE_V2 operators declare their output partitioning."""
    if shuffle_strategy != ShuffleStrategy.SHUFFLE_V2 or columns is None:
        return None
    return HashPartitioning(columns, num_partitions)


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
    partitioning = None
    if partitioning_columns is not None:
        partitioning = HashPartitioning(partitioning_columns, num_partitions=8)
    distribution = ClusteredDistribution(required_columns)
    assert distribution.is_satisfied_by(partitioning) == satisfied


@pytest.mark.parametrize(
    "join_type,expected_columns",
    [
        ("inner", ("id",)),
        ("left_outer", ("id",)),
        ("full_outer", ("id",)),
        ("left_semi", ("id",)),
        ("left_anti", ("id",)),
        # These joins keep the right key's name, which isn't tracked.
        ("right_outer", None),
        ("right_semi", None),
        ("right_anti", None),
    ],
)
def test_join_output_partitioning(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    join_type: str,
    expected_columns: Optional[Tuple[str, ...]],
):
    ds = _join(join_type=join_type)
    expected = _expected_partitioning(
        configure_shuffle_method, expected_columns, NUM_PARTITIONS
    )
    assert _planned_dag(ds).output_partitioning == expected
    # The partitioning columns are real output columns.
    if expected_columns is not None:
        assert set(expected_columns).issubset(ds.schema().names)


def test_aggregate_output_partitioning(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    ds = _left().groupby(["grp", "id"]).aggregate(Sum(on="lval", alias_name="s"))
    aggregate = ds._logical_plan.dag
    assert isinstance(aggregate, Aggregate)
    assert aggregate.required_input_distribution == (
        ClusteredDistribution(("grp", "id"))
    )
    expected = _expected_partitioning(
        configure_shuffle_method,
        ("grp", "id"),
        DataContext.get_current().default_hash_shuffle_parallelism,
    )
    assert _planned_dag(ds).output_partitioning == expected


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
    configure_shuffle_method,
    transform: Callable[[Dataset], Dataset],
    expected_columns: Optional[Tuple[str, ...]],
):
    """Only filters and key-preserving projections keep the partitioning."""
    expected = _expected_partitioning(
        configure_shuffle_method, expected_columns, NUM_PARTITIONS
    )
    assert _planned_dag(transform(_join())).output_partitioning == expected


def _project(ds: Dataset) -> Project:
    project = ds._logical_plan.dag
    assert isinstance(project, Project)
    return project


def test_project_preserves_columns_without_schema(
    ray_start_regular_shared_2_cpus,
):
    """With an unknown input schema, columns pass through ``star()``."""
    opaque = _left().map_batches(lambda b: b)
    assert opaque.schema(fetch_if_missing=False) is None

    project = _project(opaque.with_column("x", col("lval") + 1))
    assert project.preserves_columns(["id", "grp"])

    project = _project(opaque.with_column("id", col("id") + 1))
    assert not project.preserves_columns(["id"])

    project = _project(opaque.rename_columns({"id": "key"}))
    assert not project.preserves_columns(["id"])
    assert project.preserves_columns(["grp"])


# ---------------------------------------------------------------------------
# Aggregate shuffle elimination
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "before_agg,keys,num_partitions,skips_shuffle",
    [
        pytest.param(_join, ["id"], None, True, id="join_key"),
        pytest.param(_join, ["id", "grp"], None, True, id="superset_of_join_key"),
        pytest.param(_join, ["id"], NUM_PARTITIONS, True, id="same_num_partitions"),
        pytest.param(_join, ["grp"], None, False, id="other_key"),
        pytest.param(
            _join, ["id"], NUM_PARTITIONS + 1, False, id="different_num_partitions"
        ),
        # Keeps the right key, rid, which isn't tracked.
        pytest.param(
            lambda: _join(join_type="right_outer"),
            ["rid"],
            None,
            False,
            id="right_outer",
        ),
        pytest.param(
            lambda: _join().with_column("id", col("id") % 3),
            ["id"],
            None,
            False,
            id="key_overwritten",
        ),
        pytest.param(
            lambda: _join().rename_columns({"id": "key"}),
            ["key"],
            None,
            False,
            id="key_renamed",
        ),
        # Maps that keep the key. FuseOperators fuses them into the join's
        # reduce, which then sends whole partitions to the aggregation.
        pytest.param(
            lambda: _join().with_column("x", col("lval") * col("rval")),
            ["id"],
            None,
            True,
            id="with_column",
        ),
        pytest.param(
            lambda: _join().select_columns(["id", "lval"]),
            ["id"],
            None,
            True,
            id="select_columns",
        ),
        pytest.param(
            lambda: _join().filter(expr=TWO_SIDED_PREDICATE),
            ["id"],
            None,
            True,
            id="filter",
        ),
    ],
)
def test_aggregate_shuffle_elimination(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    before_agg: Callable[[], Dataset],
    keys: List[str],
    num_partitions: Optional[int],
    skips_shuffle: bool,
):
    """Only SHUFFLE_V2 skips the shuffle, and every strategy gives the same rows."""
    ds, expected = _sum_every_column(before_agg(), keys, num_partitions)
    assert _skips_aggregate_shuffle(ds) == (
        skips_shuffle and configure_shuffle_method == ShuffleStrategy.SHUFFLE_V2
    )
    _assert_same_rows(ds, expected, keys)


def test_unsupported_aggregation_keeps_shuffle(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    """Collection aggregations keep every raw row, so the shuffle stays."""
    keys = ["id", "grp"]
    joined = _join()
    ds = joined.groupby(keys).aggregate(
        CountDistinct(on="lval", alias_name="lval"),
        CountDistinct(on="rval", alias_name="rval"),
    )
    assert not _skips_aggregate_shuffle(ds)
    expected = (
        joined.to_pandas()
        .groupby(keys)
        .agg(lval=("lval", "nunique"), rval=("rval", "nunique"))
        .reset_index()
    )
    _assert_same_rows(ds, expected, keys)


def test_partition_spread_over_many_blocks(
    ray_start_regular_shared_2_cpus, configure_shuffle_method, restore_data_context
):
    """Each aggregation task gets a whole partition, however many blocks it spans."""
    DataContext.get_current().target_max_block_size = 256
    joined = _join()
    assert joined.materialize().num_blocks() > 4 * NUM_PARTITIONS

    ds, expected = _sum_every_column(joined, ["id"])
    ds = ds.materialize()
    _assert_same_rows(ds, expected, ["id"])
    if configure_shuffle_method == ShuffleStrategy.SHUFFLE_V2:
        # One aggregation task per join partition.
        (aggregate_stats,) = [
            stats
            for stats in ds._get_stats_summary().operators_stats
            if "mode=complete" in stats.operator_name
        ]
        assert aggregate_stats.task_rows.count == NUM_PARTITIONS


def test_left_outer_join_then_count(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    """TPC-H q13 shape: keys without a match on the right count as 0."""
    joined = _join(join_type="left_outer", right=_right().filter(expr=col("rid") < 60))
    # Reads every column, so no Project to drop columns sits before the group-by.
    ds = joined.groupby("id").aggregate(
        Count(on="rval", ignore_nulls=True, alias_name="c"),
        Max(on="grp", alias_name="grp"),
        Sum(on="lval", alias_name="lval"),
    )
    assert _skips_aggregate_shuffle(ds) == (
        configure_shuffle_method == ShuffleStrategy.SHUFFLE_V2
    )
    expected = (
        joined.to_pandas()
        .groupby("id")
        .agg(c=("rval", "count"), grp=("grp", "max"), lval=("lval", "sum"))
        .reset_index()
    )
    # The right side keeps rid < 60, so the other 60 ids have no match.
    assert expected["c"].eq(0).sum() == 60
    _assert_same_rows(ds, expected, ["id"])


def test_groupby_after_groupby_skips_second_shuffle(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    """A group-by's reduce declares its partitioning too."""
    first = _left().groupby("grp").aggregate(Sum(on="lval", alias_name="s"))
    ds = first.groupby(["grp", "s"]).aggregate(Count(alias_name="c"))
    is_v2 = configure_shuffle_method == ShuffleStrategy.SHUFFLE_V2
    assert _skips_aggregate_shuffle(ds) == is_v2
    if is_v2:
        assert _physical_dag(ds).name == (
            "HashAggregate(key_columns=('grp', 's'), mode=complete)"
        )

    expected = _left().to_pandas().groupby("grp").agg(s=("lval", "sum")).reset_index()
    expected["c"] = 1
    _assert_same_rows(ds, expected, ["grp"])


def test_groupby_after_skipped_shuffle_keeps_its_shuffle(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    """The one-phase aggregation declares no partitioning, so a later group-by
    keeps its shuffle."""
    if configure_shuffle_method == ShuffleStrategy.HASH_SHUFFLE:
        pytest.skip("Three pools of shuffle aggregator actors don't fit in 2 CPUs.")
    first, first_expected = _sum_every_column(_join(), ["id"])
    ds = first.groupby(["id", "grp", "lval", "rval"]).aggregate(Count(alias_name="c"))
    complete_ops = [
        op for op in _physical_dag(ds).post_order_iter() if "mode=complete" in op.name
    ]
    if configure_shuffle_method == ShuffleStrategy.SHUFFLE_V2:
        (complete_op,) = complete_ops
        assert complete_op.name.startswith("HashAggregate(key_columns=('id',)")
    else:
        assert complete_ops == []

    first_expected["c"] = 1
    _assert_same_rows(ds, first_expected, ["id"])


def test_map_after_one_phase_aggregation(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    """The one-phase aggregation doesn't fuse, so a map after it runs in its
    own tasks."""
    agg, expected = _sum_every_column(_join(), ["id"])
    ds = agg.with_column("lval2", col("lval") * 2)
    if configure_shuffle_method == ShuffleStrategy.SHUFFLE_V2:
        dag = _physical_dag(ds)
        assert dag.name == "Project"
        (aggregate,) = dag.input_dependencies
        assert aggregate.name == "HashAggregate(key_columns=('id',), mode=complete)"

    expected["lval2"] = expected["lval"] * 2
    _assert_same_rows(ds, expected, ["id"])


@pytest.mark.parametrize(
    "bytes_per_task, expected_memory",
    [(1000, 1000 * COMPLETE_AGGREGATION_PEAK_MEMORY_MULTIPLIER), (None, None)],
)
def test_one_phase_aggregation_memory_request(
    ray_start_regular_shared_2_cpus,
    restore_data_context,
    bytes_per_task: Optional[int],
    expected_memory: Optional[int],
):
    """The request scales with a task's whole output; no estimate until a task
    finishes."""
    restore_data_context.shuffle_strategy = ShuffleStrategy.SHUFFLE_V2
    ds, _ = _sum_every_column(_join(), ["id"])
    aggregate = _physical_dag(ds)
    assert isinstance(aggregate, MapOperator)
    assert aggregate.name == "HashAggregate(key_columns=('id',), mode=complete)"
    aggregate._metrics = MagicMock(
        spec=OpRuntimeMetrics,
        average_bytes_per_output=1,
        average_bytes_outputs_per_task=bytes_per_task,
    )
    assert aggregate._get_dynamic_ray_remote_args().get("memory") == expected_memory


def _filter_join_sum_add_column() -> Dataset:
    joined = _join().filter(expr=TWO_SIDED_PREDICATE, concurrency=1)
    ds, _ = _sum_every_column(joined, ["id"])
    return ds.with_column("lval2", col("lval") * 2)


@pytest.mark.parametrize(
    "build",
    [
        # The filter runs in its own tasks. The map after the group-by fuses
        # into the restored reduce.
        pytest.param(_filter_join_sum_add_column, id="map_between_join_and_groupby"),
        # TPC-H q18 shape: with a limit downstream, FuseOperators doesn't fuse
        # into the reduce, so the select stays between the join and the group-by.
        pytest.param(
            lambda: _join()
            .select_columns(["id", "rval"])
            .groupby("id")
            .aggregate(Sum(on="rval", alias_name="s"))
            .sort(["s", "id"])
            .limit(5),
            id="downstream_limit",
        ),
    ],
)
def test_restored_shuffle_is_planned_as_without_elimination(
    ray_start_regular_shared_2_cpus,
    configure_shuffle_method,
    monkeypatch,
    build: Callable[[], Dataset],
):
    ds = build()
    names = [op.name for op in _physical_dag(ds).post_order_iter()]
    assert not any("mode=complete" in name for name in names)
    # Rows are dicts, which don't sort, so compare their string forms.
    rows = sorted(str(row) for row in ds.take_all())

    rules = (EliminateAggregateShuffle, EnsureLocalAggregateReadsWholePartitions)
    monkeypatch.setattr(
        optimizers,
        "_PHYSICAL_RULESET",
        Ruleset([rule for rule in get_physical_ruleset() if rule not in rules]),
    )
    ds = build()
    assert [op.name for op in _physical_dag(ds).post_order_iter()] == names
    assert sorted(str(row) for row in ds.take_all()) == rows


def test_empty_join_output(ray_start_regular_shared_2_cpus, configure_shuffle_method):
    joined = _join(right=_right().filter(expr=col("rid") > 10_000))
    ds, _ = _sum_every_column(joined, ["id"])
    assert _skips_aggregate_shuffle(ds) == (
        configure_shuffle_method == ShuffleStrategy.SHUFFLE_V2
    )
    assert ds.take_all() == []


def test_global_aggregation_keeps_shuffle(ray_start_regular_shared_2_cpus):
    joined = _join()
    ds = joined.groupby(None).aggregate(Sum(on="rval", alias_name="s"))
    assert not _skips_aggregate_shuffle(ds)
    assert ds.take_all() == [{"s": joined.to_pandas()["rval"].sum()}]


@pytest.mark.parametrize(
    "key_columns,aggs,expected",
    [
        (("k",), (Sum(on="v"), Count(), Mean(on="v"), Max(on="v")), True),
        # Collection aggregations.
        (("k",), (CountDistinct(on="v"),), False),
        (("k",), (Unique(on="v"),), False),
        (("k",), (Sum(on="v"), CountDistinct(on="v")), False),
        # Python engine.
        (("k",), (AbsMax(on="v"),), False),
    ],
)
def test_complete_aggregation_support(
    key_columns: Tuple[str, ...], aggs: Tuple[AggregateFn, ...], expected: bool
):
    """Only Arrow-vectorized reductions aggregate in one phase."""
    supported = _make_complete_aggregating_block_fn(key_columns, aggs) is not None
    assert supported == expected


def test_complete_aggregating_block_fn():
    """Aggregates every block it's given into one row per group."""
    aggregate = _make_complete_aggregating_block_fn(
        ("k",),
        (Sum(on="v", alias_name="s"), Count(alias_name="c"), Sum(on="v")),
    )
    assert aggregate is not None
    blocks = [
        pa.table({"k": [1, 2], "v": [1, 2], "unused": ["a", "b"]}),
        pa.table(
            {
                "k": pa.array([], pa.int64()),
                "v": pa.array([], pa.int64()),
                "unused": pa.array([], pa.string()),
            }
        ),
        pd.DataFrame({"k": [2, 3], "v": [10, 20], "unused": ["c", "d"]}),
        pa.table({"k": [1], "v": [100], "unused": ["e"]}),
    ]
    ctx = TaskContext(task_idx=0, op_name="test")
    out = pa.concat_tables(aggregate(iter(blocks), ctx))
    assert out.sort_by("k").to_pydict() == {
        "k": [1, 2, 3],
        "s": [101, 12, 20],
        "c": [2, 2, 1],
        "sum(v)": [101, 12, 20],
    }
    # No rows means no groups and no block.
    assert list(aggregate(iter(blocks[1:2]), ctx)) == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
