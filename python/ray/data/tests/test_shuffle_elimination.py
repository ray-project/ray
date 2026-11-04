from typing import Callable, Optional, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator, TaskContext
from ray.data._internal.execution.operators.hash_aggregate_v2 import (
    _make_complete_aggregating_block_fn,
)
from ray.data._internal.logical.interfaces import (
    ClusteredDistribution,
    HashPartitioning,
    LogicalPlan,
)
from ray.data._internal.logical.operators import Aggregate, Project
from ray.data._internal.logical.optimizers import LogicalOptimizer
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
