"""Unit tests for Delta Lake add-action file pruning.

These tests cover ``prune_add_actions``, which decides -- from the Delta
transaction log alone -- which data files a query could possibly need. The
function is pure and needs no Ray cluster.

The overriding requirement is that pruning is *conservative*: it may keep a
file that turns out to hold no matching row, but it must never drop a file
that holds one. ``test_pruning_never_drops_a_matching_file`` asserts exactly
that property over randomized inputs.
"""

import random
from typing import List, Optional

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.listing.delta_file_pruning import (
    prune_add_actions,
)
from ray.data.expressions import Expr, col, lit

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _stats_actions() -> pa.Table:
    """Three files with disjoint, ascending ``val`` ranges.

        f1: val in [1, 5]    name in [a, c]
        f2: val in [6, 10]   name in [d, f]
        f3: val in [11, 15]  name in [g, i], 2 nulls in val
    """
    return pa.table(
        {
            "path": ["f1.parquet", "f2.parquet", "f3.parquet"],
            "size_bytes": [100, 100, 100],
            "num_records": [10, 10, 10],
            "min.val": [1, 6, 11],
            "max.val": [5, 10, 15],
            "null_count.val": [0, 0, 2],
            "min.name": ["a", "d", "g"],
            "max.name": ["c", "f", "i"],
            "null_count.name": [0, 0, 0],
        }
    )


def _partition_actions() -> pa.Table:
    """Four files across a ``region`` x ``year`` partition grid.

    Partition values are strings in the Delta log even when the column is
    typed otherwise -- ``year`` here is logically an int64. The values 9 and
    100 are chosen because their lexicographic and numeric orderings differ,
    so a missing cast shows up as a wrong answer rather than a coincidence.
    """
    return pa.table(
        {
            "path": ["p1.parquet", "p2.parquet", "p3.parquet", "p4.parquet"],
            "size_bytes": [10, 10, 10, 10],
            "num_records": [1, 1, 1, 1],
            "partition.region": ["US", "US", "EU", "EU"],
            "partition.year": ["9", "100", "9", "100"],
        }
    )


_PARTITION_SCHEMA = pa.schema(
    [
        pa.field("region", pa.string()),
        pa.field("year", pa.int64()),
        pa.field("val", pa.int64()),
    ]
)


def _paths(table: pa.Table) -> List[str]:
    return table.column("path").to_pylist()


# ---------------------------------------------------------------------------
# Statistics-based skipping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "predicate,expected,description",
    [
        # --- simple comparisons, both operand orders -------------------
        (col("val") > lit(10), ["f3.parquet"], "gt drops files whose max <= v"),
        (lit(10) < col("val"), ["f3.parquet"], "gt with literal on the left"),
        (
            col("val") >= lit(10),
            ["f2.parquet", "f3.parquet"],
            "ge keeps the file whose max == v",
        ),
        (col("val") < lit(6), ["f1.parquet"], "lt drops files whose min >= v"),
        (
            col("val") <= lit(5),
            ["f1.parquet"],
            "le keeps the file whose min == v",
        ),
        (lit(5) >= col("val"), ["f1.parquet"], "le with literal on the left"),
        (
            col("val") == lit(7),
            ["f2.parquet"],
            "eq keeps only the file whose range covers v",
        ),
        (
            col("val") == lit(100),
            [],
            "eq outside every range prunes everything",
        ),
        # --- provably unprunable ---------------------------------------
        (
            col("val") != lit(7),
            ["f1.parquet", "f2.parquet", "f3.parquet"],
            "ne cannot be proven from an interval",
        ),
        (
            ~(col("val") > lit(10)),
            ["f1.parquet", "f2.parquet", "f3.parquet"],
            "not is not reasoned through",
        ),
        (
            col("unknown_column") > lit(3),
            ["f1.parquet", "f2.parquet", "f3.parquet"],
            "a column absent from the stats keeps everything",
        ),
        (
            col("val") > col("other"),
            ["f1.parquet", "f2.parquet", "f3.parquet"],
            "column-to-column comparison keeps everything",
        ),
        # --- boolean composition ---------------------------------------
        (
            (col("val") > lit(5)) & (col("val") < lit(11)),
            ["f2.parquet"],
            "and drops when either side drops",
        ),
        (
            (col("val") < lit(2)) | (col("val") > lit(14)),
            ["f1.parquet", "f3.parquet"],
            "or drops only when both sides drop",
        ),
        (
            (col("val") > lit(100)) & (col("val") != lit(3)),
            [],
            "and still prunes when only one side is provable",
        ),
        (
            (col("val") > lit(100)) | (col("val") != lit(3)),
            ["f1.parquet", "f2.parquet", "f3.parquet"],
            "or keeps everything when one side is unprovable",
        ),
        # --- non-numeric stats ------------------------------------------
        (col("name") > lit("f"), ["f3.parquet"], "string stats prune too"),
    ],
)
def test_statistics_pruning(predicate: Expr, expected: List[str], description: str):
    result = prune_add_actions(_stats_actions(), data_predicate=predicate)
    assert _paths(result) == expected, description


def test_no_predicate_keeps_everything():
    actions = _stats_actions()
    assert _paths(prune_add_actions(actions)) == _paths(actions)


@pytest.mark.parametrize(
    "min_val,max_val,predicate,kept,description",
    [
        # A missing bound is treated as unbounded in that direction, which is
        # sound and still lets the *other* bound prune.
        (None, 5, col("val") < lit(-100), True, "missing min reads as -inf"),
        (1, None, col("val") > lit(1000), True, "missing max reads as +inf"),
        (None, None, col("val") == lit(50), True, "no stats at all"),
        (
            None,
            5,
            col("val") > lit(1000),
            False,
            "a present max still disproves the predicate on its own",
        ),
        (
            1,
            None,
            col("val") < lit(-100),
            False,
            "a present min still disproves the predicate on its own",
        ),
    ],
)
def test_partial_statistics(
    min_val: Optional[int],
    max_val: Optional[int],
    predicate: Expr,
    kept: bool,
    description: str,
):
    actions = pa.table(
        {
            "path": ["f1.parquet"],
            "size_bytes": [100],
            "num_records": [10],
            "min.val": pa.array([min_val], type=pa.int64()),
            "max.val": pa.array([max_val], type=pa.int64()),
            "null_count.val": [0],
        }
    )
    result = prune_add_actions(actions, data_predicate=predicate)
    assert _paths(result) == (["f1.parquet"] if kept else []), description


def test_stats_columns_absent_from_table_keep_everything():
    """A log with no statistics at all must not prune."""
    actions = pa.table(
        {
            "path": ["f1.parquet", "f2.parquet"],
            "size_bytes": [1, 1],
            "num_records": [1, 1],
        }
    )
    result = prune_add_actions(actions, data_predicate=col("val") > lit(1000))
    assert _paths(result) == ["f1.parquet", "f2.parquet"]


def test_empty_add_actions():
    actions = _stats_actions().slice(0, 0)
    result = prune_add_actions(actions, data_predicate=col("val") > lit(3))
    assert result.num_rows == 0
    assert result.schema == actions.schema


# ---------------------------------------------------------------------------
# Partition pruning
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "predicate,expected,description",
    [
        (
            col("region") == lit("US"),
            ["p1.parquet", "p2.parquet"],
            "string partition equality",
        ),
        (
            col("region") != lit("US"),
            ["p3.parquet", "p4.parquet"],
            "partition values are exact, so != is prunable",
        ),
        (
            col("year") > lit(50),
            ["p2.parquet", "p4.parquet"],
            "int partition compares numerically, not lexicographically",
        ),
        (
            col("year") == lit(9),
            ["p1.parquet", "p3.parquet"],
            "int partition equality after cast",
        ),
        (
            (col("region") == lit("EU")) & (col("year") == lit(100)),
            ["p4.parquet"],
            "conjunction over two partition columns",
        ),
        (
            (col("region") == lit("US")) | (col("year") == lit(9)),
            ["p1.parquet", "p2.parquet", "p3.parquet"],
            "disjunction over two partition columns",
        ),
    ],
)
def test_partition_pruning(predicate: Expr, expected: List[str], description: str):
    result = prune_add_actions(
        _partition_actions(),
        partition_predicate=predicate,
        table_schema=_PARTITION_SCHEMA,
    )
    assert _paths(result) == expected, description


def test_partition_and_statistics_pruning_compose():
    actions = pa.table(
        {
            "path": ["a.parquet", "b.parquet", "c.parquet"],
            "size_bytes": [1, 1, 1],
            "num_records": [5, 5, 5],
            "partition.region": ["US", "US", "EU"],
            "partition.year": ["9", "9", "9"],
            "min.val": [1, 100, 1],
            "max.val": [5, 105, 5],
            "null_count.val": [0, 0, 0],
        }
    )
    result = prune_add_actions(
        actions,
        partition_predicate=col("region") == lit("US"),
        data_predicate=col("val") > lit(50),
        table_schema=_PARTITION_SCHEMA,
    )
    # a: right region, wrong range. c: right range, wrong region.
    assert _paths(result) == ["b.parquet"]


# ---------------------------------------------------------------------------
# The safety property
# ---------------------------------------------------------------------------


def _predicate_matches(predicate_kind: str, value: Optional[int], bound: int) -> bool:
    """Evaluate the same predicate ``_random_predicate`` builds, in Python."""
    if value is None:
        # A null satisfies none of the comparisons used here.
        return False
    if predicate_kind == "gt":
        return value > bound
    if predicate_kind == "ge":
        return value >= bound
    if predicate_kind == "lt":
        return value < bound
    if predicate_kind == "le":
        return value <= bound
    if predicate_kind == "eq":
        return value == bound
    raise AssertionError(f"unhandled predicate kind {predicate_kind}")


def _random_predicate(predicate_kind: str, bound: int) -> Expr:
    return {
        "gt": lambda: col("val") > lit(bound),
        "ge": lambda: col("val") >= lit(bound),
        "lt": lambda: col("val") < lit(bound),
        "le": lambda: col("val") <= lit(bound),
        "eq": lambda: col("val") == lit(bound),
    }[predicate_kind]()


@pytest.mark.parametrize("seed", range(25))
def test_pruning_never_drops_a_matching_file(seed: int):
    """Generated-input check of the one property that must never break.

    Each seed is a fixed case: the RNG is seeded per parameter, so the
    generated files, rows and predicate are identical on every run. Stats are
    derived from the rows the way Delta derives them, then every file holding
    at least one matching row must survive pruning. Over-retention is fine;
    dropping a matching file is silent data loss.
    """
    rng = random.Random(seed)
    kinds = ["gt", "ge", "lt", "le", "eq"]

    files = []
    for i in range(rng.randint(1, 6)):
        rows = [
            rng.choice([None, rng.randint(-20, 20)]) for _ in range(rng.randint(1, 8))
        ]
        files.append((f"f{i}.parquet", rows))

    def _stat(rows, fn):
        present = [r for r in rows if r is not None]
        return fn(present) if present else None

    actions = pa.table(
        {
            "path": [name for name, _ in files],
            "size_bytes": [1] * len(files),
            "num_records": [len(rows) for _, rows in files],
            "min.val": pa.array([_stat(r, min) for _, r in files], type=pa.int64()),
            "max.val": pa.array([_stat(r, max) for _, r in files], type=pa.int64()),
            "null_count.val": [sum(1 for v in rows if v is None) for _, rows in files],
        }
    )

    kind = rng.choice(kinds)
    bound = rng.randint(-20, 20)
    kept = set(_paths(prune_add_actions(actions, data_predicate=_random_predicate(kind, bound))))

    for name, rows in files:
        if any(_predicate_matches(kind, v, bound) for v in rows):
            assert name in kept, (
                f"seed={seed}: {name} holds a row matching val {kind} {bound} "
                f"(rows={rows}) but was pruned away"
            )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
