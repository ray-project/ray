"""Unit tests for :class:`DeriveListFilesPushdown`.

``ListFiles`` prunes row groups, sizes columns, and stops listing early using
the constraints it carries. Those constraints are only sound while they are no
stronger than what the downstream ``ReadFiles`` scanner actually applies -- a
predicate ``ListFiles`` prunes by but the reader never evaluates drops rows
with no error. These tests pin that invariant, including for plan shapes no
current rule produces but a future one could.
"""

from dataclasses import replace
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.listing_utils import sample_files
from ray.data._internal.datasource_v2.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.scanners.arrow_file_scanner import (
    ArrowFileScanner,
)
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Plan,
    Rule,
)
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.optimizers import LogicalOptimizer, get_logical_ruleset
from ray.data._internal.logical.rules.derive_list_files_pushdown import (
    DeriveListFilesPushdown,
)
from ray.data.context import DataContext
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _mk_read_files(tmp_path: Path) -> ReadFiles:
    """A minimal ``ListFiles -> ReadFiles`` chain over one Parquet file."""
    f = tmp_path / "data.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}), str(f))

    datasource = ParquetDatasourceV2([str(f)])
    indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
    sample = sample_files(indexer, datasource.paths, datasource.filesystem)
    schema = datasource.infer_schema(sample)

    list_files_op = ListFiles(
        paths=list(datasource.paths),
        file_indexer=indexer,
        filesystem=datasource.filesystem,
        source_paths=list(datasource.paths),
        file_extensions=datasource.file_extensions,
    )
    return ReadFiles(
        datasource_name=datasource.name,
        scanner=datasource.create_scanner(schema=schema),
        schema=schema,
        parallelism=-1,
        input_dependencies=[list_files_op],
    )


def _apply(dag: LogicalOperator) -> LogicalPlan:
    plan = LogicalPlan(dag=dag, context=DataContext.get_current())
    return DeriveListFilesPushdown().apply(plan)


def _list_files_of(plan: Plan) -> ListFiles:
    (list_files,) = [
        op for op in plan.dag.post_order_iter() if isinstance(op, ListFiles)
    ]
    return list_files


def _source_list_files(read_files: ReadFiles) -> ListFiles:
    """The ``ListFiles`` feeding ``read_files``, typed as such.

    ``input_dependencies`` is declared as plain ``LogicalOperator``.
    """
    (list_files,) = read_files.input_dependencies
    assert isinstance(list_files, ListFiles), list_files
    return list_files


def _scanner_of(read_files: ReadFiles) -> ArrowFileScanner:
    """The scanner of ``read_files``, typed as the pushdown-capable subclass.

    ``ReadFiles.scanner`` is declared as the base ``Scanner``, which carries
    none of the ``Supports*`` pushdown methods these tests drive.
    """
    scanner = read_files.scanner
    assert isinstance(scanner, ArrowFileScanner), scanner
    return scanner


def test_derives_state_the_scanner_accepted(tmp_path):
    read_files = _mk_read_files(tmp_path)
    predicate = col("a") > 2
    scanner, _residual = _scanner_of(read_files).push_filters(predicate)
    scanner = scanner.prune_columns(["a"]).push_limit(5)
    read_files = replace(read_files, scanner=scanner)

    list_files = _list_files_of(_apply(read_files))

    assert list_files.predicate is predicate
    assert list_files.projected_columns == ["a"]
    assert list_files.limit == 5


def test_no_pushdown_leaves_list_files_unconstrained(tmp_path):
    list_files = _list_files_of(_apply(_mk_read_files(tmp_path)))

    assert list_files.predicate is None
    assert list_files.projected_columns is None
    assert list_files.limit is None


@pytest.mark.parametrize(
    "stale",
    [
        {"predicate": col("a") > 2},
        {"projected_columns": ["a"]},
        {"limit": 1},
        {"predicate": col("a") > 2, "projected_columns": ["a"], "limit": 1},
    ],
    ids=["predicate", "columns", "limit", "all"],
)
def test_state_the_scanner_does_not_carry_is_cleared(tmp_path, stale):
    """The failure the rule exists to prevent.

    A ``ListFiles`` carrying constraints its ``ReadFiles`` does not apply --
    e.g. a rewrite dropped or weakened the scanner's predicate -- would prune
    row groups nothing downstream re-checks.
    """
    read_files = _mk_read_files(tmp_path)
    read_files = replace(
        read_files,
        input_dependencies=[replace(_source_list_files(read_files), **stale)],
    )

    list_files = _list_files_of(_apply(read_files))

    assert list_files.predicate is None
    assert list_files.projected_columns is None
    assert list_files.limit is None


def test_state_is_cleared_when_consumer_is_not_read_files(tmp_path):
    """``PushdownCountFiles`` rewrites ``ReadFiles`` out of the plan entirely."""
    read_files = _mk_read_files(tmp_path)
    list_files = replace(
        _source_list_files(read_files), predicate=col("a") > 2, limit=1
    )
    count_rows = MapBatches(
        fn=lambda batch: batch,
        input_dependencies=[list_files],
        batch_format="pyarrow",
        can_modify_num_rows=True,
    )

    derived = _list_files_of(_apply(count_rows))

    assert derived.predicate is None
    assert derived.limit is None


class _WeakenScannerPredicate(Rule):
    """Stand-in for a future rule that rewrites the scanner's predicate."""

    def apply(self, plan: LogicalPlan) -> LogicalPlan:  # pyrefly: ignore[bad-override]
        def transform(node: LogicalOperator) -> LogicalOperator:
            if isinstance(node, ReadFiles):
                scanner = _scanner_of(node)
                if scanner.pushed_predicate() is not None:
                    return replace(node, scanner=replace(scanner, predicate=None))
            return node

        return LogicalPlan(
            dag=plan.dag._apply_transform(transform), context=plan.context
        )


@pytest.fixture
def weakening_rule():
    ruleset = get_logical_ruleset()
    ruleset.add(_WeakenScannerPredicate)
    try:
        yield
    finally:
        ruleset.remove(_WeakenScannerPredicate)


def test_optimizer_does_not_strand_a_predicate_a_later_rule_dropped(
    tmp_path, weakening_rule
):
    """A rule that drops the scanner's predicate must weaken listing too.

    The rule runs after the pushdown rules and knows nothing about
    ``ListFiles``; the invariant has to hold anyway.
    """
    from ray.data._internal.logical.operators import Filter

    read_files = _mk_read_files(tmp_path)
    dag = Filter(predicate_expr=col("a") > 2, input_dependencies=[read_files])

    optimized = LogicalOptimizer().optimize(
        LogicalPlan(dag=dag, context=DataContext.get_current())
    )

    read_files_ops: List[ReadFiles] = [
        op for op in optimized.dag.post_order_iter() if isinstance(op, ReadFiles)
    ]
    (scanner_predicate,) = [_scanner_of(op).pushed_predicate() for op in read_files_ops]
    assert scanner_predicate is None
    assert _list_files_of(optimized).predicate is None


def test_optimizer_keeps_list_files_in_sync_with_the_scanner(tmp_path):
    """Without the weakening rule, the pushed predicate does reach listing."""
    from ray.data._internal.logical.operators import Filter

    read_files = _mk_read_files(tmp_path)
    dag = Filter(predicate_expr=col("a") > 2, input_dependencies=[read_files])

    optimized = LogicalOptimizer().optimize(
        LogicalPlan(dag=dag, context=DataContext.get_current())
    )

    (scanner_predicate,) = [
        _scanner_of(op).pushed_predicate()
        for op in optimized.dag.post_order_iter()
        if isinstance(op, ReadFiles)
    ]
    assert scanner_predicate is not None
    assert _list_files_of(optimized).predicate is scanner_predicate


# ---------------------------------------------------------------------------
# count_ships_whole_manifest: the count rewrite keeps its path pruner
# ---------------------------------------------------------------------------


def _hive_tree(root: Path, sizes) -> str:
    """One file per ``year=<key>`` directory, ``sizes[key]`` rows each.

    Sizes differ on purpose: a pruning bug that sums the files it should have
    dropped still returns the right total when the partitions are equal.
    """
    for year, num_rows in sizes.items():
        part = root / f"year={year}"
        part.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table({"v": list(range(num_rows))}), part / "data.parquet")
    return str(root)


def _count_plan(ds):
    from ray.data._internal.logical.operators.count_operator import Count
    from ray.data._internal.logical.operators.map_operator import Project

    source = ds._logical_plan.dag
    count = Count(input_dependencies=[Project(exprs=[], input_dependencies=[source])])
    return LogicalOptimizer().optimize(LogicalPlan(count, ds.context))


def walk_children(op):
    yield op
    for child in op.input_dependencies:
        yield from walk_children(child)


def test_count_rewrite_keeps_the_partition_pruner(ray_start_regular_shared, tmp_path):
    """Listing must still drop files by path after the ``ReadFiles`` is gone.

    ``PushdownCountFiles`` deletes the ``ReadFiles``, and this rule clears
    every constraint when the consumer is not one. That is right by default --
    it cannot know an arbitrary ``MapBatches`` drops the same files. The count
    rewrite is the exception: its ``count_rows`` calls ``prune_manifest``
    itself.
    """
    from ray.data.expressions import col

    root = _hive_tree(tmp_path, {"2023": 10, "2024": 20})
    ds = ray.data.read_parquet(root).filter(expr=col("year") == "2023")

    plan = _count_plan(ds)
    list_files = next(op for op in walk_children(plan.dag) if isinstance(op, ListFiles))
    assert list_files.partition_pruner is not None


def test_count_rewrite_without_a_filter_has_no_pruner(
    ray_start_regular_shared, tmp_path
):
    """Nothing to prune by, so nothing is set -- the marker is not a blanket."""
    root = _hive_tree(tmp_path, {"2023": 10, "2024": 20})
    ds = ray.data.read_parquet(root)

    plan = _count_plan(ds)
    list_files = next(op for op in walk_children(plan.dag) if isinstance(op, ListFiles))
    assert list_files.partition_pruner is None


def test_plain_map_batches_over_listing_still_loses_everything(
    ray_start_regular_shared, tmp_path
):
    """The exception is the marker class, not ``MapBatches`` in general.

    A hand-built ``MapBatches`` over a ``ListFiles`` carrying a pruner must
    still have it cleared: nothing downstream applies it.
    """
    from ray.data._internal.datasource_v2.listing.file_pruners import (
        PartitionPredicatePruner,
    )
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle
    from ray.data.expressions import col

    root = _hive_tree(tmp_path, {"2023": 10, "2024": 20})
    ds = ray.data.read_parquet(root)
    list_files = next(
        op for op in walk_children(ds._logical_plan.dag) if isinstance(op, ListFiles)
    )
    pruner = PartitionPredicatePruner(
        Partitioning(PartitionStyle.HIVE), col("year") == "2023"
    )
    list_files = replace(list_files, partition_pruner=pruner)

    node = MapBatches(fn=lambda b: b, input_dependencies=[list_files])
    result = DeriveListFilesPushdown().apply(LogicalPlan(node, ds.context))

    rebuilt = next(op for op in walk_children(result.dag) if isinstance(op, ListFiles))
    assert rebuilt.partition_pruner is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
