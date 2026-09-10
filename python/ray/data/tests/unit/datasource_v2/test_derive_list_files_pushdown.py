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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
