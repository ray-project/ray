"""Unit tests for :class:`DeriveListFilesPushdown`.

``ListFiles`` prunes row groups, sizes columns, and stops listing early using
the constraints it carries. Those constraints are only sound while they are no
stronger than what the downstream ``ReadFiles`` scanner actually applies -- a
predicate ``ListFiles`` prunes by but the reader never evaluates drops rows
with no error. These tests pin that invariant, including for plan shapes no
current rule produces but a future one could.
"""

import ast
import copy
import inspect
from dataclasses import replace
from importlib import import_module
from importlib.util import resolve_name
from pathlib import Path
from typing import List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.common.arrow_file_scanner import ArrowFileScanner
from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data._internal.logical.interfaces import (
    LogicalOperator,
    LogicalPlan,
    Plan,
    Rule,
)
from ray.data._internal.logical.operators import (
    Filter,
    Limit,
    ListFiles,
    Project,
    ReadFiles,
)
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.optimizers import LogicalOptimizer, get_logical_ruleset
from ray.data._internal.logical.rules.derive_list_files_pushdown import (
    DeriveListFilesPushdown,
)
from ray.data.context import DataContext
from ray.data.datasource import FileShuffleConfig
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import Expr, col


def _footer_indexer(value):
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )

    assert isinstance(value, FooterFileIndexer)
    return value


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


@pytest.mark.parametrize(
    "shuffle_config, expected_limit",
    [
        (None, 5),
        (FileShuffleConfig(seed=1), 5),
        (FileShuffleConfig(seed=1, small_chunks_shuffle=True), None),
    ],
    ids=["no_shuffle", "file_shuffle", "small_chunks_shuffle"],
)
def test_limit_is_not_derived_onto_row_group_shuffle(
    tmp_path, shuffle_config, expected_limit
):
    """Listing stops early before the row-group shuffle runs.

    A listing limit would shrink the pool the shuffle permutes to the first
    files listed, so only the scanner keeps the limit.
    """
    read_files = _mk_read_files(tmp_path)
    predicate = col("a") > 2
    scanner, _residual = _scanner_of(read_files).push_filters(predicate)
    read_files = replace(
        read_files,
        scanner=scanner.push_limit(5),
        input_dependencies=[
            replace(
                _source_list_files(read_files),
                shuffle_config_factory=lambda: shuffle_config,
            )
        ],
    )

    plan = _apply(read_files)

    list_files = _list_files_of(plan)
    assert list_files.limit == expected_limit
    assert list_files.predicate is predicate
    (scanner_limit,) = [
        _scanner_of(op).pushed_limit()
        for op in plan.dag.post_order_iter()
        if isinstance(op, ReadFiles)
    ]
    assert scanner_limit == 5


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


def test_listing_partition_pruner_retains_full_schema_after_optimization(tmp_path):
    from ray.data._internal.logical.operators import Filter

    read_files = _mk_read_files(tmp_path)
    scanner = (
        replace(
            _scanner_of(read_files),
            schema=pa.schema([("a", pa.int64()), ("year", pa.int32())]),
            partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year"]),
        )
        .prune_columns(["a"])
        .push_limit(1)
    )
    read_files = replace(read_files, scanner=scanner)
    dag = Filter(predicate_expr=col("year") == 2024, input_dependencies=[read_files])

    optimized = LogicalOptimizer().optimize(
        LogicalPlan(dag=dag, context=DataContext.get_current())
    )
    list_files = _list_files_of(optimized)
    pruner = list_files.partition_pruner

    assert pruner is not None
    assert pruner.should_include("/root/year=2024/a.parquet")
    assert not pruner.should_include("/root/year=2023/b.parquet")
    assert list_files.projected_columns == ["a"]
    assert list_files.limit == 1


@pytest.mark.parametrize("columns", [["b"], []])
def test_block_udf_filter_keeps_listing_and_read_limit_unset(tmp_path, columns):
    read_files = replace(_mk_read_files(tmp_path), block_udf=lambda table: table)
    predicate = col("a") > 1
    filtered = Filter(predicate_expr=predicate, input_dependencies=[read_files])
    projected = Project(
        exprs=[col(name) for name in columns], input_dependencies=[filtered]
    )
    dag = Limit(limit=1, input_dependencies=[projected])

    optimized = LogicalOptimizer().optimize(
        LogicalPlan(dag=dag, context=DataContext.get_current())
    )
    (read,) = [
        op for op in optimized.dag.post_order_iter() if isinstance(op, ReadFiles)
    ]
    (remaining_filter,) = [
        op for op in optimized.dag.post_order_iter() if isinstance(op, Filter)
    ]
    assert remaining_filter.predicate_expr is not None
    assert remaining_filter.predicate_expr.structurally_equals(predicate)
    assert "a" in read.infer_schema().names
    scanner = _scanner_of(read)
    assert scanner.predicate is None
    assert scanner.partition_predicate is None
    assert scanner.limit is None
    listing = _list_files_of(optimized)
    assert listing.predicate is None
    assert listing.partition_pruner is None
    assert listing.limit is None


def _previous_hint():
    from ray.data._internal.datasource_v2.formats.parquet.listing_pruning import (
        ListingPruningHint,
    )
    from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
        ParquetScanner,
    )

    scanner = ParquetScanner(
        schema=pa.schema([("a", pa.int64()), ("p", pa.string())]),
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["p"]),
    )
    context = scanner.listing_pruning_context()
    assert context is not None
    return ListingPruningHint.from_residual(context, (col("p") == "a") | (col("a") > 9))


@pytest.mark.parametrize(
    "shape", ["bare", "read", "filter", "count", "non_read", "declined_scanner"]
)
def test_stale_hint_cleared_before_any_root_qualification(tmp_path, shape):
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )
    from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
        ParquetScanner,
    )
    from ray.data._internal.logical.operators.count_operator import Count

    original = FooterFileIndexer(ignore_missing_paths=False)
    hint = _previous_hint()
    configured = original.with_listing_hint(hint)
    read = _mk_read_files(tmp_path)
    listing = replace(_source_list_files(read), file_indexer=configured)
    q = col("a") > 1
    assert isinstance(read.scanner, ParquetScanner)
    read = replace(
        read,
        scanner=replace(read.scanner, predicate=q, columns=("a",), limit=5),
        input_dependencies=[listing],
    )
    r = (col("a") < 99) | (col("b") == "x")
    if shape == "bare":
        root = listing
    elif shape == "filter":
        root = Filter(predicate_expr=r, input_dependencies=[read])
    elif shape == "count":
        root = Count(input_dependencies=[read])
    elif shape == "non_read":
        root = MapBatches(
            fn=lambda batch: batch,
            input_dependencies=[listing],
            batch_format="pyarrow",
            can_modify_num_rows=True,
        )
    elif shape == "declined_scanner":

        class DecliningScanner(ParquetScanner):
            def listing_pruning_context(self):
                raise AssertionError("no context probe on an inactive/unknown source")

        root = replace(read, scanner=DecliningScanner(schema=read.schema))
    else:
        root = read
    derived = _apply(root)
    cleared = _list_files_of(derived)
    assert _footer_indexer(cleared.file_indexer)._listing_hint is None
    assert cleared.file_indexer is not configured
    assert configured._listing_hint is hint
    assert original._listing_hint is None
    assert (
        _footer_indexer(_list_files_of(_apply(derived.dag)).file_indexer)._listing_hint
        is None
    )
    if shape in ("read", "filter", "count"):
        assert cleared.predicate is q
        assert cleared.projected_columns == ["a"]
        assert cleared.limit == 5
    if shape == "filter":
        assert isinstance(derived.dag, Filter)
        assert derived.dag.predicate_expr is r


def test_shared_indexer_and_replaced_root_are_independently_cleaned(tmp_path):
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )
    from ray.data._internal.logical.operators.count_operator import Count
    from ray.data._internal.logical.operators.n_ary_operator import Union

    configured = FooterFileIndexer(ignore_missing_paths=False).with_listing_hint(
        _previous_hint()
    )
    read = _mk_read_files(tmp_path)
    listing = replace(_source_list_files(read), file_indexer=configured)
    read = replace(read, input_dependencies=[listing])
    first = _apply(Count(input_dependencies=[read]))
    second = _apply(Filter(predicate_expr=col("a") > 0, input_dependencies=[read]))
    assert _footer_indexer(_list_files_of(first).file_indexer)._listing_hint is None
    assert _footer_indexer(_list_files_of(second).file_indexer)._listing_hint is None
    assert configured._listing_hint is not None
    # Include a shared-DAG repeated source and a bare listing with no ReadFiles.
    result = _apply(Union(input_dependencies=[read, listing, read]))
    listings = [
        node for node in result.dag.post_order_iter() if isinstance(node, ListFiles)
    ]
    assert listings and all(
        _footer_indexer(node.file_indexer)._listing_hint is None for node in listings
    )
    assert configured._listing_hint is not None


@pytest.mark.parametrize("shape", ["bare", "read", "non_read"])
def test_unknown_indexer_clear_override_is_never_called(tmp_path, shape):
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )

    class CustomIndexer(FooterFileIndexer):
        def with_listing_hint(self, hint):
            raise AssertionError("custom hint callback must not be probed")

    indexer = CustomIndexer(ignore_missing_paths=False)
    read = _mk_read_files(tmp_path)
    listing = replace(_source_list_files(read), file_indexer=indexer)
    if shape == "bare":
        root = listing
    elif shape == "read":
        root = replace(read, input_dependencies=[listing])
    else:
        root = MapBatches(
            fn=lambda batch: batch,
            input_dependencies=[listing],
            batch_format="pyarrow",
            can_modify_num_rows=True,
        )
    assert _list_files_of(_apply(root)).file_indexer is indexer


class _CapabilityScanner(Scanner):
    """A non-Parquet scanner with no format-specific limit attribute."""

    def read_schema(self) -> pa.Schema:
        return pa.schema([("a", pa.int64()), ("b", pa.string())])

    def create_reader(self):
        raise AssertionError("The logical rule must not instantiate a reader")


class _CapabilityIndexer(NonSamplingFileIndexer):
    """An independent implementation of the residual-pruning contract."""

    residual: Optional[Expr] = None
    scanner: Optional[Scanner] = None

    def without_residual_pruning(self):
        if self.residual is None:
            return self
        cleared = copy.copy(self)
        cleared.residual = None
        cleared.scanner = None
        return cleared

    def with_residual_pruning(self, scanner, residual):
        configured = copy.copy(self)
        configured.scanner = scanner
        configured.residual = residual
        return configured


def test_residual_pruning_capability_defaults_to_noop():
    indexer = NonSamplingFileIndexer(ignore_missing_paths=False)
    assert indexer.without_residual_pruning() is indexer
    assert indexer.with_residual_pruning(_CapabilityScanner(), col("a") > 1) is indexer


def test_residual_pruning_rule_accepts_non_parquet_capability(tmp_path):
    original = _CapabilityIndexer(ignore_missing_paths=False)
    scanner = _CapabilityScanner()
    read = _mk_read_files(tmp_path)
    listing = replace(_source_list_files(read), file_indexer=original)
    read = replace(read, scanner=scanner, input_dependencies=[listing])
    residual = (col("a") > 1) | (col("b") == "x")
    result = _apply(Filter(predicate_expr=residual, input_dependencies=[read]))
    configured = _list_files_of(result).file_indexer
    assert isinstance(configured, _CapabilityIndexer)
    assert configured is not original
    assert configured.scanner is scanner
    assert configured.residual is residual
    assert original.residual is None
    assert isinstance(result.dag, Filter)
    assert result.dag.predicate_expr is residual


@pytest.mark.parametrize("shape", ["bare", "read", "count"])
def test_independent_capability_clears_stale_hint_before_early_return(tmp_path, shape):
    from ray.data._internal.logical.operators.count_operator import Count

    original = _CapabilityIndexer(ignore_missing_paths=False)
    scanner = _CapabilityScanner()
    residual = col("a") > 1
    configured = original.with_residual_pruning(scanner, residual)
    read = _mk_read_files(tmp_path)
    listing = replace(_source_list_files(read), file_indexer=configured)
    read = replace(read, scanner=scanner, input_dependencies=[listing])
    root = listing if shape == "bare" else read
    if shape == "count":
        root = Count(input_dependencies=[read])
    cleared = _list_files_of(_apply(root)).file_indexer
    assert isinstance(cleared, _CapabilityIndexer)
    assert cleared is not configured
    assert cleared.residual is None
    assert configured.residual is residual
    assert original.residual is None


def test_parquet_adapter_declines_mismatched_scanner_and_reader_limit():
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )
    from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
        ParquetScanner,
    )

    indexer = FooterFileIndexer(ignore_missing_paths=False)
    residual = (col("p") == "a") | (col("a") > 9)
    assert indexer.with_residual_pruning(_CapabilityScanner(), residual) is indexer
    scanner = ParquetScanner(
        schema=pa.schema([("a", pa.int64()), ("p", pa.string())]),
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["p"]),
    )
    assert indexer.with_residual_pruning(replace(scanner, limit=1), residual) is indexer
    configured = indexer.with_residual_pruning(scanner, residual)
    assert configured is not indexer
    assert configured._listing_hint is not None
    assert indexer._listing_hint is None
    assert configured.without_residual_pruning()._listing_hint is None


def test_parquet_capability_requires_explicit_subclass_admission():
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )
    from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
        ParquetScanner,
    )

    class DerivedIndexer(FooterFileIndexer):
        def with_listing_hint(self, hint):
            raise AssertionError(
                "Legacy hook does not opt a subclass into the capability"
            )

    class DerivedScanner(ParquetScanner):
        def listing_pruning_context(self):
            raise AssertionError("Unknown scanner semantics must not be probed")

    residual = (col("p") == "a") | (col("a") > 9)
    scanner = ParquetScanner(
        schema=pa.schema([("a", pa.int64()), ("p", pa.string())]),
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["p"]),
    )
    derived = DerivedIndexer(ignore_missing_paths=False)
    assert derived.without_residual_pruning() is derived
    assert derived.with_residual_pruning(scanner, residual) is derived
    indexer = FooterFileIndexer(ignore_missing_paths=False)
    unknown = DerivedScanner(schema=scanner.schema, partitioning=scanner.partitioning)
    assert indexer.with_residual_pruning(unknown, residual) is indexer


@pytest.mark.parametrize(
    "module_name",
    [
        "ray.data._internal.datasource_v2.common.listing_pruning",
        "ray.data._internal.logical.rules.derive_list_files_pushdown",
    ],
)
def test_listing_layers_do_not_import_format_implementations(module_name):
    """Keep direct format dependencies out of shared expression and plan logic."""
    module = import_module(module_name)
    tree = ast.parse(inspect.getsource(module))
    format_package = "ray.data._internal.datasource_v2.formats"
    dependencies = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            dependencies.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            package = resolve_name(
                "." * node.level + (node.module or ""),
                module_name.rpartition(".")[0],
            )
            dependencies.append(package)
            dependencies.extend(f"{package}.{alias.name}" for alias in node.names)
    assert not any(
        name == format_package or name.startswith(f"{format_package}.")
        for name in dependencies
    ), dependencies


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
