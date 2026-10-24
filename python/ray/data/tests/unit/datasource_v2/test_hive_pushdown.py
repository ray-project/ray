"""Table scan plans and full logical optimizer composition."""

import sys
from dataclasses import replace

import pyarrow as pa
import pytest

import ray.cloudpickle as cloudpickle
from ray.data._internal.datasource_v2.formats.hive.hive_contract import HiveReadSpec
from ray.data._internal.datasource_v2.formats.hive.hive_datasource_v2 import (
    HiveDatasourceV2,
    _HiveScanner,
    _HiveTableScanner,
)
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
)
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan
from ray.data._internal.logical.operators import Filter, Limit, Project, ReadFiles
from ray.data._internal.logical.operators.all_to_all_operator import Sort
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.block import Block
from ray.data.context import DataContext
from ray.data.expressions import col

_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False, metadata={b"field": b"id"}),
        ("name", pa.string()),
        ("score", pa.float64()),
        ("unused", pa.string()),
    ],
    metadata={b"source": b"hive"},
)
_TYPES = (
    ("id", "bigint"),
    ("name", "string"),
    ("score", "double"),
    ("unused", "string"),
)


def _scanner(limit=None):
    return _HiveTableScanner(
        HiveReadSpec(lambda: None, table="events", limit=limit), _SCHEMA, _TYPES
    )


def _read(scanner):
    return ReadFiles(
        datasource_name="Hive",
        scanner=scanner,
        schema=_SCHEMA,
        parallelism=1,
        input_dependencies=[InputData([])],
    )


def _nodes(root):
    yield root
    for child in root.input_dependencies:
        yield from _nodes(child)


def _table_scan(root) -> _HiveTableScanner:
    for node in _nodes(root):
        if isinstance(node, ReadFiles):
            assert isinstance(node.scanner, _HiveTableScanner)
            return node.scanner
    raise AssertionError("Missing table scan")


def _schema(root) -> pa.Schema:
    assert isinstance(root, LogicalOperator)
    schema = root.infer_schema()
    assert isinstance(schema, pa.Schema)
    return schema


def test_table_plan_cloning_schema_serialization_and_sql():
    original = _scanner()
    first, remainder = original.push_filters(col("id") > 1)
    assert remainder is None
    final = first.prune_columns(["name"]).push_limit(20).push_limit(5)
    final = cloudpickle.loads(cloudpickle.dumps(final))
    assert (
        original.columns is None
        and original.predicate is None
        and original.limit is None
    )
    assert final.native_types == _TYPES
    assert final.pruned_column_names() == ("name",)
    assert final.pushed_limit() == 5
    assert final.pushed_predicate().structurally_equals(col("id") > 1)
    assert final.read_schema().metadata == _SCHEMA.metadata
    assert (
        "WHERE (`id` > CAST('1' AS bigint)) LIMIT 5"
        in final.create_reader()._options["statement"]
    )
    assert (
        final.create_reader()
        ._options["statement"]
        .startswith("SELECT `name` FROM `default`.`events`")
    )
    assert "CAST" not in repr(final)
    selected = original.prune_columns(["id"]).read_schema()
    assert selected.field("id").equals(_SCHEMA.field("id"), check_metadata=True)
    with pytest.raises(ValueError, match="unavailable"):
        final.prune_columns(["id"])


@pytest.mark.parametrize("scanner", [_scanner(100), _scanner().push_limit(100)])
def test_limit_is_a_filter_barrier(scanner):
    predicate = col("id") > 1
    result, residual = scanner.push_filters(predicate)
    assert result is scanner and residual is predicate
    assert scanner.push_limit(10).pushed_limit() == 10


@pytest.mark.parametrize(
    "limit,expected",
    [(0, 0), (2**31 - 1, 2**31 - 1), (2**31, None), (2**63, None)],
    ids=["zero", "maximum", "overflow", "large_integer"],
)
def test_dataset_limit_sql_integer_boundary(limit, expected):
    original = _scanner()
    scanner = original.push_limit(limit)
    assert scanner.pushed_limit() == expected
    statement = scanner.create_reader()._options["statement"]
    if expected is None:
        assert scanner is original
        assert " LIMIT " not in statement
    else:
        assert statement.endswith(f" LIMIT {expected}")
    assert original.limit is None


@pytest.mark.parametrize(
    "source_limit,limits,expected",
    [
        (None, (2**31, 10), 10),
        (None, (10, 2**31), 10),
        (100, (2**31,), 100),
        (100, (2**31, 10), 10),
    ],
)
def test_oversized_limit_preserves_smaller_limits(source_limit, limits, expected):
    scanner = _scanner(source_limit)
    for limit in limits:
        scanner = scanner.push_limit(limit)
    assert scanner.pushed_limit() == expected
    assert scanner.create_reader()._options["statement"].endswith(f" LIMIT {expected}")


@pytest.mark.parametrize("filter_after_limit", [False, True])
def test_oversized_limit_keeps_ray_limit_and_filter_order(filter_after_limit):
    source = _read(_scanner())
    predicate = col("id") > 1
    root: LogicalOperator
    if filter_after_limit:
        limited = Limit(2**31, input_dependencies=[source])
        root = Filter(predicate_expr=predicate, input_dependencies=[limited])
    else:
        filtered = Filter(predicate_expr=predicate, input_dependencies=[source])
        root = Limit(2**31, input_dependencies=[filtered])
    root = Project(exprs=[col("name")], input_dependencies=[root])
    optimized = LogicalOptimizer().optimize(
        LogicalPlan(root, DataContext.get_current())
    )
    nodes = list(_nodes(optimized.dag))
    scan = _table_scan(optimized.dag)
    assert scan.pushed_limit() is None
    assert any(isinstance(node, Limit) and node.limit == 2**31 for node in nodes)
    assert any(isinstance(node, Filter) for node in nodes) == filter_after_limit
    assert (scan.pushed_predicate() is None) == filter_after_limit
    assert scan.columns == (("name", "id") if filter_after_limit else ("name",))
    assert " LIMIT " not in scan.create_reader()._options["statement"]
    assert _schema(optimized.dag).names == ["name"]
    assert LogicalOptimizer().optimize(optimized).dag.dag_str == optimized.dag.dag_str


def test_query_scanner_does_not_advertise_sql_rewrite():
    schema = pa.schema([("id", pa.int64())])
    spec = HiveReadSpec(
        lambda: pytest.fail("No driver query connection"),
        query="SELECT id",
        schema=schema,
    )
    scanner = HiveDatasourceV2(spec).create_scanner(schema)
    assert isinstance(scanner, _HiveScanner)
    for mixin in (SupportsColumnPruning, SupportsFilterPushdown, SupportsLimitPushdown):
        assert not isinstance(scanner, mixin)
    assert scanner.create_reader()._options == {}


@pytest.mark.parametrize(
    "source_limit,steps,sql_limit,where,columns,residual",
    [
        (None, "p,l10,project", 10, True, ("name",), False),
        (None, "mixed,l10,project", None, True, ("name", "score"), True),
        (None, "l100,p,project", 100, False, ("name", "id"), True),
        (100, "p,l10,project", 100, False, ("name", "id"), True),
        (100, "l10,project", 10, False, ("name",), False),
        (None, "p,l100,p,project", 100, True, ("name", "id"), True),
        (None, "mixed,empty", None, True, ("score",), True),
        (None, "p,empty", None, True, (), False),
    ],
)
def test_full_optimizer_preserves_projection_residual_and_limit(
    source_limit, steps, sql_limit, where, columns, residual
):
    source = _read(_scanner(source_limit))
    root = source
    for step in steps.split(","):
        if step in ("p", "mixed"):
            predicate = col("id") > 1
            if step == "mixed":
                predicate = predicate & (col("score") > 1.0)
            root = Filter(predicate_expr=predicate, input_dependencies=[root])
        elif step.startswith("l"):
            root = Limit(int(step[1:]), input_dependencies=[root])
        else:
            root = Project(
                exprs=[] if step == "empty" else [col("name")],
                input_dependencies=[root],
            )
    plan = LogicalPlan(root, DataContext.get_current())
    optimized = LogicalOptimizer().optimize(plan)
    nodes = list(_nodes(optimized.dag))
    scan = _table_scan(optimized.dag)
    assert scan.pushed_limit() == sql_limit
    assert (scan.predicate is not None) == where
    assert scan.columns == columns
    assert any(isinstance(node, Filter) for node in nodes) == residual
    assert _schema(optimized.dag).names == ([] if "empty" in steps else ["name"])
    again = LogicalOptimizer().optimize(optimized)
    assert again.dag.dag_str == optimized.dag.dag_str
    assert isinstance(source.scanner, _HiveTableScanner)
    assert source.scanner.columns is None and source.scanner.predicate is None


def test_callable_filter_blocks_column_pruning():
    source = _read(_scanner())
    root = Project(
        exprs=[col("name")],
        input_dependencies=[
            Filter(fn=lambda row: row["id"] > 1, input_dependencies=[source])
        ],
    )
    optimized = LogicalOptimizer().optimize(
        LogicalPlan(root, DataContext.get_current())
    )
    scan = _table_scan(optimized.dag)
    assert scan.columns is None


def test_non_prunable_query_keeps_filter_before_projection():
    scanner = _HiveScanner(
        HiveReadSpec(lambda: None, query="SELECT * FROM events", schema=_SCHEMA),
        _SCHEMA,
    )
    source = _read(scanner)
    selected = Project(exprs=[col("name"), col("id")], input_dependencies=[source])
    root = Filter(predicate_expr=col("id") > 1, input_dependencies=[selected])
    optimized = LogicalOptimizer().optimize(
        LogicalPlan(root, DataContext.get_current())
    )
    assert isinstance(optimized.dag, Project)
    filtered = optimized.dag.input_dependencies[0]
    assert isinstance(filtered, Filter)
    assert filtered.input_dependencies[0] is source


@pytest.mark.parametrize("extra_column", [False, True])
def test_partial_filter_pruning_does_not_depend_on_the_display_name(extra_column):
    long_name = "score_" + "x" * 64
    fields = [("name", pa.string()), ("id", pa.int64())]
    fields += [(long_name, pa.float64()), ("score", pa.float64())]
    native_types = [("name", "string"), ("id", "bigint")]
    native_types += [(long_name, "double"), ("score", "double")]
    if extra_column:
        fields.append(("unused", pa.string()))
        native_types.append(("unused", "string"))
    schema = pa.schema(fields)
    scanner = _HiveTableScanner(
        HiveReadSpec(lambda: None, table="events"), schema, tuple(native_types)
    )
    source = replace(_read(scanner), schema=schema)
    first_residual = col(long_name) > 1.0
    second_residual = col("score") > 2.0
    accepted = col("id") > 1
    filtered = Filter(
        predicate_expr=first_residual & (accepted & second_residual),
        input_dependencies=[source],
    )
    residual = Filter(
        predicate_expr=first_residual & second_residual,
        input_dependencies=[source],
    )
    assert filtered.name == residual.name  # The truncated label cannot show progress.
    root = Project(exprs=[col("name")], input_dependencies=[filtered])
    optimized = LogicalOptimizer().optimize(
        LogicalPlan(root, DataContext.get_current())
    )
    scan = _table_scan(optimized.dag)
    assert scan.columns == ("name", long_name, "score")
    pushed = scan.pushed_predicate()
    assert pushed is not None and pushed.structurally_equals(accepted)
    assert _schema(optimized.dag).names == ["name"]
    assert (
        scan.create_reader()
        ._options["statement"]
        .startswith(f"SELECT `name`, `{long_name}`, `score` FROM")
    )
    assert LogicalOptimizer().optimize(optimized).dag.dag_str == optimized.dag.dag_str


def test_filter_on_removed_column_is_not_resurrected():
    source = _read(_scanner())
    selected = Project(exprs=[col("name")], input_dependencies=[source])
    root = Filter(predicate_expr=col("id") > 1, input_dependencies=[selected])
    optimized = LogicalOptimizer().optimize(
        LogicalPlan(root, DataContext.get_current())
    )
    scan = _table_scan(optimized.dag)
    assert scan.columns == ("name",)
    assert scan.predicate is None
    assert any(isinstance(node, Filter) for node in _nodes(optimized.dag))


def test_sort_is_a_limit_barrier():
    source = _read(_scanner())
    root = Limit(
        2, input_dependencies=[Sort(SortKey("id"), input_dependencies=[source])]
    )
    optimized = LogicalOptimizer().optimize(
        LogicalPlan(root, DataContext.get_current())
    )
    scan = _table_scan(optimized.dag)
    assert scan.pushed_limit() is None


@pytest.mark.parametrize("operator", ["filter", "limit"])
def test_opaque_block_transform_keeps_its_physical_input_columns(operator):
    def transform(block: Block) -> Block:
        assert isinstance(block, pa.Table)
        return block.select(["unused", "score", "id"]).rename_columns(
            ["name", "score", "id"]
        )

    source = replace(
        _read(_scanner()),
        block_udf=transform,
    )
    row_op = (
        Filter(predicate_expr=col("score") > 1.0, input_dependencies=[source])
        if operator == "filter"
        else Limit(1, input_dependencies=[source])
    )
    root = Project(
        exprs=[col("name")],
        input_dependencies=[row_op],
    )
    optimized = LogicalOptimizer().optimize(
        LogicalPlan(root, DataContext.get_current())
    )
    scan = _table_scan(optimized.dag)
    assert scan.columns is None
    assert _schema(optimized.dag).names == ["name"]


@pytest.mark.parametrize("column,pushed", [("identifier", True), ("next", False)])
def test_rename_rebinding_and_computed_column_filter_boundaries(column, pushed):
    source = _read(_scanner())
    project = Project(
        exprs=[col("id")._rename("identifier"), (col("id") + 1).alias("next")],
        input_dependencies=[source],
    )
    root = Project(
        exprs=[col("next")],
        input_dependencies=[
            Filter(predicate_expr=col(column) > 1, input_dependencies=[project])
        ],
    )
    optimized = LogicalOptimizer().optimize(
        LogicalPlan(root, DataContext.get_current())
    )
    scan = _table_scan(optimized.dag)
    assert scan.columns == ("id",)
    assert (scan.predicate is not None) == pushed
    assert _schema(optimized.dag).names == ["next"]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
