"""Hive table projection and limit plans through the logical optimizer."""

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
from ray.data._internal.datasource_v2.formats.hive.hive_direct import (
    _HiveParquetScanner,
)
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
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
from ray.data._internal.metadata_exporter import sanitize_for_struct
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data.context import DataContext
from ray.data.expressions import col

_SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False, metadata={b"field": b"id"}),
        ("name", pa.string()),
        ("score", pa.float64()),
    ],
    metadata={b"source": b"hive"},
)


def _scanner(limit=None):
    return _HiveTableScanner(
        HiveReadSpec(lambda: None, table="events", limit=limit), _SCHEMA
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


def _optimized(root: LogicalOperator) -> LogicalOperator:
    result = (
        LogicalOptimizer().optimize(LogicalPlan(root, DataContext.get_current())).dag
    )
    assert isinstance(result, LogicalOperator)
    return result


def _table_scan(root: LogicalOperator) -> _HiveTableScanner:
    for node in _nodes(root):
        if isinstance(node, ReadFiles):
            assert isinstance(node.scanner, _HiveTableScanner)
            return node.scanner
    raise AssertionError("Missing table scan")


def test_table_scanner_capabilities_clone_and_projection():
    original = _scanner()
    assert isinstance(original, SupportsColumnPruning)
    assert isinstance(original, SupportsLimitPushdown)
    assert not isinstance(original, SupportsFilterPushdown)
    final = cloudpickle.loads(
        cloudpickle.dumps(original.prune_columns(["name"]).push_limit(20).push_limit(5))
    )
    assert original.columns is None and original.limit is None
    assert final.pruned_column_names() == ("name",)
    assert final.pushed_limit() == 5
    assert final.read_schema().metadata == _SCHEMA.metadata
    assert final.create_reader()._options["statement"] == (
        "SELECT `name` FROM `default`.`events` LIMIT 5"
    )
    assert (
        original.prune_columns(["id"])
        .read_schema()
        .field("id")
        .equals(_SCHEMA.field("id"), check_metadata=True)
    )
    with pytest.raises(ValueError, match="unavailable"):
        final.prune_columns(["id"])


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


@pytest.mark.parametrize("limit", [True, False])
@pytest.mark.parametrize("source_limit", [None, 5])
@pytest.mark.parametrize("existing_limit", [None, 2])
def test_boolean_limit_remains_in_ray(limit, source_limit, existing_limit):
    scanner = _scanner(source_limit)
    if existing_limit is not None:
        scanner = scanner.push_limit(existing_limit)
    previous_limit = scanner.pushed_limit()
    assert scanner.push_limit(limit) is scanner
    assert scanner.pushed_limit() == previous_limit
    statement = scanner.create_reader()._options["statement"]
    if previous_limit is None:
        assert " LIMIT " not in statement
    else:
        assert statement.endswith(f" LIMIT {previous_limit}")


@pytest.mark.parametrize(
    "source_limit,limits,expected",
    [
        (None, (2**31, 10), 10),
        (None, (10, 2**31), 10),
        (100, (2**31,), 100),
        (100, (2**31, 10), 10),
        (5, (2,), 2),
    ],
)
def test_limits_preserve_smaller_source_and_accepted_limits(
    source_limit, limits, expected
):
    scanner = _scanner(source_limit)
    for limit in limits:
        scanner = scanner.push_limit(limit)
    assert scanner.pushed_limit() == expected
    assert scanner.create_reader()._options["statement"].endswith(f" LIMIT {expected}")


@pytest.mark.parametrize(
    "source_limit,steps,sql_limit",
    [
        (5, ("filter", "limit2"), 5),
        (None, ("filter", "limit2"), None),
        (None, ("limit5", "filter"), 5),
        (5, ("limit2",), 2),
        (None, ("limit_big", "filter", "limit2"), None),
    ],
)
def test_filter_is_a_limit_barrier(source_limit, steps, sql_limit):
    root = _read(_scanner(source_limit))
    for step in steps:
        if step == "filter":
            root = Filter(predicate_expr=col("id") > 1, input_dependencies=[root])
        else:
            value = {"limit2": 2, "limit5": 5, "limit_big": 2**31}[step]
            root = Limit(value, input_dependencies=[root])
    optimized = _optimized(root)
    scan = _table_scan(optimized)
    assert scan.pushed_limit() == sql_limit
    assert not isinstance(scan, SupportsFilterPushdown)
    assert sum(isinstance(node, Filter) for node in _nodes(optimized)) == steps.count(
        "filter"
    )
    statement = scan.create_reader()._options["statement"]
    assert " WHERE " not in statement
    if sql_limit is None:
        assert " LIMIT " not in statement
    else:
        assert statement.endswith(f" LIMIT {sql_limit}")
    assert _optimized(optimized).dag_str == optimized.dag_str


def test_projection_below_filter_is_not_required():
    read = _read(_scanner())
    filtered = Filter(predicate_expr=col("id") > 1, input_dependencies=[read])
    projected = Project(exprs=[col("name")], input_dependencies=[filtered])
    optimized = _optimized(projected)
    assert _table_scan(optimized).columns is None
    assert any(isinstance(node, Filter) for node in _nodes(optimized))
    schema = optimized.infer_schema()
    assert isinstance(schema, pa.Schema)
    assert schema.names == ["name"]


def test_sort_is_a_limit_barrier():
    root = Limit(
        2,
        input_dependencies=[
            Sort(SortKey("id"), input_dependencies=[_read(_scanner())])
        ],
    )
    assert _table_scan(_optimized(root)).pushed_limit() is None


def test_query_scanner_does_not_advertise_pushdown():
    spec = HiveReadSpec(lambda: None, query="SELECT id", schema=_SCHEMA)
    scanner = HiveDatasourceV2(spec).create_scanner(_SCHEMA)
    assert isinstance(scanner, _HiveScanner)
    for mixin in (SupportsColumnPruning, SupportsFilterPushdown, SupportsLimitPushdown):
        assert not isinstance(scanner, mixin)
    assert scanner.create_reader()._options == {}


@pytest.mark.parametrize("source", ["parquet", "orc", "hive_direct", "hive_query"])
def test_non_table_read_metadata_preserves_original_args(source):
    if source == "hive_query":
        scanner = _HiveScanner(
            HiveReadSpec(lambda: None, query="SELECT id", schema=_SCHEMA), _SCHEMA
        )
        name = "Hive"
    elif source == "hive_direct":
        scanner = _HiveParquetScanner(
            schema=_SCHEMA, hive_schema=_SCHEMA, sampled_names=()
        )
        name = "HiveDirect"
    else:
        scanner = (ParquetScanner if source == "parquet" else OrcScanner)(
            schema=_SCHEMA
        )
        name = source
    read = replace(_read(scanner), datasource_name=name)
    original = super(ReadFiles, read)._get_args()
    current = read._get_args()
    assert current["_scanner"] is original["_scanner"] is scanner
    assert sanitize_for_struct(current) == sanitize_for_struct(original)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
