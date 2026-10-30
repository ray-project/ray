"""Unit tests for :class:`BindGeneratedIdColumn`.

``CheckpointConfig(generated_id_column=...)`` can be set on a dataset after
``read_parquet()`` built it, so the generated ID column is added when the plan
is optimized. These tests pin what the rule adds and where.
"""
from dataclasses import replace
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
    ParquetDatasourceV2,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan
from ray.data._internal.logical.operators import InputData, ListFiles, ReadFiles
from ray.data._internal.logical.operators.map_operator import MapBatches, Project
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.logical.rules.bind_generated_id_column import (
    BindGeneratedIdColumn,
)
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.generated_id import GENERATED_ID_COLUMN_TYPE, GeneratedIdColumn
from ray.data.checkpoint.interfaces import InvalidCheckpointingConfig
from ray.data.context import DataContext
from ray.data.expressions import col, star

ID_COLUMN = "generated_id"


class _FakeScanner(Scanner):
    """A scanner of no particular format, standing in for a non-Parquet one."""

    def read_schema(self) -> pa.Schema:
        return pa.schema([])

    def create_reader(self) -> Reader:
        raise NotImplementedError


def _prune_columns(read_files: ReadFiles, columns: "list[str]") -> ReadFiles:
    scanner = read_files.scanner
    assert isinstance(scanner, ParquetScanner), scanner
    return replace(read_files, scanner=scanner.prune_columns(columns))


def _schema_names(op: LogicalOperator) -> "list[str]":
    schema = op.infer_schema()
    assert schema is not None, op
    assert isinstance(schema, pa.Schema), schema
    return list(schema.names)


def _mk_read_files(tmp_path: Path, table: Optional[pa.Table] = None) -> ReadFiles:
    """A ``ListFiles -> ReadFiles`` chain over one Parquet file."""
    f = tmp_path / "data.parquet"
    if table is None:
        table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    pq.write_table(table, str(f))

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


@pytest.fixture
def context(tmp_path) -> DataContext:
    context = DataContext.get_current().copy()
    context.checkpoint_config = CheckpointConfig(
        checkpoint_path=str(tmp_path / "ckpt"), generated_id_column=ID_COLUMN
    )
    return context


def _apply(dag: LogicalOperator, context: DataContext) -> LogicalPlan:
    return BindGeneratedIdColumn().apply(LogicalPlan(dag=dag, context=context))


def _scanner_of(plan: LogicalPlan) -> ParquetScanner:
    (read_files,) = [
        op for op in plan.dag.post_order_iter() if isinstance(op, ReadFiles)
    ]
    scanner = read_files.scanner
    assert isinstance(scanner, ParquetScanner), scanner
    return scanner


def test_adds_the_column_to_the_read(tmp_path, context):
    scanner = _scanner_of(_apply(_mk_read_files(tmp_path), context))

    assert GeneratedIdColumn(ID_COLUMN) in scanner.synthesized_columns
    schema = scanner.read_schema()
    assert schema.names == ["a", "b", ID_COLUMN]
    assert schema.field(ID_COLUMN).type == GENERATED_ID_COLUMN_TYPE


@pytest.mark.parametrize(
    "make_config",
    [
        lambda path: None,
        lambda path: CheckpointConfig(id_column="a", checkpoint_path=path),
    ],
    ids=["no_config", "id_column"],
)
def test_no_generated_id_config_leaves_the_plan_alone(tmp_path, make_config):
    context = DataContext.get_current().copy()
    context.checkpoint_config = make_config(str(tmp_path / "ckpt"))
    plan = LogicalPlan(dag=_mk_read_files(tmp_path), context=context)

    assert BindGeneratedIdColumn().apply(plan) is plan


def test_is_idempotent(tmp_path, context):
    """The optimizer can run on a plan it already optimized: execution keeps
    the optimized DAG on the dataset."""
    once = _apply(_mk_read_files(tmp_path), context)

    assert BindGeneratedIdColumn().apply(once) is once


def test_adds_the_column_to_a_projection_pushed_into_the_read(tmp_path, context):
    read_files = _prune_columns(_mk_read_files(tmp_path), ["a"])

    scanner = _scanner_of(_apply(read_files, context))

    assert scanner.columns == ("a", ID_COLUMN)
    assert scanner.read_schema().names == ["a", ID_COLUMN]


@pytest.mark.parametrize(
    "exprs,expected_names",
    [
        ([col("a")], ["a", ID_COLUMN]),
        # ``with_column``: ``*`` was expanded without the column.
        ([star(), (col("a") + 1).alias("c")], ["a", "b", "c", ID_COLUMN]),
        # Already kept, e.g. selected by name.
        ([col(ID_COLUMN), col("a")], [ID_COLUMN, "a"]),
    ],
    ids=["select_columns", "with_column", "already_kept"],
)
def test_keeps_the_column_through_a_project_built_before_binding(
    tmp_path, context, exprs, expected_names
):
    project = Project(exprs=exprs, input_dependencies=[_mk_read_files(tmp_path)])

    plan = _apply(project, context)

    assert isinstance(plan.dag, Project)
    assert [expr.name for expr in plan.dag.exprs] == expected_names
    assert _schema_names(plan.dag) == expected_names


@pytest.mark.parametrize(
    "exprs", [[col("a")], [star()]], ids=["explicit_columns", "star"]
)
def test_leaves_a_project_over_an_unknown_schema_alone(tmp_path, context, exprs):
    """A UDF in between might drop the column; the write checks for it."""
    udf = MapBatches(
        fn=lambda batch: batch,
        input_dependencies=[_mk_read_files(tmp_path)],
        batch_format="pyarrow",
    )
    project = Project(exprs=exprs, input_dependencies=[udf])

    plan = _apply(project, context)

    assert isinstance(plan.dag, Project)
    assert plan.dag.exprs == project.exprs


def test_leaves_reads_and_projects_that_keep_no_columns_alone(tmp_path, context):
    """E.g. ``count()``, which projects the read to zero columns."""
    read_files = _prune_columns(_mk_read_files(tmp_path), [])
    project = Project(exprs=[], input_dependencies=[read_files])
    plan = LogicalPlan(dag=project, context=context)

    assert BindGeneratedIdColumn().apply(plan) is plan


def test_leaves_non_parquet_reads_alone(context):
    """The planner rejects these for generated-ID plans."""
    read_files = ReadFiles(
        datasource_name="JSONv2",
        scanner=_FakeScanner(),
        schema=pa.schema([]),
        parallelism=-1,
        input_dependencies=[InputData(input_data=[])],
    )
    plan = LogicalPlan(dag=read_files, context=context)

    assert BindGeneratedIdColumn().apply(plan) is plan


def test_raises_on_name_clash(tmp_path, context):
    """The column never replaces a file column, unlike ``path`` or
    ``row_hash``."""
    read_files = _mk_read_files(tmp_path, pa.table({"a": [1], ID_COLUMN: [2]}))

    with pytest.raises(InvalidCheckpointingConfig, match="already exists"):
        _apply(read_files, context)


def test_optimizer_binds_after_projection_pushdown(tmp_path, context):
    """The rule runs after the rule loop, and listing sees the bound
    projection."""
    project = Project(exprs=[col("a")], input_dependencies=[_mk_read_files(tmp_path)])

    optimized = LogicalOptimizer().optimize(LogicalPlan(dag=project, context=context))

    assert isinstance(optimized, LogicalPlan), optimized
    assert _schema_names(optimized.dag) == ["a", ID_COLUMN]
    assert _scanner_of(optimized).columns == ("a", ID_COLUMN)
    (list_files,) = [
        op for op in optimized.dag.post_order_iter() if isinstance(op, ListFiles)
    ]
    assert list_files.projected_columns == ["a", ID_COLUMN]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
