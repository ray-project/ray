import os
import random
from typing import Any, Dict, Generator, List, Tuple, Type

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from pkg_resources import parse_version
from pyiceberg import (
    catalog as pyi_catalog,
    expressions as pyi_expr,
    schema as pyi_schema,
    types as pyi_types,
)
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform

import ray
from ray.data import read_iceberg
from ray.data._internal.datasource.iceberg_datasource import IcebergDatasource
from ray.data._internal.logical.operators import Filter, Project
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.util import rows_same
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.expressions import col
from ray.data.tests.test_util import (
    get_operator_types as _get_operator_types,
    plan_has_operator as _has_operator_type,
)

_CATALOG_NAME = "ray_catalog"
_DB_NAME = "ray_db"
_TABLE_NAME = "ray_test"
_WAREHOUSE_PATH = "/tmp/warehouse"

_CATALOG_KWARGS = {
    "name": _CATALOG_NAME,
    "type": "sql",
    "uri": f"sqlite:///{_WAREHOUSE_PATH}/ray_pyiceberg_test_catalog.db",
    "warehouse": f"file://{_WAREHOUSE_PATH}",
}

_SCHEMA = pa.schema(
    [
        pa.field("col_a", pa.int32()),
        pa.field("col_b", pa.string()),
        pa.field("col_c", pa.int16()),
    ]
)


def create_pa_table():
    return pa.Table.from_pydict(
        mapping={
            "col_a": list(range(120)),
            "col_b": random.choices(["a", "b", "c", "d"], k=120),
            "col_c": random.choices(list(range(10)), k=120),
        },
        schema=_SCHEMA,
    )


@pytest.fixture(autouse=True, scope="function")
def pyiceberg_table():
    if not os.path.exists(_WAREHOUSE_PATH):
        os.makedirs(_WAREHOUSE_PATH)
    dummy_catalog = SqlCatalog(
        _CATALOG_NAME,
        **{
            "uri": f"sqlite:///{_WAREHOUSE_PATH}/ray_pyiceberg_test_catalog.db",
            "warehouse": f"file://{_WAREHOUSE_PATH}",
        },
    )

    pya_table = create_pa_table()

    if (_DB_NAME,) not in dummy_catalog.list_namespaces():
        dummy_catalog.create_namespace(_DB_NAME)
    if (_DB_NAME, _TABLE_NAME) in dummy_catalog.list_tables(_DB_NAME):
        dummy_catalog.drop_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Create the table, and add data to it
    table = dummy_catalog.create_table(
        f"{_DB_NAME}.{_TABLE_NAME}",
        schema=pyi_schema.Schema(
            pyi_types.NestedField(
                field_id=1,
                name="col_a",
                field_type=pyi_types.IntegerType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=2,
                name="col_b",
                field_type=pyi_types.StringType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=3,
                name="col_c",
                field_type=pyi_types.IntegerType(),
                required=False,
            ),
        ),
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=3, field_id=3, transform=IdentityTransform(), name="col_c"
            )
        ),
    )
    table.append(pya_table)

    # Delete some data so there are delete file(s)
    table.delete(delete_filter=pyi_expr.GreaterThanOrEqual("col_a", 101))


@pytest.fixture
def fast_retry_config():
    """Configure DataContext for fast retry testing."""
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    iceberg_config = ctx.iceberg_config
    original_max_attempts = iceberg_config.write_file_max_attempts
    original_max_backoff = iceberg_config.write_file_retry_max_backoff_s
    original_errors = ctx.retried_io_errors

    iceberg_config.write_file_max_attempts = 3
    iceberg_config.write_file_retry_max_backoff_s = 1
    ctx.retried_io_errors = list(original_errors) + ["TestTransientError"]

    yield ctx

    # Restore original settings
    iceberg_config.write_file_max_attempts = original_max_attempts
    iceberg_config.write_file_retry_max_backoff_s = original_max_backoff
    ctx.retried_io_errors = original_errors


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_get_catalog():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    catalog = iceberg_ds._get_catalog()
    assert catalog.name == _CATALOG_NAME


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_plan_files():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    plan_files = iceberg_ds.plan_files
    assert len(plan_files) == 10


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_chunk_plan_files():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    chunks = iceberg_ds._distribute_tasks_into_equal_chunks(iceberg_ds.plan_files, 5)
    assert (len(c) == 2 for c in chunks), chunks

    chunks = iceberg_ds._distribute_tasks_into_equal_chunks(iceberg_ds.plan_files, 20)
    assert (
        sum(len(c) == 1 for c in chunks) == 10
        and sum(len(c) == 0 for c in chunks) == 10
    ), chunks


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_get_read_tasks():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    read_tasks = iceberg_ds.get_read_tasks(5)
    assert len(read_tasks) == 5
    assert all(len(rt.metadata.input_files) == 2 for rt in read_tasks)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_filtered_read():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        row_filter=pyi_expr.In("col_c", {1, 2, 3, 4}),
        selected_fields=("col_b",),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    read_tasks = iceberg_ds.get_read_tasks(5)
    # Should be capped to 4, as there will be only 4 files
    assert len(read_tasks) == 4, read_tasks
    assert all(len(rt.metadata.input_files) == 1 for rt in read_tasks)


def _iceberg_scan_row_count(**scan_kwargs) -> int:
    """Rows PyIceberg itself returns for a scan -- the ground truth for counts."""
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS.copy())
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    return table.scan(**scan_kwargs).to_arrow().num_rows


# Every entry builds a Dataset whose row count is unambiguous, so ``count()`` can
# be checked against the rows the same query actually yields. ``count()`` has two
# strategies -- read the count from plan metadata, or project to zero columns and
# count what comes back -- and these cases cover both, including the cases that
# defeat the metadata short-circuit.
_COUNT_CASES = {
    "plain": lambda: read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    ),
    "select_columns": lambda: read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    ).select_columns(["col_b"]),
    # An expression filter is pushed into the datasource, which removes the
    # ``Filter`` from the plan and leaves the zero-column projection sitting
    # directly on the read.
    "expr_filter": lambda: read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    ).filter(expr=col("col_a") < 10),
    # Same query as a Python UDF: the predicate cannot be pushed down, so the
    # ``Filter`` stays in the plan. This is the control case -- it must keep
    # working, so a fix cannot simply disable predicate pushdown.
    "udf_filter": lambda: read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    ).filter(lambda row: row["col_a"] < 10),
    "expr_filter_then_select": lambda: read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    .filter(expr=col("col_a") < 10)
    .select_columns(["col_b"]),
    # A filter supplied at read time leaves no ``Filter`` in the plan at all, so
    # the count is answered from the Iceberg manifest.
    "read_time_row_filter": lambda: read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        row_filter=pyi_expr.LessThan("col_a", 10),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    ),
    "select_columns_materialized": lambda: read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    .select_columns(["col_b"])
    .materialize(),
}


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
@pytest.mark.parametrize("case", list(_COUNT_CASES), ids=list(_COUNT_CASES))
def test_count_matches_rows_actually_produced(case):
    """``count()`` must agree with the number of rows the query yields.

    A projection or a pushed-down filter must not change the answer: the count is
    a property of the query, not of how much of it was pushed into the reader.
    """
    make_ds = _COUNT_CASES[case]
    expected = len(make_ds().take_all())
    assert make_ds().count() == expected


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_empty_projection_preserves_row_count():
    """Selecting zero columns must yield N rows of no columns, not zero rows.

    ``Dataset.count()`` projects to zero columns to avoid reading column data, so
    an empty projection that drops rows silently corrupts every count built that
    way.
    """
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        selected_fields=(),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    expected = _iceberg_scan_row_count()
    assert expected > 0, "fixture table should not be empty"

    rows_read = sum(
        block.num_rows
        for read_task in iceberg_ds.get_read_tasks(1)
        for block in read_task()
    )
    assert rows_read == expected


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
@pytest.mark.parametrize(
    ("row_filter", "count_must_be_exact"),
    [
        (None, True),
        # Prunes whole files, so the manifest row counts stay exact and must
        # keep being reported -- this is the free ``count()`` we do not want to
        # regress while fixing the case below.
        (pyi_expr.In("col_c", {1, 2}), True),
        # Selective *within* a file: file-level pruning cannot resolve it, so a
        # manifest row count would overcount and must be reported as unknown.
        (pyi_expr.LessThan("col_a", 10), False),
    ],
    ids=["no_filter", "partition_filter", "row_level_filter"],
)
def test_reported_num_rows_matches_rows_read(row_filter, count_must_be_exact):
    """Read-task metadata must never claim a row count the read does not deliver.

    ``Dataset.count()`` returns this number directly when the plan has nothing
    that could change the row count, so an overcount here reaches the user as
    the answer. ``None`` means "unknown" and makes ``count()`` do the read, so it
    is always safe -- but it also gives up a free count, hence
    ``count_must_be_exact`` pinning the cases where the shortcut is sound.
    """
    kwargs = {} if row_filter is None else {"row_filter": row_filter}
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        **kwargs,
    )
    read_tasks = iceberg_ds.get_read_tasks(2)

    claimed = [read_task.metadata.num_rows for read_task in read_tasks]
    actual = sum(block.num_rows for read_task in read_tasks for block in read_task())

    if count_must_be_exact:
        assert all(count is not None for count in claimed), (
            "row counts are exact for this filter and must still be reported, "
            "otherwise count() pays for a read it does not need"
        )
    if all(count is not None for count in claimed):
        assert sum(claimed) == actual


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
@pytest.mark.parametrize(
    "push_down",
    [
        lambda ds: ds.apply_predicate(col("col_a") < 10),
        lambda ds: ds.apply_predicate(col("col_a") < 10).apply_projection(
            {"col_b": "col_b"}
        ),
    ],
    ids=["predicate", "predicate_then_projection"],
)
def test_pushdown_does_not_inherit_stale_plan_files(push_down):
    """A pushdown clone must re-plan its scan, not inherit the cache.

    ``apply_predicate`` and ``apply_projection`` shallow-copy the datasource, so
    a plan-file cache populated beforehand -- by ``estimate_inmemory_data_size``,
    say, which Ray calls to autodetect parallelism -- would be shared with the
    clone. Those tasks were planned without the predicate, so every residual is
    ``AlwaysTrue`` and ``get_read_tasks`` would report the unfiltered manifest
    total: exactly the overcount this file's other tests pin against.

    Reading the cache twice also has to keep working. ``DataScan.plan_files`` is
    annotated ``Iterable[FileScanTask]``, so a future PyIceberg returning a
    generator would otherwise leave the second read empty -- silently yielding
    zero read tasks and an empty dataset.
    """
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Warm the cache before pushdown, and read it twice.
    assert iceberg_ds.estimate_inmemory_data_size() > 0
    assert iceberg_ds.estimate_inmemory_data_size() > 0, "cache must be re-readable"
    unfiltered_files = len(iceberg_ds.plan_files)
    assert unfiltered_files > 0, "fixture should plan at least one file"

    filtered_ds = push_down(iceberg_ds)
    read_tasks = filtered_ds.get_read_tasks(2)
    assert read_tasks, "pushdown must not leave the clone with an exhausted cache"

    claimed = [read_task.metadata.num_rows for read_task in read_tasks]
    actual = sum(block.num_rows for read_task in read_tasks for block in read_task())

    assert actual == 10, f"filter should match 10 of the fixture's rows, got {actual}"
    if all(count is not None for count in claimed):
        assert (
            sum(claimed) == actual
        ), "reported row count came from plan files that predate the predicate"


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_read_iceberg_does_not_mutate_caller_kwargs():
    """The caller's dicts belong to the caller.

    Both dicts were stored by reference. ``catalog_kwargs`` is then
    ``pop("name")``-ed, so the caller loses the catalog name and a second read of
    the same table raises ``NoSuchTableError``. ``scan_kwargs`` has
    ``snapshot_id`` written into it, which fails silently instead: a later read
    reusing the dict inherits the pin and returns rows from a stale snapshot.
    """
    catalog_kwargs = _CATALOG_KWARGS.copy()
    scan_kwargs = {}
    expected_catalog_kwargs = catalog_kwargs.copy()

    # An explicit ``snapshot_id`` is required to reach the ``scan_kwargs`` write:
    # the buggy line is guarded by ``if snapshot_id``, so passing ``None`` would
    # leave the dict untouched whether or not the bug is present.
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS.copy())
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    snapshot_id = table.current_snapshot().snapshot_id

    first = read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=catalog_kwargs,
        scan_kwargs=scan_kwargs,
        snapshot_id=snapshot_id,
    )
    assert catalog_kwargs == expected_catalog_kwargs
    assert scan_kwargs == {}

    # Reusing the same dicts must work exactly like the first read.
    second = read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=catalog_kwargs,
        scan_kwargs=scan_kwargs,
    )
    assert second.count() == first.count()


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_read_basic():

    row_filter = pyi_expr.In("col_c", {1, 2, 3, 4, 5, 6, 7, 8})

    ray_ds = read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        row_filter=row_filter,
        selected_fields=("col_a", "col_b"),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    table: pa.Table = pa.concat_tables((ray.get(ref) for ref in ray_ds.to_arrow_refs()))

    expected_schema = pa.schema(
        [pa.field("col_a", pa.int32()), pa.field("col_b", pa.string())]
    )
    assert table.schema.equals(expected_schema)

    # Read the raw table from PyIceberg
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    orig_table_p = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(row_filter=row_filter, selected_fields=("col_a", "col_b"))
        .to_pandas()
        .sort_values(["col_a", "col_b"])
        .reset_index(drop=True)
    )

    # Actually compare the tables now
    table_p = ray_ds.to_pandas().sort_values(["col_a", "col_b"]).reset_index(drop=True)
    orig_table_p = orig_table_p.astype(table_p.dtypes.to_dict())
    pd.testing.assert_frame_equal(orig_table_p, table_p)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_basic():

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    ds = ray.data.from_arrow(create_pa_table())
    ds.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Read the raw table from PyIceberg after writing
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    orig_table_p = (
        table.scan()
        .to_pandas()
        .sort_values(["col_a", "col_b", "col_c"])
        .reset_index(drop=True)
    )

    table_p = (
        ds.to_pandas().sort_values(["col_a", "col_b", "col_c"]).reset_index(drop=True)
    )
    assert rows_same(table_p, orig_table_p)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_concurrency():

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()

    data = pd.DataFrame(
        {
            "col_a": np.array([1, 2, 3, 4], dtype=np.int32),
            "col_b": ["1", "2", "3", "4"],
            "col_c": np.array([1, 2, 3, 4], dtype=np.int32),
        }
    )
    write_ds = ray.data.from_pandas(data).repartition(2)
    write_ds.write_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        concurrency=2,
    )
    read_ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        selected_fields=("col_a",),
    )
    df = read_ds.to_pandas().sort_values("col_a").reset_index(drop=True)
    assert df["col_a"].tolist() == [1, 2, 3, 4]


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_predicate_pushdown():
    """Test that predicate pushdown works correctly with Iceberg datasource."""
    # Read the table and apply filters using Ray Data expressions
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Apply filter using Ray Data expression syntax
    filtered_ds = ds.filter(expr=col("col_c") >= 5)

    # Verify the filter is pushed down to the read operation
    # by checking the optimized logical plan
    logical_plan = filtered_ds._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # The plan should only contain the Read operator, with no Filter operator
    # This indicates the filter was pushed down to the datasource
    assert not _has_operator_type(
        optimized_plan, Filter
    ), f"Filter should be pushed down to read, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results are correct
    result = filtered_ds.to_pandas()
    assert all(result["col_c"] >= 5), "All rows should have col_c >= 5"
    assert len(result) > 0, "Should have some results"

    # Verify against direct PyIceberg read with the same filter
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    expected_table = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(row_filter=pyi_expr.GreaterThanOrEqual("col_c", 5))
        .to_pandas()
    )

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_predicate_pushdown_with_initial_filter():
    """Test that predicate pushdown works when combined with initial row_filter."""
    # Read with an initial PyIceberg filter
    initial_filter = pyi_expr.LessThan("col_a", 50)

    # Expect deprecation warning for row_filter
    with pytest.warns(DeprecationWarning, match="row_filter.*deprecated"):
        ds = ray.data.read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            row_filter=initial_filter,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )

    # Apply additional filter using Ray Data expression
    filtered_ds = ds.filter(expr=col("col_c") >= 5)

    # Verify both filters are pushed down
    logical_plan = filtered_ds._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # No Filter operator should remain in the plan
    assert not _has_operator_type(
        optimized_plan, Filter
    ), f"Filters should be pushed down to read, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results satisfy both conditions
    result = filtered_ds.to_pandas()
    assert all(result["col_a"] < 50), "All rows should have col_a < 50"
    assert all(result["col_c"] >= 5), "All rows should have col_c >= 5"
    assert len(result) > 0, "Should have some results"

    # Verify against direct PyIceberg read with combined filter
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    combined_filter = pyi_expr.And(
        pyi_expr.LessThan("col_a", 50), pyi_expr.GreaterThanOrEqual("col_c", 5)
    )
    expected_table = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(row_filter=combined_filter)
        .to_pandas()
    )

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_projection_pushdown():
    """Test that projection pushdown works correctly with Iceberg datasource."""
    # Read the table and apply projection using select
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Select only specific columns
    projected_ds = ds.select_columns(["col_a", "col_c"])

    # Verify the projection is pushed down to the read operation
    logical_plan = projected_ds._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # The plan should only contain the Read operator, with no Project operator
    # This indicates the projection was pushed down to the datasource
    assert not _has_operator_type(
        optimized_plan, Project
    ), f"Projection should be pushed down to read, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results only contain the selected columns
    result = projected_ds.to_pandas()
    assert set(result.columns) == {
        "col_a",
        "col_c",
    }, f"Expected only col_a and col_c, got: {result.columns}"
    assert len(result) > 0, "Should have some results"

    # Verify against direct PyIceberg read with the same projection
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    expected_table = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(selected_fields=("col_a", "col_c"))
        .to_pandas()
    )

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
@pytest.mark.parametrize(
    "selected_cols,filter_expr,pyi_filter,expected_cols",
    [
        # Test 1: Projection only on col_a
        (["col_a"], None, None, {"col_a"}),
        # Test 2: Projection on col_a and col_b with filter on col_c
        (
            ["col_a", "col_b"],
            col("col_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"col_a", "col_b"},
        ),
        # Test 3: Projection on all columns with filter
        (
            ["col_a", "col_b", "col_c"],
            col("col_a") < 50,
            pyi_expr.LessThan("col_a", 50),
            {"col_a", "col_b", "col_c"},
        ),
        # Test 4: Single column projection with complex filter
        (
            ["col_b"],
            (col("col_c") >= 3) & (col("col_c") <= 7),
            pyi_expr.And(
                pyi_expr.GreaterThanOrEqual("col_c", 3),
                pyi_expr.LessThanOrEqual("col_c", 7),
            ),
            {"col_b"},
        ),
    ],
)
def test_projection_and_predicate_pushdown(
    selected_cols, filter_expr, pyi_filter, expected_cols
):
    """Test that both projection and predicate pushdown work together."""
    # Read the table
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Apply projection
    projected_ds = ds.select_columns(selected_cols)

    # Apply filter if provided
    if filter_expr is not None:
        filtered_ds = projected_ds.filter(expr=filter_expr)
    else:
        filtered_ds = projected_ds

    # Verify both optimizations are applied
    logical_plan = filtered_ds._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # Both Filter and Project should be pushed down
    assert not _has_operator_type(
        optimized_plan, Filter
    ), f"Filter should be pushed down, got operators: {_get_operator_types(optimized_plan)}"
    assert not _has_operator_type(
        optimized_plan, Project
    ), f"Projection should be pushed down, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results
    result = filtered_ds.to_pandas()
    assert (
        set(result.columns) == expected_cols
    ), f"Expected columns {expected_cols}, got: {result.columns}"

    # Verify results match direct PyIceberg query
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    if pyi_filter is not None:
        expected_table = table.scan(
            row_filter=pyi_filter, selected_fields=tuple(selected_cols)
        ).to_pandas()
    else:
        expected_table = table.scan(selected_fields=tuple(selected_cols)).to_pandas()

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
@pytest.mark.parametrize(
    "rename_map,select_cols,filter_expr,pyi_filter,expected_cols",
    [
        # Test 1: Just rename
        (
            {"col_a": "column_a", "col_b": "column_b"},
            None,
            None,
            None,
            {"column_a", "column_b", "col_c"},
        ),
        # Test 2: Just select (no rename, no filter)
        (
            None,
            ["col_a", "col_c"],
            None,
            None,
            {"col_a", "col_c"},
        ),
        # Test 3: Just filter (no rename, no select)
        (
            None,
            None,
            col("col_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"col_a", "col_b", "col_c"},
        ),
        # Test 4: Rename + Select
        (
            {"col_a": "column_a"},
            ["column_a", "col_b"],
            None,
            None,
            {"column_a", "col_b"},
        ),
        # Test 5: Rename + Filter
        (
            {"col_a": "column_a", "col_c": "column_c"},
            None,
            col("column_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"column_a", "col_b", "column_c"},
        ),
        # Test 6: Select + Filter (different columns)
        (
            None,
            ["col_a", "col_b"],
            col("col_c") >= 5,
            pyi_expr.GreaterThanOrEqual("col_c", 5),
            {"col_a", "col_b"},
        ),
        # Test 7: Rename + Select + Filter (all three together).
        # Filter references a selected column (the renamed ``column_a``);
        # filtering on a column the select dropped is not a valid plan
        # after projection pushdown.
        (
            {"col_a": "column_a", "col_b": "column_b"},
            ["column_a", "column_b"],
            col("column_a") >= 50,
            pyi_expr.GreaterThanOrEqual("col_a", 50),
            {"column_a", "column_b"},
        ),
        # Test 8: Complex rename + select with multiple renames
        (
            {"col_a": "id", "col_b": "name", "col_c": "value"},
            ["id", "value"],
            None,
            None,
            {"id", "value"},
        ),
        # Test 9: Rename + Select + Complex filter
        (
            {"col_a": "id", "col_c": "value"},
            ["id", "value"],
            (col("value") >= 3) & (col("value") <= 7),
            pyi_expr.And(
                pyi_expr.GreaterThanOrEqual("col_c", 3),
                pyi_expr.LessThanOrEqual("col_c", 7),
            ),
            {"id", "value"},
        ),
    ],
)
def test_rename_select_filter_combinations(
    rename_map, select_cols, filter_expr, pyi_filter, expected_cols
):
    """Test all combinations of rename_columns, select_columns, and filter operations."""
    # Read the table
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Apply rename if provided
    if rename_map is not None:
        ds = ds.rename_columns(rename_map)

    # Apply select if provided
    if select_cols is not None:
        ds = ds.select_columns(select_cols)

    # Apply filter if provided
    if filter_expr is not None:
        ds = ds.filter(expr=filter_expr)

    # Verify optimizations are applied
    logical_plan = ds._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    # Filter, when present, should always be pushed into the scan.
    if filter_expr is not None:
        assert not _has_operator_type(
            optimized_plan, Filter
        ), f"Filter should be pushed down, got operators: {_get_operator_types(optimized_plan)}"

    # Pure column selection (no renames) is subsumed by the scan's column
    # pruning, so no ``Project`` remains. Renames stay as a ``Project`` of
    # ``AliasExpr``s above the pruned scan after filter pushdown.
    if rename_map is not None:
        assert _has_operator_type(
            optimized_plan, Project
        ), f"Renames should remain as a Project above the scan, got operators: {_get_operator_types(optimized_plan)}"
    elif select_cols is not None:
        assert not _has_operator_type(
            optimized_plan, Project
        ), f"Pure column selection should be pushed into the scan, got operators: {_get_operator_types(optimized_plan)}"

    # Verify the results
    result = ds.to_pandas()
    assert (
        set(result.columns) == expected_cols
    ), f"Expected columns {expected_cols}, got: {result.columns}"
    assert len(result) > 0, "Should have some results"

    # Verify results match direct PyIceberg query
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    # Build the column mapping for verification
    # We need to map renamed columns back to original names for PyIceberg
    reverse_rename_map = {}
    if rename_map is not None:
        reverse_rename_map = {v: k for k, v in rename_map.items()}

    # Determine which original columns to select
    if select_cols is not None:
        # Map selected columns back to original names
        original_cols = tuple(reverse_rename_map.get(col, col) for col in select_cols)
    else:
        # All columns
        original_cols = ("col_a", "col_b", "col_c")

    # Get expected data from PyIceberg
    if pyi_filter is not None:
        expected_table = table.scan(
            row_filter=pyi_filter, selected_fields=original_cols
        ).to_pandas()
    else:
        expected_table = table.scan(selected_fields=original_cols).to_pandas()

    # Apply renames to expected table to match result
    if rename_map is not None:
        cols_to_rename = {
            orig: new
            for orig, new in rename_map.items()
            if orig in expected_table.columns
        }
        expected_table = expected_table.rename(columns=cols_to_rename)

    assert rows_same(result, expected_table)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_predicate_pushdown_complex_expression():
    """Test predicate pushdown with complex expressions."""
    # Apply a complex filter expression
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    # Complex expression: (col_c >= 3) & (col_c <= 7) & (col_a <= 50)
    filtered_ds = (
        ds.filter(expr=(col("col_c") >= 3))
        .filter(expr=col("col_c") <= 7)
        .filter(expr=col("col_a") <= 50)
    )

    # Verify the results
    result = filtered_ds.to_pandas()

    # Verify optimizations are applied
    logical_plan = filtered_ds._logical_plan
    optimized_plan = LogicalOptimizer().optimize(logical_plan)

    assert not _has_operator_type(
        optimized_plan, Filter
    ), f"Filter should be pushed down, got operators: {_get_operator_types(optimized_plan)}"
    assert not _has_operator_type(
        optimized_plan, Project
    ), f"Projection should be pushed down, got operators: {_get_operator_types(optimized_plan)}"

    assert all(result["col_c"] >= 3), "All rows should have col_c >= 3"
    assert all(result["col_c"] <= 7), "All rows should have col_c <= 7"
    assert all(result["col_a"] <= 50), "All rows should have col_a <= 50"
    assert len(result) > 0, "Should have some results"

    # Verify against direct PyIceberg read
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    combined_filter = pyi_expr.And(
        pyi_expr.And(
            pyi_expr.GreaterThanOrEqual("col_c", 3),
            pyi_expr.LessThanOrEqual("col_c", 7),
        ),
        pyi_expr.LessThanOrEqual("col_a", 50),
    )
    expected_table = (
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
        .scan(row_filter=combined_filter)
        .to_pandas()
    )

    assert rows_same(result, expected_table)


# Helper functions and fixtures for schema evolution tests


@pytest.fixture
def clean_table() -> Generator[Tuple[Catalog, Table], None, None]:
    """Pytest fixture to get a clean Iceberg table by deleting all data."""
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    table.delete()
    yield sql_catalog, table


def _create_typed_dataframe(data_dict: Dict[str, List[Any]]) -> pd.DataFrame:
    """Create a pandas DataFrame with proper int32 dtypes for col_a and col_c."""
    df = pd.DataFrame(data_dict)
    if "col_a" in df.columns:
        # Use nullable Int32 to support NaN values
        df["col_a"] = df["col_a"].astype("Int32")
    if "col_c" in df.columns:
        # Use nullable Int32 to support NaN values
        df["col_c"] = df["col_c"].astype("Int32")
    # Cast object/string columns to a nullable string dtype so ``None`` is
    # represented as ``<NA>``, matching the Arrow-backed ``string[pyarrow]``
    # produced by ``read_iceberg().to_pandas()``.
    for column in df.columns:
        if df[column].dtype == object:
            df[column] = df[column].astype("string")
    return df


def _write_to_iceberg(df: pd.DataFrame, **kwargs: Any) -> None:
    """Write a DataFrame to the test Iceberg table."""
    ds = ray.data.from_pandas(df)
    write_kwargs: Dict[str, Any] = {
        "table_identifier": f"{_DB_NAME}.{_TABLE_NAME}",
        "catalog_kwargs": _CATALOG_KWARGS.copy(),
    }
    write_kwargs.update(kwargs)
    ds.write_iceberg(**write_kwargs)


def _read_from_iceberg(
    sort_by: "str | List[str] | None" = None,
) -> pd.DataFrame:
    """Read data from the test Iceberg table and optionally sort."""
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    result_df = ds.to_pandas()
    if sort_by:
        result_df = result_df.sort_values(sort_by).reset_index(drop=True)
    return result_df


def _verify_schema(expected_fields: Dict[str, Type[pyi_types.IcebergType]]) -> None:
    """
    Verify the Iceberg table schema matches expected fields.

    Args:
        expected_fields: Dict mapping field names to PyIceberg type classes
                        e.g., {"col_a": pyi_types.IntegerType, "col_b": pyi_types.StringType}
    """
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    schema = {field.name: field.field_type for field in table.schema().fields}

    assert len(schema) == len(
        expected_fields
    ), f"Expected {len(expected_fields)} fields, got {len(schema)}"

    for field_name, expected_type in expected_fields.items():
        assert field_name in schema, f"Field {field_name} not found in schema"
        assert isinstance(schema[field_name], expected_type), (
            f"Field {field_name} expected type {expected_type}, "
            f"got {type(schema[field_name])}"
        )


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestSchemaEvolution:
    """Test schema evolution during writes."""

    def test_schema_evolution_add_column(self, clean_table):
        """Test adding new columns works."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["row_1", "row_2"], "col_c": [1, 2]}
        )
        _write_to_iceberg(initial_data)

        new_data = _create_typed_dataframe(
            {
                "col_a": [3, 4],
                "col_b": ["row_3", "row_4"],
                "col_c": [3, 4],
                "col_d": ["extra_3", "extra_4"],
            }
        )
        _write_to_iceberg(new_data)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["row_1", "row_2", "row_3", "row_4"],
                "col_c": [1, 2, 3, 4],
                "col_d": [None, None, "extra_3", "extra_4"],
            }
        )
        assert rows_same(result_df, expected)

    def test_multiple_schema_evolutions(self, clean_table):
        """Test multiple sequential schema evolutions."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1], "col_b": ["row_1"], "col_c": [10]}
        )
        _write_to_iceberg(initial_data)

        # First evolution: add col_d
        data_with_d = _create_typed_dataframe(
            {"col_a": [2], "col_b": ["row_2"], "col_c": [20], "col_d": ["extra_2"]}
        )
        _write_to_iceberg(data_with_d)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        # Second evolution: add col_e
        data_with_e = _create_typed_dataframe(
            {
                "col_a": [3],
                "col_b": ["row_3"],
                "col_c": [30],
                "col_d": ["extra_3"],
                "col_e": ["bonus_3"],
            }
        )
        _write_to_iceberg(data_with_e)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
                "col_e": pyi_types.StringType,
            }
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["row_1", "row_2", "row_3"],
                "col_c": [10, 20, 30],
                "col_d": [None, "extra_2", "extra_3"],
                "col_e": [None, None, "bonus_3"],
            }
        )
        assert rows_same(result_df, expected)

    def test_column_order_independence(self, clean_table):
        """Test writing data with columns in different order works."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["row_1", "row_2"], "col_c": [1, 2]}
        )
        _write_to_iceberg(initial_data)

        # Append data with columns in different order
        reordered_data = _create_typed_dataframe(
            {"col_c": [3, 4], "col_a": [3, 4], "col_b": ["row_3", "row_4"]}
        )
        _write_to_iceberg(reordered_data)

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["row_1", "row_2", "row_3", "row_4"],
                "col_c": [1, 2, 3, 4],
            }
        )
        assert rows_same(result_df, expected)

    @pytest.mark.parametrize(
        "initial_dtype,promoted_dtype,promoted_iceberg_type",
        [
            (np.int32, np.int64, pyi_types.LongType),
            (np.float32, np.float64, pyi_types.DoubleType),
        ],
        ids=["int32_to_int64", "float32_to_float64"],
    )
    def test_schema_evolution_type_promotion(
        self, clean_table, initial_dtype, promoted_dtype, promoted_iceberg_type
    ):
        """Test type promotion (int32 -> int64, float32 -> float64) works."""
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [1, 2]}
        )
        initial_data["col_new"] = np.array([10, 20], dtype=initial_dtype)
        _write_to_iceberg(initial_data)

        new_data = _create_typed_dataframe(
            {"col_a": [3, 4], "col_b": ["c", "d"], "col_c": [3, 4]}
        )
        new_data["col_new"] = np.array([30, 40], dtype=promoted_dtype)
        _write_to_iceberg(new_data)

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_new": promoted_iceberg_type,
            }
        )

        result_df = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["a", "b", "c", "d"],
                "col_c": [1, 2, 3, 4],
            }
        )
        expected["col_new"] = np.array([10, 20, 30, 40], dtype=promoted_dtype)
        assert rows_same(result_df, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestOverwriteMode:
    """Test overwrite mode functionality."""

    def test_write_overwrite_full(self, clean_table):
        """Test full table overwrite replaces all data."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(initial_data)

        # Verify initial write
        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        assert rows_same(result, expected)

        # Overwrite with new data
        new_data = _create_typed_dataframe(
            {"col_a": [100, 200], "col_b": ["x", "y"], "col_c": [1, 2]}
        )
        _write_to_iceberg(new_data, mode=SaveMode.OVERWRITE)

        # Verify overwrite replaced all data
        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [100, 200], "col_b": ["x", "y"], "col_c": [1, 2]}
        )
        assert rows_same(result, expected)

    def test_write_overwrite_with_filter(self, clean_table):
        """Test partial overwrite using a filter expression."""
        from ray.data import SaveMode

        # Write initial data with different col_c values
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5, 6],
                "col_b": ["a", "b", "c", "d", "e", "f"],
                "col_c": [1, 1, 2, 2, 3, 3],
            }
        )
        _write_to_iceberg(initial_data)

        # Overwrite only rows where col_c == 2
        new_data = _create_typed_dataframe(
            {"col_a": [30, 40], "col_b": ["new_c", "new_d"], "col_c": [2, 2]}
        )
        _write_to_iceberg(
            new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_c") == 2
        )

        # Verify: rows with col_c == 2 should be replaced, others unchanged
        result = _read_from_iceberg(sort_by="col_a")

        # Expected: rows with col_c in {1, 3} unchanged, col_c == 2 rows replaced
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 5, 6, 30, 40],
                "col_b": ["a", "b", "e", "f", "new_c", "new_d"],
                "col_c": [1, 1, 3, 3, 2, 2],
            }
        )
        assert rows_same(result, expected)

    def test_write_overwrite_with_complex_filter(self, clean_table):
        """Test partial overwrite with a complex filter expression."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": ["a", "b", "c", "d", "e"],
                "col_c": [1, 2, 3, 4, 5],
            }
        )
        _write_to_iceberg(initial_data)

        # Overwrite rows where col_c >= 3 AND col_c <= 4
        new_data = _create_typed_dataframe(
            {"col_a": [30, 40], "col_b": ["new_c", "new_d"], "col_c": [3, 4]}
        )
        _write_to_iceberg(
            new_data,
            mode=SaveMode.OVERWRITE,
            overwrite_filter=(col("col_c") >= 3) & (col("col_c") <= 4),
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 5, 30, 40],
                "col_b": ["a", "b", "e", "new_c", "new_d"],
                "col_c": [1, 2, 5, 3, 4],
            }
        )
        assert rows_same(result, expected)

    def test_write_overwrite_empty_result(self, clean_table):
        """Test overwrite with filter that matches no rows just appends."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [1, 2, 3]}
        )
        _write_to_iceberg(initial_data)

        # Overwrite with filter that matches nothing
        new_data = _create_typed_dataframe(
            {"col_a": [100], "col_b": ["new"], "col_c": [999]}
        )
        _write_to_iceberg(
            new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_c") == 999
        )

        # Original data should remain plus new data
        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 100],
                "col_b": ["a", "b", "c", "new"],
                "col_c": [1, 2, 3, 999],
            }
        )
        assert rows_same(result, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestUpsertMode:
    """Test upsert mode functionality."""

    def test_write_upsert_basic(self, clean_table):
        """Test basic upsert with explicit join columns."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(initial_data)

        # Upsert: update col_a=2, insert col_a=4
        upsert_data = _create_typed_dataframe(
            {"col_a": [2, 4], "col_b": ["updated_b", "new_d"], "col_c": [25, 40]}
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["a", "updated_b", "c", "new_d"],
                "col_c": [10, 25, 30, 40],
            }
        )
        assert rows_same(result, expected)

    def test_write_upsert_multiple_join_cols(self, clean_table):
        """Test upsert with multiple join columns."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 1, 2, 2],
                "col_b": ["a", "b", "a", "b"],
                "col_c": [10, 20, 30, 40],
            }
        )
        _write_to_iceberg(initial_data)

        # Upsert using (col_a, col_b) as composite key
        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["b", "a", "c"],
                "col_c": [25, 35, 50],  # Update (1,b) and (2,a), insert (3,c)
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a", "col_b"]},
        )

        result = _read_from_iceberg(sort_by=["col_a", "col_b"])
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 1, 2, 2, 3],
                "col_b": ["a", "b", "a", "b", "c"],
                "col_c": [10, 25, 35, 40, 50],
            }
        )
        assert rows_same(result, expected)

    def test_write_upsert_all_updates(self, clean_table):
        """Test upsert where all rows are updates (no inserts)."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(initial_data)

        # Upsert all existing rows
        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["updated_a", "updated_b", "updated_c"],
                "col_c": [100, 200, 300],
            }
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["updated_a", "updated_b", "updated_c"],
                "col_c": [100, 200, 300],
            }
        )
        assert rows_same(result, expected)

    def test_write_upsert_all_inserts(self, clean_table):
        """Test upsert where all rows are inserts (no updates)."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(initial_data)

        # Upsert all new rows
        upsert_data = _create_typed_dataframe(
            {"col_a": [4, 5, 6], "col_b": ["d", "e", "f"], "col_c": [40, 50, 60]}
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5, 6],
                "col_b": ["a", "b", "c", "d", "e", "f"],
                "col_c": [10, 20, 30, 40, 50, 60],
            }
        )
        assert rows_same(result, expected)

    def test_write_upsert_empty_table(self, clean_table):
        """Test upsert into empty table is equivalent to insert."""
        from ray.data import SaveMode

        # Table is already empty from fixture
        # Upsert data into empty table
        upsert_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        assert rows_same(result, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestUpsertScanMerge:
    """Test the scan-merge upsert algorithm for correctness.

    See ``IcebergDatasink._commit_upsert_scan_merge`` for algorithm details.
    """

    def test_upsert_preserves_rows_sparse_keys(self, clean_table):
        """Sparse upsert keys leave intermediate rows that must be preserved
        after the rewrite."""
        from ray.data import SaveMode

        seed = _create_typed_dataframe(
            {
                "col_a": list(range(1, 11)),
                "col_b": [f"seed_{i}" for i in range(1, 11)],
                "col_c": [1] * 10,
            }
        )
        _write_to_iceberg(seed)

        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 10],
                "col_b": ["updated_1", "updated_10"],
                "col_c": [1, 1],
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a"]},
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": list(range(1, 11)),
                "col_b": ["updated_1"]
                + [f"seed_{i}" for i in range(2, 10)]
                + ["updated_10"],
                "col_c": [1] * 10,
            }
        )
        assert rows_same(result, expected)

    def test_upsert_across_multiple_files(self, clean_table):
        """Two separate seed writes produce at least two data files. A sparse
        upsert that spans both files must preserve non-upsert rows in each."""
        from ray.data import SaveMode

        _write_to_iceberg(
            _create_typed_dataframe(
                {
                    "col_a": [1, 2, 3],
                    "col_b": ["seed_1", "seed_2", "seed_3"],
                    "col_c": [1, 1, 1],
                }
            )
        )
        _write_to_iceberg(
            _create_typed_dataframe(
                {
                    "col_a": [10, 11, 12],
                    "col_b": ["seed_10", "seed_11", "seed_12"],
                    "col_c": [1, 1, 1],
                }
            )
        )

        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 12],
                "col_b": ["updated_1", "updated_12"],
                "col_c": [1, 1],
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a"]},
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 10, 11, 12],
                "col_b": [
                    "updated_1",
                    "seed_2",
                    "seed_3",
                    "seed_10",
                    "seed_11",
                    "updated_12",
                ],
                "col_c": [1] * 6,
            }
        )
        assert rows_same(result, expected)

    def test_upsert_whole_file_delete_when_all_keys_match(self, clean_table):
        """When every seed row in the coarse range is in the upsert batch, the
        original file is wholly deleted and only upsert rows remain — no
        duplicates."""
        from ray.data import SaveMode

        seed = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": ["seed_1", "seed_2", "seed_3", "seed_4", "seed_5"],
                "col_c": [1] * 5,
            }
        )
        _write_to_iceberg(seed)

        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": [f"updated_{i}" for i in range(1, 6)],
                "col_c": [1] * 5,
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a"]},
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": [f"updated_{i}" for i in range(1, 6)],
                "col_c": [1] * 5,
            }
        )
        assert rows_same(result, expected)

    def test_upsert_pure_insert_short_circuit(self, clean_table):
        """Upsert keys outside the seed's coarse range hit zero candidate
        files; the new rows must still be appended."""
        from ray.data import SaveMode

        seed = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["seed_1", "seed_2", "seed_3"],
                "col_c": [1, 1, 1],
            }
        )
        _write_to_iceberg(seed)

        upsert_data = _create_typed_dataframe(
            {
                "col_a": [100, 101],
                "col_b": ["new_100", "new_101"],
                "col_c": [1, 1],
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a"]},
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 100, 101],
                "col_b": ["seed_1", "seed_2", "seed_3", "new_100", "new_101"],
                "col_c": [1] * 5,
            }
        )
        assert rows_same(result, expected)

    def test_upsert_composite_key_preserves_rows(self, clean_table):
        """Composite-key anti-join must match on all join columns; rows that
        share one column with an upsert key but not the full composite must be
        preserved."""
        from ray.data import SaveMode

        composites = [(a, b) for a in [1, 2, 3] for b in ["x", "y", "z"]]
        seed = _create_typed_dataframe(
            {
                "col_a": [a for a, _ in composites],
                "col_b": [b for _, b in composites],
                "col_c": [1] * len(composites),
            }
        )
        _write_to_iceberg(seed)

        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 3],
                "col_b": ["x", "z"],
                "col_c": [99, 99],
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a", "col_b"]},
        )

        result = _read_from_iceberg(sort_by=["col_a", "col_b"])
        expected_col_c = [
            99 if (a, b) in {(1, "x"), (3, "z")} else 1 for a, b in sorted(composites)
        ]
        expected = _create_typed_dataframe(
            {
                "col_a": [a for a, _ in sorted(composites)],
                "col_b": [b for _, b in sorted(composites)],
                "col_c": expected_col_c,
            }
        )
        assert rows_same(result, expected)

    def test_upsert_string_key(self, clean_table):
        """String join column exercises the type-cast path in
        _rewrite_iceberg_file that aligns utf8 / large_utf8 between the file
        batch and the upsert-keys table."""
        from ray.data import SaveMode

        seed = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": ["a", "b", "c", "d", "e"],
                "col_c": [1] * 5,
            }
        )
        _write_to_iceberg(seed)

        upsert_data = _create_typed_dataframe(
            {
                "col_a": [10, 20],
                "col_b": ["a", "e"],
                "col_c": [1, 1],
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_b"]},
        )

        result = _read_from_iceberg(sort_by="col_b")
        expected = _create_typed_dataframe(
            {
                "col_a": [10, 2, 3, 4, 20],
                "col_b": ["a", "b", "c", "d", "e"],
                "col_c": [1] * 5,
            }
        )
        assert rows_same(result, expected)

    def test_upsert_case_insensitive_join_cols(self, clean_table):
        """``case_sensitive=False`` should let join_cols match table columns
        whose casing differs from the supplied names."""
        from ray.data import SaveMode

        seed = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5],
                "col_b": ["seed_1", "seed_2", "seed_3", "seed_4", "seed_5"],
                "col_c": [1] * 5,
            }
        )
        _write_to_iceberg(seed)

        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 5, 6],
                "col_b": ["updated_1", "updated_5", "new_6"],
                "col_c": [1, 1, 1],
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["COL_A"], "case_sensitive": False},
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5, 6],
                "col_b": [
                    "updated_1",
                    "seed_2",
                    "seed_3",
                    "seed_4",
                    "updated_5",
                    "new_6",
                ],
                "col_c": [1] * 6,
            }
        )
        assert rows_same(result, expected)

    def test_upsert_with_new_column(self, clean_table):
        """Upsert that introduces a new column must evolve the table schema,
        populate the new column for upserted rows, and leave NULLs for
        untouched seed rows (including preserved rows rewritten during upsert)."""
        from ray.data import SaveMode

        seed = _create_typed_dataframe(
            {
                "col_a": list(range(1, 6)),
                "col_b": [f"seed_{i}" for i in range(1, 6)],
                "col_c": [1] * 5,
            }
        )
        _write_to_iceberg(seed)

        # Upsert touches col_a=1 and col_a=5 (preserved rows at 2, 3, 4 in the
        # same file) and introduces a new column ``col_d``.
        upsert_data = _create_typed_dataframe(
            {
                "col_a": [1, 5, 6],
                "col_b": ["updated_1", "updated_5", "new_6"],
                "col_c": [1, 1, 1],
                "col_d": ["d_1", "d_5", "d_6"],
            }
        )
        _write_to_iceberg(
            upsert_data,
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a"]},
        )

        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4, 5, 6],
                "col_b": [
                    "updated_1",
                    "seed_2",
                    "seed_3",
                    "seed_4",
                    "updated_5",
                    "new_6",
                ],
                "col_c": [1] * 6,
                "col_d": ["d_1", None, None, None, "d_5", "d_6"],
            }
        )
        assert rows_same(result, expected)


@pytest.fixture
def table_with_identifier_fields() -> Generator[Tuple[Catalog, Table], None, None]:
    """Pytest fixture to create a table with identifier fields for upsert tests."""
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)

    # Drop existing table if it exists
    identifier_table_name = "identifier_test"
    if (_DB_NAME, identifier_table_name) in sql_catalog.list_tables(_DB_NAME):
        sql_catalog.drop_table(f"{_DB_NAME}.{identifier_table_name}")

    # Create table with identifier fields (col_a is the key)
    table = sql_catalog.create_table(
        f"{_DB_NAME}.{identifier_table_name}",
        schema=pyi_schema.Schema(
            pyi_types.NestedField(
                field_id=1,
                name="col_a",
                field_type=pyi_types.IntegerType(),
                required=True,
            ),
            pyi_types.NestedField(
                field_id=2,
                name="col_b",
                field_type=pyi_types.StringType(),
                required=False,
            ),
            pyi_types.NestedField(
                field_id=3,
                name="col_c",
                field_type=pyi_types.IntegerType(),
                required=False,
            ),
            identifier_field_ids=[1],  # col_a is the identifier field
        ),
    )

    yield sql_catalog, table, identifier_table_name

    # Cleanup
    if (_DB_NAME, identifier_table_name) in sql_catalog.list_tables(_DB_NAME):
        sql_catalog.drop_table(f"{_DB_NAME}.{identifier_table_name}")


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestUpsertWithIdentifierFields:
    """Test upsert using table's identifier fields."""

    def test_upsert_uses_table_identifier_fields(self, table_with_identifier_fields):
        """Test upsert without explicit join_cols uses table's identifier fields."""
        from ray.data import SaveMode

        sql_catalog, table, table_name = table_with_identifier_fields
        table_identifier = f"{_DB_NAME}.{table_name}"

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        ds = ray.data.from_pandas(initial_data)
        ds.write_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )

        # Upsert without join_cols - should use identifier fields from table schema
        upsert_data = _create_typed_dataframe(
            {"col_a": [2, 4], "col_b": ["updated_b", "new_d"], "col_c": [25, 40]}
        )
        ds_upsert = ray.data.from_pandas(upsert_data)
        ds_upsert.write_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
            mode=SaveMode.UPSERT,
        )

        # Read back and verify
        result = ray.data.read_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        ).to_pandas()
        result = result.sort_values("col_a").reset_index(drop=True)

        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["a", "updated_b", "c", "new_d"],
                "col_c": [10, 25, 30, 40],
            }
        )
        assert rows_same(result, expected)

    def test_upsert_explicit_join_cols_override_identifier_fields(
        self, table_with_identifier_fields
    ):
        """Test that explicit join_cols override table's identifier fields."""
        from ray.data import SaveMode

        sql_catalog, table, table_name = table_with_identifier_fields
        table_identifier = f"{_DB_NAME}.{table_name}"

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2, 3], "col_b": ["a", "b", "c"], "col_c": [10, 20, 30]}
        )
        ds = ray.data.from_pandas(initial_data)
        ds.write_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )

        # Upsert with explicit join_cols on col_b instead of table's col_a identifier
        upsert_data = _create_typed_dataframe(
            {"col_a": [100, 200], "col_b": ["b", "d"], "col_c": [25, 40]}
        )
        ds_upsert = ray.data.from_pandas(upsert_data)
        ds_upsert.write_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_b"]},
        )

        # Read back and verify - join on col_b means row with col_b="b" was updated
        result = ray.data.read_iceberg(
            table_identifier=table_identifier,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        ).to_pandas()
        result = result.sort_values("col_a").reset_index(drop=True)

        expected = _create_typed_dataframe(
            {
                "col_a": [1, 3, 100, 200],
                "col_b": ["a", "c", "b", "d"],
                "col_c": [10, 30, 25, 40],
            }
        )
        assert rows_same(result, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestUpsertAndOverwriteMissingJoinCols:
    """Test error handling for upsert without join columns."""

    def test_upsert_without_join_cols_or_identifier_fields_raises(self, clean_table):
        """Test that upsert without join_cols on table without identifier fields raises."""
        from ray.data import SaveMode

        # Write some initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [10, 20]}
        )
        _write_to_iceberg(initial_data)

        # Upsert without join_cols should raise since table has no identifier fields
        upsert_data = _create_typed_dataframe(
            {"col_a": [2, 3], "col_b": ["updated_b", "c"], "col_c": [25, 30]}
        )

        with pytest.raises(ValueError, match="join_cols"):
            _write_to_iceberg(upsert_data, mode=SaveMode.UPSERT)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
class TestSchemaEvolutionWithModes:
    """Test schema evolution with different write modes."""

    def test_schema_evolution_with_overwrite(self, clean_table):
        """Test schema evolution works with overwrite mode."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [10, 20]}
        )
        _write_to_iceberg(initial_data)

        # Overwrite with data that has a new column
        new_data = _create_typed_dataframe(
            {"col_a": [3, 4], "col_b": ["c", "d"], "col_c": [30, 40]}
        )
        new_data["col_d"] = ["extra_3", "extra_4"]
        _write_to_iceberg(new_data, mode=SaveMode.OVERWRITE)

        # Verify schema evolved
        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [3, 4], "col_b": ["c", "d"], "col_c": [30, 40]}
        )
        expected["col_d"] = ["extra_3", "extra_4"]
        assert rows_same(result, expected)

    def test_schema_evolution_with_upsert(self, clean_table):
        """Test schema evolution works with upsert mode."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [10, 20]}
        )
        _write_to_iceberg(initial_data)

        # Upsert with data that has a new column
        upsert_data = _create_typed_dataframe(
            {"col_a": [2, 3], "col_b": ["updated_b", "c"], "col_c": [25, 30]}
        )
        upsert_data["col_d"] = ["extra_2", "extra_3"]
        _write_to_iceberg(
            upsert_data, mode=SaveMode.UPSERT, upsert_kwargs={"join_cols": ["col_a"]}
        )

        # Verify schema evolved
        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3],
                "col_b": ["a", "updated_b", "c"],
                "col_c": [10, 25, 30],
                "col_d": [None, "extra_2", "extra_3"],
            }
        )
        assert rows_same(result, expected)

    def test_schema_evolution_with_partial_overwrite(self, clean_table):
        """Test schema evolution works with partial overwrite."""
        from ray.data import SaveMode

        # Write initial data
        initial_data = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["a", "b", "c", "d"],
                "col_c": [1, 1, 2, 2],
            }
        )
        _write_to_iceberg(initial_data)

        # Partial overwrite with new column for col_c == 2
        overwrite_data = _create_typed_dataframe(
            {"col_a": [30, 40], "col_b": ["new_c", "new_d"], "col_c": [2, 2]}
        )
        overwrite_data["col_d"] = ["extra_c", "extra_d"]
        _write_to_iceberg(
            overwrite_data,
            mode=SaveMode.OVERWRITE,
            overwrite_filter=col("col_c") == 2,
        )

        # Verify schema evolved
        _verify_schema(
            {
                "col_a": pyi_types.IntegerType,
                "col_b": pyi_types.StringType,
                "col_c": pyi_types.IntegerType,
                "col_d": pyi_types.StringType,
            }
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 30, 40],
                "col_b": ["a", "b", "new_c", "new_d"],
                "col_c": [1, 1, 2, 2],
                "col_d": [None, None, "extra_c", "extra_d"],
            }
        )
        assert rows_same(result, expected)


@pytest.mark.skipif(
    get_pyarrow_version() < parse_version("14.0.0"),
    reason="PyIceberg 0.7.0 fails on pyarrow <= 14.0.0",
)
def test_write_retry_on_transient_error(pyiceberg_table, fast_retry_config):
    """Test that transient errors during file writes trigger retries."""
    from unittest.mock import patch

    from ray.data._internal.datasource.iceberg_datasink import IcebergDatasink
    from ray.data._internal.execution.interfaces import TaskContext

    # Create datasink and initialize it
    datasink = IcebergDatasink(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    datasink.on_write_start()

    # Track call count to simulate transient failures
    call_count = {"count": 0}

    # Import original function before patching
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    original_func = _dataframe_to_data_files

    def flaky_dataframe_to_data_files(*args, **kwargs):
        call_count["count"] += 1
        if call_count["count"] <= 2:
            # Fail first 2 attempts with a retryable error
            raise IOError("TestTransientError: simulated transient failure")
        # Succeed on 3rd attempt
        return original_func(*args, **kwargs)

    # Create test data
    data = pa.Table.from_pydict(
        {"col_a": [200, 201], "col_b": ["x", "y"], "col_c": [5, 5]},
        schema=_SCHEMA,
    )

    # Patch at pyiceberg module level and call write directly
    with patch(
        "pyiceberg.io.pyarrow._dataframe_to_data_files",
        side_effect=flaky_dataframe_to_data_files,
    ):
        # Call write directly (bypassing Ray workers)
        task_ctx = TaskContext(task_idx=0, op_name="Write")
        result = datasink.write([data], task_ctx)

    # Verify retries occurred (called 3 times: 2 failures + 1 success)
    assert (
        call_count["count"] == 3
    ), f"Expected 3 calls (2 retries + 1 success), got {call_count['count']}"

    # Verify write result has data files
    assert len(result.data_files) > 0, "Expected data files in result"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
