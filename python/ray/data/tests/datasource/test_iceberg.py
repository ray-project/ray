import json
import os
import random
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, Dict, Generator, List, Tuple, Type

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg import (
    catalog as pyi_catalog,
    expressions as pyi_expr,
    schema as pyi_schema,
    types as pyi_types,
)
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.table import FileScanTask, Table
from pyiceberg.table.snapshots import (
    TOTAL_EQUALITY_DELETES,
    TOTAL_POSITION_DELETES,
    TOTAL_RECORDS,
)
from pyiceberg.transforms import IdentityTransform

import ray
from ray.data import read_iceberg
from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource.iceberg_datasource import (
    IcebergDatasource,
    _get_read_task,
)
from ray.data._internal.datasource.parquet_datasource import (
    PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
)
from ray.data._internal.datasource_v2.formats.iceberg.iceberg_utils import (
    _estimate_inmemory_file_size,
)
from ray.data._internal.logical.operators import Filter, Project, Read
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.object_extensions.arrow import ArrowPythonObjectArray
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor
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


@pytest.fixture(
    params=[(False, True), (True, True), (True, False)],
    ids=["reader_v1", "reader_v2", "reader_v2_one_call_per_task"],
)
def both_iceberg_readers(request, restore_data_context):
    """Run a read-path test against both the V1 and V2 Iceberg datasources.

    V2 runs twice: one file per ``ArrowScan`` call (the default), and the whole
    read task in one call (``read_file_tasks_sequentially = False``).
    ``restore_data_context`` puts the context back afterwards. Every assertion
    must hold for each, which is the parity bar.
    """
    reader_v2, one_file_per_call = request.param
    restore_data_context.use_datasource_v2 = reader_v2
    restore_data_context.iceberg_config.read_file_tasks_sequentially = one_file_per_call
    return reader_v2


def _assert_filter_placement(plan) -> None:
    """Both readers push a filter Iceberg can express into the read."""
    assert not _has_operator_type(
        plan, Filter
    ), f"got operators: {_get_operator_types(plan)}"


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


def test_get_catalog():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    catalog = iceberg_ds._get_catalog()
    assert catalog.name == _CATALOG_NAME


def test_plan_files():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    plan_files = iceberg_ds.plan_files
    assert len(plan_files) == 10


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


def test_get_read_tasks():

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    read_tasks = iceberg_ds.get_read_tasks(5)
    assert len(read_tasks) == 5
    assert all(len(rt.metadata.input_files) == 2 for rt in read_tasks)


def test_get_read_task_streams_files_and_preserves_limit(monkeypatch):
    from pyiceberg.io import pyarrow as pyi_pa_io

    batches = {
        "file-1": pa.record_batch({"value": [1, 2, 3, 4]}),
        "file-2": pa.record_batch({"value": [5, 6, 7, 8]}),
    }
    scanned_files = []
    scanner_limits = []

    class FakeArrowScan:
        def __init__(self, **kwargs):
            self._limit = kwargs["limit"]
            scanner_limits.append(self._limit)

        def to_record_batches(self, tasks):
            (task,) = tasks
            scanned_files.append(task)
            batch = batches[task]
            if self._limit is not None:
                batch = batch.slice(0, self._limit)
            yield batch

    monkeypatch.setattr(pyi_pa_io, "ArrowScan", FakeArrowScan)

    schema = pyi_schema.Schema(
        pyi_types.NestedField(
            field_id=1,
            name="value",
            field_type=pyi_types.LongType(),
            required=False,
        )
    )

    tables = iter(
        _get_read_task(
            tasks=("file-1", "file-2"),
            table_io=object(),
            table_metadata=object(),
            row_filter=object(),
            case_sensitive=True,
            limit=5,
            schema=schema,
        )
    )

    assert next(tables).column("value").to_pylist() == [1, 2, 3, 4]
    assert scanned_files == ["file-1"]
    assert scanner_limits == [5]

    assert next(tables).column("value").to_pylist() == [5]
    assert scanned_files == ["file-1", "file-2"]
    assert scanner_limits == [5, 1]
    with pytest.raises(StopIteration):
        next(tables)


def test_get_read_tasks_can_disable_bounded_memory(monkeypatch):
    from pyiceberg.io import pyarrow as pyi_pa_io

    from ray.data.context import DataContext

    scanned_task_counts = []

    class FakeArrowScan:
        def __init__(self, **kwargs):
            pass

        def to_table(self, tasks):
            scanned_task_counts.append(len(tuple(tasks)))
            return pa.table({"value": [1]})

    monkeypatch.setattr(pyi_pa_io, "ArrowScan", FakeArrowScan)

    data_context = DataContext()
    data_context.iceberg_config.read_file_tasks_sequentially = False
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    expected_task_count = len(iceberg_ds.plan_files)

    read_task = iceberg_ds.get_read_tasks(1, data_context=data_context)[0]

    assert [table.to_pydict() for table in read_task()] == [{"value": [1]}]
    assert scanned_task_counts == [expected_task_count]


def test_get_read_task_rejects_pickle_object_columns(monkeypatch, tmp_path):
    """A scan result carrying a pickled-object column must be rejected before
    anything is unpickled."""
    from pyiceberg.io import pyarrow as pyi_pa_io

    marker = tmp_path / "exploit_marker"

    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    poisoned = pa.table(
        {"value": [1, 2], "evil": ArrowPythonObjectArray.from_objects([Exploit()] * 2)}
    )

    class FakeArrowScan:
        def __init__(self, **kwargs):
            pass

        def to_table(self, tasks):
            return poisoned

    monkeypatch.setattr(pyi_pa_io, "ArrowScan", FakeArrowScan)

    schema = pyi_schema.Schema(
        pyi_types.NestedField(
            field_id=1, name="value", field_type=pyi_types.LongType(), required=False
        )
    )

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        list(
            _get_read_task(
                tasks=("file-1",),
                table_io=object(),
                table_metadata=object(),
                row_filter=object(),
                case_sensitive=True,
                limit=None,
                schema=schema,
                read_file_tasks_sequentially=False,
            )
        )

    assert not marker.exists(), "pickle.load executed attacker code"


def test_get_read_task_normalizes_batch_schemas(monkeypatch):
    from pyiceberg.io import pyarrow as pyi_pa_io

    batches = {
        "small-offsets": pa.record_batch(
            {
                "string": pa.array(["a"], type=pa.string()),
                "list": pa.array([["b"]], type=pa.list_(pa.string())),
            }
        ),
        "large-offsets": pa.record_batch(
            {
                "string": pa.array(["c"], type=pa.large_string()),
                "list": pa.array([["d"]], type=pa.large_list(pa.large_string())),
            }
        ),
    }

    class FakeArrowScan:
        def __init__(self, **kwargs):
            pass

        def to_record_batches(self, tasks):
            (task,) = tasks
            yield batches[task]

    monkeypatch.setattr(pyi_pa_io, "ArrowScan", FakeArrowScan)

    schema = pyi_schema.Schema(
        pyi_types.NestedField(
            field_id=1,
            name="string",
            field_type=pyi_types.StringType(),
            required=False,
        ),
        pyi_types.NestedField(
            field_id=2,
            name="list",
            field_type=pyi_types.ListType(
                element_id=3,
                element=pyi_types.StringType(),
                element_required=False,
            ),
            required=False,
        ),
    )
    expected_schema = pyi_pa_io.schema_to_pyarrow(schema, include_field_ids=False)

    tables = list(
        _get_read_task(
            tasks=("small-offsets", "large-offsets"),
            table_io=object(),
            table_metadata=object(),
            row_filter=object(),
            case_sensitive=True,
            limit=None,
            schema=schema,
        )
    )

    assert [table.schema for table in tables] == [expected_schema, expected_schema]
    assert [table.to_pydict() for table in tables] == [
        {"string": ["a"], "list": [["b"]]},
        {"string": ["c"], "list": [["d"]]},
    ]


@pytest.mark.parametrize(
    "file_format,column_sizes,projected_field_ids,expected_size",
    [
        (
            FileFormat.PARQUET,
            {1: 10, 2: 20},
            {1, 2},
            30 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
        ),
        (
            FileFormat.PARQUET,
            {1: 10},
            {1, 2},
            100 * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
        ),
        (FileFormat.PARQUET, {1: 10}, set(), 0),
        (FileFormat.AVRO, {1: 10, 2: 20}, {1, 2}, 100),
    ],
)
def test_estimate_inmemory_file_size(
    file_format, column_sizes, projected_field_ids, expected_size
):
    data_file = SimpleNamespace(
        file_format=file_format,
        file_size_in_bytes=100,
        column_sizes=column_sizes,
    )

    assert _estimate_inmemory_file_size(data_file, projected_field_ids) == expected_size


def test_size_estimate_matches_read_task_metadata():
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        selected_fields=("col_a",),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    read_tasks = iceberg_ds.get_read_tasks(3)
    expected_size = sum(
        task.file.column_sizes[1] * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
        for task in iceberg_ds.plan_files
    )

    assert iceberg_ds.estimate_inmemory_data_size() == expected_size
    assert sum(task.metadata.size_bytes for task in read_tasks) == expected_size


def test_default_size_estimate_uses_full_parquet_file_size():
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    assert iceberg_ds.estimate_inmemory_data_size() == sum(
        task.file.file_size_in_bytes * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
        for task in iceberg_ds.plan_files
    )


def test_projection_pushdown_updates_planner_size_estimate(
    monkeypatch, restore_data_context
):
    """The legacy reader's size estimate follows the projected columns."""
    from ray._private import auto_init_hook

    restore_data_context.use_datasource_v2 = False
    monkeypatch.setattr(auto_init_hook, "enable_auto_connect", False)
    monkeypatch.setattr(ray.util, "get_current_placement_group", lambda: None)
    projected_ds = read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        override_num_blocks=1,
    ).select_columns(["col_a"])

    optimized_plan = LogicalOptimizer().optimize(projected_ds._logical_plan)
    read_op = optimized_plan.dag
    assert isinstance(read_op, Read)

    expected_size = sum(
        task.file.column_sizes[1] * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
        for task in read_op.datasource.plan_files
    )
    assert read_op.infer_metadata().size_bytes == expected_size


def test_empty_projection_has_zero_size_estimate():
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        selected_fields=(),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )

    assert iceberg_ds.estimate_inmemory_data_size() == 0
    assert all(task.metadata.size_bytes == 0 for task in iceberg_ds.get_read_tasks(3))


def test_read_task_remains_compact_with_large_table_metadata():
    shared_metadata = "x" * (1024 * 1024)
    catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    table = catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    with table.transaction() as transaction:
        transaction.set_properties({"ray.test.large_metadata": shared_metadata})

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    read_task_ref = ray.put(iceberg_ds.get_read_tasks(1)[0])

    read_task_size = ray.experimental.get_local_object_locations([read_task_ref])[
        read_task_ref
    ]["object_size"]
    assert read_task_size < len(shared_metadata) // 2

    read_task = ray.get(read_task_ref)
    assert sum(block.num_rows for block in read_task()) == 101


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


# ``count()`` has two strategies -- read the count from plan metadata, or project
# to zero columns and count what comes back. These cases cover both, including
# the ones that defeat the metadata short-circuit.
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


@pytest.mark.parametrize("case", list(_COUNT_CASES), ids=list(_COUNT_CASES))
def test_count_matches_rows_actually_produced(case):
    """``count()`` must agree with the number of rows the query yields.

    The count is a property of the query, not of how much of it was pushed into
    the reader.
    """
    make_ds = _COUNT_CASES[case]
    expected = len(make_ds().take_all())
    assert make_ds().count() == expected


def test_empty_projection_preserves_row_count():
    """Selecting zero columns must yield N rows of no columns, not zero rows.

    ``Dataset.count()`` projects to zero columns to avoid reading column data, so
    an empty projection that drops rows corrupts every count built that way.
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


def test_empty_projection_reads_only_the_stub_column():
    """An empty projection must read no real column, only the stub.

    It projects a field present in no schema version, relying on PyIceberg
    null-filling a missing optional field. Should an upgrade stop doing that,
    the row count collapses to zero silently, so pin the resulting schema: one
    ``null``-typed stub column, matching what every other reader produces.
    """
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        selected_fields=(),
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    blocks = [
        block for read_task in iceberg_ds.get_read_tasks(1) for block in read_task()
    ]
    assert blocks, "fixture table should produce at least one block"
    expected_schema = pa.schema(
        [pa.field(_BATCH_SIZE_PRESERVING_STUB_COL_NAME, pa.null())]
    )
    for block in blocks:
        assert block.schema.equals(expected_schema), block.schema


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

    ``Dataset.count()`` returns this number directly when nothing in the plan can
    change the row count, so an overcount reaches the user as the answer. ``None``
    is always safe but gives up a free count, hence ``count_must_be_exact``
    pinning the cases where the shortcut is sound.
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


def test_reported_num_rows_is_unknown_when_scan_stops_early():
    """A ``limit`` makes the read stop early, so no manifest count describes it.

    The manifests still say how many rows each file holds and the filter may be
    fully resolved, so the exactness test would otherwise pass and report the
    full total for a read that returns ``limit`` rows.
    """
    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        # No row filter, so residuals are all ``AlwaysTrue``: the limit is the
        # only reason the count cannot be trusted.
        scan_kwargs={"limit": 5},
    )
    read_tasks = iceberg_ds.get_read_tasks(1)

    claimed = [read_task.metadata.num_rows for read_task in read_tasks]
    actual = sum(block.num_rows for read_task in read_tasks for block in read_task())

    assert actual == 5, f"limit should cap a single task at 5 rows, got {actual}"
    assert all(
        count is None for count in claimed
    ), f"row counts cannot be exact under a limit, got {claimed}"


def test_empty_projection_survives_schema_evolution_on_pinned_snapshot():
    """An empty projection must not depend on the table's current schema.

    Reading a pinned snapshot after a column was added is where naming a real
    stand-in column breaks: picked from the *current* schema it is absent from
    the snapshot, PyIceberg raises ``ValueError: Could not find column``, and
    ``count()`` fails instead of counting. The fabricated stub belongs to no
    schema version, so no schema change can reach it.
    """
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS.copy())
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    old_snapshot_id = table.current_snapshot().snapshot_id
    expected_rows = 101  # the fixture appends 120 rows, then deletes col_a >= 101

    with table.update_schema() as update:
        update.add_column("col_d", pyi_types.BooleanType())
    table = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}")
    assert "col_d" in table.schema().column_names, "schema evolution should apply"

    iceberg_ds = IcebergDatasource(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
        snapshot_id=old_snapshot_id,
    )
    read_tasks = iceberg_ds.apply_projection({}).get_read_tasks(2)
    actual = sum(block.num_rows for read_task in read_tasks for block in read_task())
    assert actual == expected_rows


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

    ``apply_predicate`` and ``apply_projection`` shallow-copy the datasource, so a
    cache populated beforehand -- by ``estimate_inmemory_data_size``, say, which
    Ray calls to autodetect parallelism -- is shared with the clone. Those tasks
    were planned without the predicate, so every residual is ``AlwaysTrue`` and
    ``get_read_tasks`` would report the unfiltered manifest total.

    Reading the cache twice also has to keep working: ``plan_files`` is annotated
    ``Iterable``, so a future PyIceberg returning a generator would otherwise
    leave the second read empty -- zero read tasks, empty dataset.
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


def test_read_iceberg_does_not_mutate_caller_kwargs():
    """The caller's dicts belong to the caller.

    Both were stored by reference. ``catalog_kwargs`` is then ``pop("name")``-ed,
    so a second read of the same table raises ``NoSuchTableError``.
    ``scan_kwargs`` gets ``snapshot_id`` written into it, which fails silently
    instead: a later read reusing the dict inherits the pin and returns stale rows.
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


@pytest.mark.usefixtures("both_iceberg_readers")
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
        [pa.field("col_a", pa.int32()), pa.field("col_b", pa.large_string())]
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


@pytest.mark.usefixtures("both_iceberg_readers")
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

    _assert_filter_placement(optimized_plan)

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


@pytest.mark.usefixtures("both_iceberg_readers")
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

    _assert_filter_placement(optimized_plan)

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


@pytest.mark.usefixtures("both_iceberg_readers")
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
@pytest.mark.usefixtures("both_iceberg_readers")
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

    if filter_expr is not None:
        _assert_filter_placement(optimized_plan)
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
@pytest.mark.usefixtures("both_iceberg_readers")
def test_rename_select_filter_combinations(
    rename_map,
    select_cols,
    filter_expr,
    pyi_filter,
    expected_cols,
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

    if filter_expr is not None:
        _assert_filter_placement(optimized_plan)

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


@pytest.mark.usefixtures("both_iceberg_readers")
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

    _assert_filter_placement(optimized_plan)
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


def test_predicate_pushdown_with_untranslatable_conjunct(both_iceberg_readers):
    """A predicate Iceberg cannot express must still be applied, above the read.

    Iceberg's expression language has no arithmetic, so ``col_a * 2 <= 100``
    has no equivalent to push. What must not happen is the predicate being
    dropped: whatever the scanner declines to take has to come back as a
    ``Filter``, or the read returns rows the user filtered out.

    Not a parity test: V1 has no residual mechanism at all, so it raises on the
    whole query rather than filtering above the read. Pinned here so that the
    difference is deliberate and so V1's behaviour cannot change unnoticed.
    """
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    filtered_ds = ds.filter(expr=(col("col_c") >= 3) & (col("col_a") * 2 <= 100))

    if not both_iceberg_readers:
        with pytest.raises(ValueError, match="Arithmetic operations"):
            filtered_ds.to_pandas()
        return

    result = filtered_ds.to_pandas()

    # Only the arithmetic part stays above the read; ``col_c >= 3`` is pushed.
    optimized_plan = LogicalOptimizer().optimize(filtered_ds._logical_plan)
    (filter_op,) = [op for op in _walk(optimized_plan.dag) if isinstance(op, Filter)]
    assert filter_op.predicate_expr.structurally_equals(col("col_a") * 2 <= 100)

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    expected = sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}").scan().to_pandas()
    expected = expected[(expected["col_c"] >= 3) & (expected["col_a"] * 2 <= 100)]
    assert len(expected) > 0, "fixture must leave rows on both sides of the filter"
    assert rows_same(result, expected)


@pytest.fixture
def empty_iceberg_table():
    """A table that exists and declares its schema but holds no data files."""
    catalog = SqlCatalog(
        _CATALOG_NAME,
        **{
            "uri": f"sqlite:///{_WAREHOUSE_PATH}/ray_pyiceberg_test_catalog.db",
            "warehouse": f"file://{_WAREHOUSE_PATH}",
        },
    )
    identifier = f"{_DB_NAME}.ray_empty"
    if (_DB_NAME, "ray_empty") in catalog.list_tables(_DB_NAME):
        catalog.drop_table(identifier)
    catalog.create_table(
        identifier,
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
        ),
    )
    yield identifier
    catalog.drop_table(identifier)


def test_read_table_with_no_data_files(both_iceberg_readers, empty_iceberg_table):
    """A declared-but-empty table reads as an empty dataset with the right schema.

    The catalog knows the columns exactly, so there is nothing to sample and
    nothing to fail on -- which is what ``schema_needs_file_sample = False``
    buys.

    Not a parity test: V1 derives its schema from the files it read, of which
    there are none, so it reports no schema at all. Pinned here for the same
    reason as the residual test above.
    """
    ds = ray.data.read_iceberg(
        table_identifier=empty_iceberg_table,
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    if not both_iceberg_readers:
        assert ds.schema() is None
        assert ds.count() == 0
        return

    assert ds.schema().names == ["col_a", "col_b"]
    assert ds.count() == 0
    assert ds.take_all() == []


@pytest.mark.usefixtures("both_iceberg_readers")
def test_read_pins_the_snapshot_across_executions():
    """Two executions of one ``Dataset`` see the same table version.

    V2 lists files inside a task at execution time rather than on the driver at
    plan time. That task must still plan against the snapshot the driver
    resolved, or a write between two executions would change the answer -- and
    so would a retried task.
    """
    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    before = len(ds.take_all())

    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}").append(create_pa_table())

    after = len(ds.take_all())
    assert before == after, (
        "the second execution picked up rows written after the read was "
        f"created ({before} -> {after})"
    )


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


def _count_driver_file_reads(monkeypatch) -> Dict[str, int]:
    """Count data files read by ``ArrowScan`` in this process.

    Ray write tasks run in separate worker processes and never see this patch,
    so anything counted here was read by the driver itself.
    """
    from pyiceberg.io import pyarrow as pyi_pa

    counter = {"files": 0}
    original_to_table = pyi_pa.ArrowScan.to_table

    def _counting_to_table(self, tasks):
        tasks = list(tasks)
        counter["files"] += len(tasks)
        return original_to_table(self, tasks)

    monkeypatch.setattr(pyi_pa.ArrowScan, "to_table", _counting_to_table)
    return counter


def _append_files(table: Table, rows: List[Dict[str, Any]]) -> None:
    """Append rows straight through PyIceberg, one call per data file.

    The test table is partitioned by ``col_c``, so each distinct ``col_c`` in a
    call produces its own data file. Building the files here instead of through
    ``write_iceberg`` keeps the file layout independent of how Ray splits blocks.
    """
    arrow_schema = table.schema().as_arrow()
    table.append(pa.Table.from_pylist([dict(row) for row in rows], schema=arrow_schema))


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

    def test_write_overwrite_with_filter_rewrites_off_the_driver(
        self, clean_table, monkeypatch
    ):
        """A filtered overwrite rewrites candidate files in tasks, not on the driver."""
        from ray.data import SaveMode

        _, table = clean_table
        # Three data files (one per ``col_c`` partition), each holding one row that
        # matches ``col_a == 2`` and one that does not, so every candidate file needs a
        # row-level rewrite rather than a whole-file delete.
        _append_files(
            table,
            [
                {"col_a": 1, "col_b": "keep_5", "col_c": 5},
                {"col_a": 2, "col_b": "drop_5", "col_c": 5},
                {"col_a": 1, "col_b": "keep_6", "col_c": 6},
                {"col_a": 2, "col_b": "drop_6", "col_c": 6},
                {"col_a": 1, "col_b": "keep_7", "col_c": 7},
                {"col_a": 2, "col_b": "drop_7", "col_c": 7},
            ],
        )

        driver_reads = _count_driver_file_reads(monkeypatch)
        new_data = _create_typed_dataframe(
            {"col_a": [30], "col_b": ["new"], "col_c": [5]}
        )
        _write_to_iceberg(
            new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_a") == 2
        )
        files_read_on_driver = driver_reads["files"]

        result = _read_from_iceberg(sort_by=["col_a", "col_c"])
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 1, 1, 30],
                "col_b": ["keep_5", "keep_6", "keep_7", "new"],
                "col_c": [5, 6, 7, 5],
            }
        )
        assert rows_same(result, expected)
        assert files_read_on_driver == 0, (
            f"the driver read {files_read_on_driver} data file(s) while committing the "
            "overwrite; the row-level rewrite belongs in Ray tasks"
        )

    def test_write_overwrite_with_filter_keeps_whole_file_deletes_metadata_only(
        self, clean_table, monkeypatch
    ):
        """A filter that covers whole files is served from metadata, reading nothing."""
        from ray.data import SaveMode

        _, table = clean_table
        # The table is partitioned by ``col_c``, so a filter on ``col_c`` covers whole
        # files and PyIceberg can drop them without opening any of them.
        _append_files(
            table,
            [
                {"col_a": 1, "col_b": "a", "col_c": 1},
                {"col_a": 2, "col_b": "b", "col_c": 2},
                {"col_a": 3, "col_b": "c", "col_c": 3},
            ],
        )

        driver_reads = _count_driver_file_reads(monkeypatch)
        new_data = _create_typed_dataframe(
            {"col_a": [30], "col_b": ["new"], "col_c": [2]}
        )
        _write_to_iceberg(
            new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_c") == 2
        )
        files_read_on_driver = driver_reads["files"]

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 3, 30],
                "col_b": ["a", "c", "new"],
                "col_c": [1, 3, 2],
            }
        )
        assert rows_same(result, expected)
        assert files_read_on_driver == 0, (
            "a filter that covers whole files must stay on the metadata-only path, "
            f"but {files_read_on_driver} data file(s) were read"
        )

    def test_write_overwrite_with_filter_skips_files_already_dropped(
        self, clean_table, monkeypatch
    ):
        """Only the files the filter covers partially are rewritten.

        A filter can cover some files entirely and others only in part. The files it
        covers entirely are dropped from metadata, so they must not be rewritten too.
        """
        from ray.data import SaveMode
        from ray.data._internal.datasource import iceberg_datasink as datasink_module

        _, table = clean_table
        # ``col_c == 2`` covers the whole col_c=2 partition file, while ``col_a == 5``
        # matches only one of the two rows in the col_c=3 partition file.
        _append_files(
            table,
            [
                {"col_a": 1, "col_b": "p2_a", "col_c": 2},
                {"col_a": 2, "col_b": "p2_b", "col_c": 2},
                {"col_a": 5, "col_b": "p3_replaced", "col_c": 3},
                {"col_a": 6, "col_b": "p3_kept", "col_c": 3},
            ],
        )

        real_task = datasink_module._rewrite_iceberg_file_by_filter
        dispatched = {"tasks": 0}

        class _CountingRemoteFunction:
            def options(self, **options):
                dispatched["tasks"] += 1
                return real_task.options(**options)

        monkeypatch.setattr(
            datasink_module,
            "_rewrite_iceberg_file_by_filter",
            _CountingRemoteFunction(),
        )
        driver_reads = _count_driver_file_reads(monkeypatch)

        new_data = _create_typed_dataframe(
            {"col_a": [30], "col_b": ["new"], "col_c": [2]}
        )
        _write_to_iceberg(
            new_data,
            mode=SaveMode.OVERWRITE,
            overwrite_filter=(col("col_c") == 2) | (col("col_a") == 5),
        )
        rewrite_tasks = dispatched["tasks"]
        files_read_on_driver = driver_reads["files"]

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [6, 30], "col_b": ["p3_kept", "new"], "col_c": [3, 2]}
        )
        assert rows_same(result, expected)
        assert rewrite_tasks == 1, (
            f"{rewrite_tasks} file(s) were rewritten; only the partially covered file "
            "needs a rewrite, the fully covered one is dropped from metadata"
        )
        assert files_read_on_driver == 0

    def test_write_overwrite_with_filter_preserves_null_rows(self, clean_table):
        """Rows whose filter column is NULL do not match the filter and must survive."""
        from ray.data import SaveMode

        _, table = clean_table
        # One data file holding a matching row, a non-matching row and two NULLs.
        _append_files(
            table,
            [
                {"col_a": 1, "col_b": "a", "col_c": 5},
                {"col_a": 2, "col_b": "b", "col_c": 5},
                {"col_a": None, "col_b": "c", "col_c": 5},
                {"col_a": None, "col_b": "d", "col_c": 5},
            ],
        )

        new_data = _create_typed_dataframe(
            {"col_a": [30], "col_b": ["new"], "col_c": [5]}
        )
        _write_to_iceberg(
            new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_a") == 2
        )

        # ``col_a == 2`` is unknown for the NULL rows, so only col_a 2 is replaced.
        result = _read_from_iceberg(sort_by="col_b")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, None, None, 30],
                "col_b": ["a", "c", "d", "new"],
                "col_c": [5, 5, 5, 5],
            }
        )
        assert rows_same(result, expected)

    def test_write_overwrite_with_filter_skips_empty_snapshot_on_metrics_miss(
        self, clean_table
    ):
        """A file the metrics flag but no row matches adds no OVERWRITE snapshot.

        The file holds ``col_a`` 1 and 3, so its min/max bounds admit ``col_a == 2``
        and the file is planned for a rewrite, yet no row matches. Nothing is deleted
        or appended, so only the append of the new rows should be committed.
        """
        from ray.data import SaveMode

        catalog, table = clean_table
        _append_files(
            table,
            [
                {"col_a": 1, "col_b": "a", "col_c": 5},
                {"col_a": 3, "col_b": "c", "col_c": 5},
            ],
        )
        table_name = f"{_DB_NAME}.{_TABLE_NAME}"
        snapshots_before = len(catalog.load_table(table_name).snapshots())

        new_data = _create_typed_dataframe(
            {"col_a": [30], "col_b": ["new"], "col_c": [5]}
        )
        _write_to_iceberg(
            new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_a") == 2
        )

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [1, 3, 30], "col_b": ["a", "c", "new"], "col_c": [5, 5, 5]}
        )
        assert rows_same(result, expected)
        new_snapshots = catalog.load_table(table_name).snapshots()[snapshots_before:]
        operations = [snapshot.summary.operation.value for snapshot in new_snapshots]
        assert operations == ["append"], (
            f"the overwrite committed {operations}; with no row to delete, only the "
            "append of the new rows should be committed"
        )

    def test_write_overwrite_with_filter_fails_on_stalled_rewrite(
        self, clean_table, monkeypatch
    ):
        """A rewrite task that never finishes fails the write instead of hanging."""
        import time

        from ray.data import SaveMode
        from ray.data._internal.datasource import iceberg_datasink as datasink_module

        _, table = clean_table
        _append_files(
            table,
            [
                {"col_a": 1, "col_b": "a", "col_c": 5},
                {"col_a": 2, "col_b": "b", "col_c": 5},
            ],
        )

        @ray.remote
        def _stalled_rewrite(*args):
            time.sleep(60)
            return None, []

        monkeypatch.setattr(
            datasink_module, "_rewrite_iceberg_file_by_filter", _stalled_rewrite
        )
        monkeypatch.setattr(datasink_module, "_REWRITE_STALL_TIMEOUT_S", 1)

        new_data = _create_typed_dataframe(
            {"col_a": [30], "col_b": ["new"], "col_c": [5]}
        )
        t0 = time.perf_counter()
        with pytest.raises(Exception, match="No Iceberg overwrite rewrite task"):
            _write_to_iceberg(
                new_data, mode=SaveMode.OVERWRITE, overwrite_filter=col("col_a") == 2
            )
        assert time.perf_counter() - t0 < 30

        # The failed write commits nothing, so the original rows are untouched.
        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {"col_a": [1, 2], "col_b": ["a", "b"], "col_c": [5, 5]}
        )
        assert rows_same(result, expected)


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

    def test_upsert_write_tasks_do_not_ship_keys_to_the_driver(
        self, clean_table, monkeypatch
    ):
        """Write tasks must hand the driver key bounds, not the keys themselves.

        Returning the key columns of every written row makes driver memory grow with
        the size of the upsert; the driver only needs the per-column min/max to build
        the coarse range filter.
        """
        from ray.data import SaveMode
        from ray.data._internal.datasource.iceberg_datasink import IcebergDatasink

        _write_to_iceberg(
            _create_typed_dataframe(
                {"col_a": [1, 2, 3], "col_b": ["s1", "s2", "s3"], "col_c": [1, 1, 1]}
            )
        )

        captured = []
        original_on_write_complete = IcebergDatasink.on_write_complete

        def _capturing_on_write_complete(self, write_result):
            captured.extend(r for r in write_result.write_returns if r)
            return original_on_write_complete(self, write_result)

        monkeypatch.setattr(
            IcebergDatasink, "on_write_complete", _capturing_on_write_complete
        )

        _write_to_iceberg(
            _create_typed_dataframe(
                {"col_a": [2, 4], "col_b": ["u2", "u4"], "col_c": [1, 1]}
            ),
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a"]},
        )

        assert captured, "no write results reached the driver"
        with_keys = [r for r in captured if getattr(r, "upsert_keys", None) is not None]
        assert not with_keys, (
            "write tasks returned upsert key tables to the driver "
            f"({[len(r.upsert_keys) for r in with_keys]} rows); the driver only needs "
            "the per-column bounds"
        )

        # Ray is free to split the write across tasks, so check the merged bounds rather
        # than any single task's: they must cover exactly the keys that were written.
        merged = {}
        for write_return in captured:
            for name, (col_min, col_max) in (
                write_return.upsert_key_bounds or {}
            ).items():
                previous = merged.get(name)
                merged[name] = (
                    (min(previous[0], col_min), max(previous[1], col_max))
                    if previous
                    else (col_min, col_max)
                )
        assert merged == {"col_a": (2, 4)}, merged
        assert sum(r.upsert_key_rows for r in captured) == 2
        assert sum(r.upsert_null_key_rows for r in captured) == 0

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["s1", "u2", "s3", "u4"],
                "col_c": [1, 1, 1, 1],
            }
        )
        assert rows_same(result, expected)

    def test_upsert_key_collection_reserves_memory_for_the_join_columns(
        self, clean_table, monkeypatch
    ):
        """The key task reserves memory sized by the join columns, not whole files."""
        from ray.data import SaveMode
        from ray.data._internal.datasource import iceberg_datasink as datasink_module
        from ray.data._internal.datasource.parquet_datasource import (
            PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT,
        )

        _write_to_iceberg(
            _create_typed_dataframe(
                {"col_a": [1, 2, 3], "col_b": ["s1", "s2", "s3"], "col_c": [1, 1, 1]}
            )
        )

        real_task = datasink_module._collect_upsert_keys
        calls = []

        class _RecordingRemoteFunction:
            def __init__(self, options=None):
                self._options = options or {}

            def options(self, **options):
                return _RecordingRemoteFunction(options)

            def remote(self, *args):
                calls.append((self._options, args))
                return real_task.options(**self._options).remote(*args)

        monkeypatch.setattr(
            datasink_module, "_collect_upsert_keys", _RecordingRemoteFunction()
        )

        _write_to_iceberg(
            _create_typed_dataframe(
                {"col_a": [2, 4], "col_b": ["u2", "u4"], "col_c": [1, 1]}
            ),
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a"]},
        )

        assert len(calls) == 1, f"the key task ran {len(calls)} time(s)"
        options, (data_files, *_) = calls[0]
        assert "memory" in options, "the key task was dispatched without a memory bound"
        field_id = clean_table[1].schema().find_field("col_a").field_id
        key_bytes = sum(data_file.column_sizes[field_id] for data_file in data_files)
        file_bytes = sum(data_file.file_size_in_bytes for data_file in data_files)
        assert options["memory"] == int(
            key_bytes * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT * 2
        )
        assert key_bytes < file_bytes

        result = _read_from_iceberg(sort_by="col_a")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, 3, 4],
                "col_b": ["s1", "u2", "s3", "u4"],
                "col_c": [1, 1, 1, 1],
            }
        )
        assert rows_same(result, expected)

    def test_upsert_row_with_null_join_key_is_inserted(self, clean_table):
        """A NULL join key matches nothing (NULL != NULL), so the row is inserted."""
        from ray.data import SaveMode

        _write_to_iceberg(
            _create_typed_dataframe(
                {"col_a": [1, 2], "col_b": ["s1", "s2"], "col_c": [1, 1]}
            )
        )

        _write_to_iceberg(
            _create_typed_dataframe(
                {"col_a": [2, None], "col_b": ["u2", "u_null"], "col_c": [1, 1]}
            ),
            mode=SaveMode.UPSERT,
            upsert_kwargs={"join_cols": ["col_a"]},
        )

        result = _read_from_iceberg(sort_by="col_b")
        expected = _create_typed_dataframe(
            {
                "col_a": [1, 2, None],
                "col_b": ["s1", "u2", "u_null"],
                "col_c": [1, 1, 1],
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


# Older PyIceberg releases have no way to turn server-side planning on.
_requires_server_side_planning = pytest.mark.skipif(
    not hasattr(RestCatalog, "supports_server_side_planning"),
    reason="This PyIceberg can't plan scans on a REST server.",
)

# Turns server-side planning on: PyIceberg 0.11 reads the first key and 0.12
# the second; each ignores the other.
_SERVER_SIDE_PLANNING_ON = {
    "rest-scan-planning-enabled": "true",
    "scan-planning-mode": "server",
}


@pytest.fixture
def fake_rest_catalog(pyiceberg_table):
    """A minimal Iceberg REST catalog serving the fixture table.

    It offers server-side scan planning and records every request. Its plan is
    the table's first file only, unlike local planning, so the files a scan
    lists show whose plan it used.
    """
    from pyiceberg.catalog.rest import DEFAULT_ENDPOINTS, Capability

    table = pyi_catalog.load_catalog(**_CATALOG_KWARGS).load_table(
        f"{_DB_NAME}.{_TABLE_NAME}"
    )
    with open(table.metadata_location.removeprefix("file://")) as f:
        metadata = json.load(f)
    planned = min(table.scan().plan_files(), key=lambda task: task.file.file_path)
    endpoints = {str(e) for e in DEFAULT_ENDPOINTS}
    endpoints.add(str(Capability.V1_SUBMIT_TABLE_SCAN_PLAN))
    table_path = f"/v1/namespaces/{_DB_NAME}/tables/{_TABLE_NAME}"
    responses = {
        ("GET", "/v1/config"): {
            "defaults": {},
            "overrides": {},
            "endpoints": sorted(endpoints),
        },
        ("GET", table_path): {
            "metadata-location": table.metadata_location,
            "metadata": metadata,
            "config": {},
        },
        ("POST", f"{table_path}/plan"): {
            "status": "completed",
            "plan-id": "plan-1",
            "file-scan-tasks": [
                {
                    "data-file": {
                        "spec-id": planned.file.spec_id,
                        "partition": list(planned.file.partition),
                        "content": "data",
                        "file-path": planned.file.file_path,
                        "file-format": "PARQUET",
                        "file-size-in-bytes": planned.file.file_size_in_bytes,
                        "record-count": planned.file.record_count,
                    }
                }
            ],
            "delete-files": [],
        },
    }
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _respond(self, method):
            path = self.path.split("?")[0]
            length = int(self.headers.get("Content-Length") or 0)
            requests.append((method, path, json.loads(self.rfile.read(length) or "{}")))
            response = responses.get((method, path))
            code = 200 if response is not None else 404
            if response is None:
                response = {
                    "error": {"message": path, "type": "NotFound", "code": code}
                }
            body = json.dumps(response).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self._respond("GET")

        def do_POST(self):
            self._respond("POST")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield SimpleNamespace(
        uri=f"http://127.0.0.1:{server.server_address[1]}",
        planned_path=planned.file.file_path,
        requests=requests,
    )
    server.shutdown()
    server.server_close()


@_requires_server_side_planning
@pytest.mark.usefixtures("both_iceberg_readers")
def test_read_uses_the_rest_catalogs_plan(fake_rest_catalog):
    """With server-side planning on, a read and a count cover the server's plan.

    The server plans one of the table's files, so rows from any other file
    would mean the reader planned the scan itself.
    """
    import pyarrow.parquet as pq

    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs={
            "name": "fake_rest",
            "type": "rest",
            "uri": fake_rest_catalog.uri,
            **_SERVER_SIDE_PLANNING_ON,
        },
    )
    rows = ds.take_all()
    plans = [
        path for _, path, _ in fake_rest_catalog.requests if path.endswith("/plan")
    ]

    planned = pq.ParquetFile(fake_rest_catalog.planned_path.removeprefix("file://"))
    planned_col_a = planned.read(columns=["col_a"])["col_a"].to_pylist()
    assert sorted(row["col_a"] for row in rows) == sorted(planned_col_a)
    assert len(plans) == 1
    assert ds.count() == len(planned_col_a)


@pytest.mark.usefixtures("both_iceberg_readers")
def test_count_and_empty_projection():
    """``count()`` and a zero-column read must still see every row.

    Both go through an empty projection, where a reader can silently return
    zero-row blocks instead of zero-width ones.
    """
    sql_catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    expected = len(
        sql_catalog.load_table(f"{_DB_NAME}.{_TABLE_NAME}").scan().to_arrow()
    )

    ds = ray.data.read_iceberg(
        table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
        catalog_kwargs=_CATALOG_KWARGS.copy(),
    )
    assert ds.count() == expected
    empty_projection = ds.select_columns([])
    assert empty_projection.count() == expected
    assert empty_projection.schema().names == []


@pytest.fixture
def nan_null_iceberg_table() -> Generator[Tuple[str, pa.Table], None, None]:
    """Three files: NaN and null in the first, the rest plain.

    The first file's ``f`` bounds are ``[0.1, 0.1]``, since Iceberg and Parquet
    both leave NaN out of them, so a filter planned on bounds alone skips it.
    """
    catalog = pyi_catalog.load_catalog(**_CATALOG_KWARGS)
    identifier = f"{_DB_NAME}.ray_nan_null"
    if (_DB_NAME, "ray_nan_null") in catalog.list_tables(_DB_NAME):
        catalog.drop_table(identifier)
    table = catalog.create_table(
        identifier,
        schema=pyi_schema.Schema(
            pyi_types.NestedField(
                field_id=1, name="f", field_type=pyi_types.DoubleType(), required=False
            ),
            pyi_types.NestedField(
                field_id=2, name="x", field_type=pyi_types.LongType(), required=False
            ),
        ),
    )
    arrow_schema = table.schema().as_arrow()
    for f, x in [
        ([0.1, float("nan")], [1, None]),
        ([0.2, 0.3], [1, 1]),
        ([2.0, 3.0], [2, 3]),
    ]:
        table.append(pa.table({"f": f, "x": x}, schema=arrow_schema))
    yield identifier, table.scan().to_arrow()
    catalog.drop_table(identifier)


def _optimized_count_plan(ds):
    """The plan ``Dataset.count()`` executes, without executing it.

    ``Dataset.count()`` builds this internally and never exposes it, so tests
    reconstruct it to assert on the optimizer's output.
    """
    from ray.data._internal.logical.interfaces import LogicalPlan
    from ray.data._internal.logical.operators.count_operator import Count

    count_op = Count(
        input_dependencies=[
            Project(exprs=[], input_dependencies=[ds._logical_plan.dag])
        ]
    )
    return LogicalOptimizer().optimize(LogicalPlan(count_op, ds.context))


def _walk(op):
    yield op
    for child in op.input_dependencies:
        yield from _walk(child)


class TestIcebergDatasourceV2:
    """Tests for the V2-only machinery, which has no V1 counterpart.

    The parity tests above cover behavior shared with V1; these cover the three
    pieces V2 adds: the manifest that carries per-file Iceberg state from the
    listing task to the read tasks, the bin packing that decides how many read
    tasks there are, and the ``count()`` that is answered from that manifest
    instead of by reading.
    """

    @staticmethod
    def _load_table() -> Table:
        return pyi_catalog.load_catalog(**_CATALOG_KWARGS).load_table(
            f"{_DB_NAME}.{_TABLE_NAME}"
        )

    @classmethod
    def _indexer(cls, reads_footers=False):
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_indexer import (  # noqa: E501
            IcebergFileIndexer,
        )

        table = cls._load_table()
        return IcebergFileIndexer(
            table_metadata=table.metadata,
            io=table.io,
            scan_kwargs={},
            snapshot_id=None,
            row_filter=pyi_expr.AlwaysTrue(),
            reads_footers=reads_footers,
        )

    @classmethod
    def _listed_chunks(cls, columns, reads_footers=True):
        """Each listed file's ``FileChunk``, by path."""
        from ray.data._internal.datasource_v2.interfaces.file_manifest import (
            FileChunk,
        )

        return {
            path: FileChunk.from_metadata(metadata)
            for manifest in cls._indexer(reads_footers=reads_footers).list_files(
                None, filesystem=None, projected_columns=columns
            )
            for path, metadata in zip(manifest.paths, manifest.file_chunk_metadatas)
        }

    def test_manifest_preserves_every_field_the_reader_uses(self):
        """A manifest round trip must return each scan task field-for-field."""
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
            manifest_from_scan_tasks,
            scan_tasks_from_manifest,
        )

        table = self._load_table()
        tasks = list(table.scan(row_filter="col_a >= 5").plan_files())
        assert tasks, "fixture table should plan at least one file"

        manifest = manifest_from_scan_tasks(tasks)
        assert len(manifest) == len(tasks)

        rebuilt = list(scan_tasks_from_manifest(manifest))
        assert len(rebuilt) == len(tasks)
        for original, copied in zip(tasks, rebuilt):
            assert copied.file.file_path == original.file.file_path
            assert copied.file.file_format == original.file.file_format
            assert copied.file.spec_id == original.file.spec_id
            assert copied.file.record_count == original.file.record_count
            assert copied.file.file_size_in_bytes == original.file.file_size_in_bytes
            assert copied.file.partition == original.file.partition
            assert copied.residual == original.residual
            assert copied.delete_files == original.delete_files

    def test_manifest_carries_delete_files(self):
        """Delete files attached to a task must survive the round trip.

        Built by hand rather than from the fixture: PyIceberg 0.11.1 deletes
        copy-on-write (it rewrites the data files), so no scan task it plans
        ever carries one -- yet a table written by an engine that deletes
        merge-on-read will, and dropping them would silently resurrect deleted
        rows.
        """
        from pyiceberg.manifest import DataFileContent, FileFormat

        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
            manifest_from_scan_tasks,
            scan_tasks_from_manifest,
        )

        table = self._load_table()
        task = list(table.scan().plan_files())[0]
        delete_paths = [
            "s3://bucket/deletes/b.parquet",
            "s3://bucket/deletes/a.parquet",
        ]
        deletes = {
            DataFile.from_args(
                content=DataFileContent.POSITION_DELETES,
                file_path=path,
                file_format=FileFormat.PARQUET,
                partition=task.file.partition,
                record_count=1,
                file_size_in_bytes=1,
            )
            for path in delete_paths
        }
        with_deletes = FileScanTask(task.file, delete_files=deletes)

        (rebuilt,) = scan_tasks_from_manifest(manifest_from_scan_tasks([with_deletes]))
        assert {f.file_path for f in rebuilt.delete_files} == set(delete_paths)
        assert all(
            f.content == DataFileContent.POSITION_DELETES for f in rebuilt.delete_files
        )

    def test_rebuilt_scan_tasks_decode_identically(self):
        """The rebuilt tasks must read byte-for-byte what the originals do."""
        from pyiceberg.io.pyarrow import ArrowScan

        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
            manifest_from_scan_tasks,
            scan_tasks_from_manifest,
        )

        table = self._load_table()
        scan = table.scan()
        tasks = list(scan.plan_files())
        rebuilt = list(scan_tasks_from_manifest(manifest_from_scan_tasks(tasks)))

        def read(scan_tasks):
            return ArrowScan(
                table_metadata=table.metadata,
                io=table.io,
                projected_schema=scan.projection(),
                row_filter=pyi_expr.AlwaysTrue(),
                case_sensitive=True,
            ).to_table(scan_tasks)

        assert read(rebuilt).equals(read(tasks))

    def test_reader_rejects_pickle_object_columns(self, monkeypatch, tmp_path):
        """A decoded batch with a pickled-object column is rejected unread.

        The V2 counterpart of ``test_get_read_task_rejects_pickle_object_columns``.
        The marker file shows nothing was unpickled.
        """
        from pyiceberg.io import pyarrow as pyi_pa_io

        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_reader import (  # noqa: E501
            IcebergFileReader,
        )
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
            manifest_from_scan_tasks,
        )

        marker = tmp_path / "exploit_marker"

        class Exploit:
            def __reduce__(self):
                return (os.system, (f"touch {marker}",))

        poisoned = pa.table(
            {
                "value": [1, 2],
                "evil": ArrowPythonObjectArray.from_objects([Exploit()] * 2),
            }
        )

        class FakeArrowScan:
            def __init__(self, **kwargs):
                pass

            def to_record_batches(self, tasks):
                yield from poisoned.to_batches()

        monkeypatch.setattr(pyi_pa_io, "ArrowScan", FakeArrowScan)

        table = self._load_table()
        manifest = manifest_from_scan_tasks(list(table.scan().plan_files())[:1])
        reader = IcebergFileReader(
            table_metadata=table.metadata,
            io=table.io,
            projected_schema=table.scan().projection(),
            row_filter=pyi_expr.AlwaysTrue(),
        )
        with pytest.raises(ValueError, match="arrow_pickled_object"):
            list(reader.read(manifest))
        assert not marker.exists()

    def test_listing_skips_excluded_read_units(self):
        """A file whose path is an excluded read unit id is not listed again."""
        indexer = self._indexer()
        listed = [
            path
            for manifest in indexer.list_files(None, filesystem=None)
            for path in manifest.paths.tolist()
        ]
        assert len(listed) > 1, "need more than one file to exclude one of them"

        remaining = [
            path
            for manifest in indexer.list_files(
                None, filesystem=None, excluded_read_unit_ids={listed[0]}
            )
            for path in manifest.paths.tolist()
        ]
        assert sorted(remaining) == sorted(listed[1:])

    @staticmethod
    def _rest_table(uri: str, **properties: str) -> Table:
        return pyi_catalog.load_catalog(
            "fake_rest", type="rest", uri=uri, **properties
        ).load_table(f"{_DB_NAME}.{_TABLE_NAME}")

    @_requires_server_side_planning
    def test_listing_lists_the_rest_catalogs_plan(self, fake_rest_catalog):
        """With server-side planning on, the listing task asks the server.

        The indexer goes through a pickle round trip, as on its way to the
        listing task, and the server gets the driver's snapshot and filter.
        """
        import cloudpickle
        from pyiceberg.catalog.rest.scan_planning import PlanTableScanRequest
        from pyiceberg.expressions.parser import parse

        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_indexer import (  # noqa: E501
            IcebergFileIndexer,
            ServerSidePlanning,
        )

        table = self._rest_table(
            fake_rest_catalog.uri, token="secret-token", **_SERVER_SIDE_PLANNING_ON
        )
        server_side_planning = ServerSidePlanning.for_table(table)
        assert server_side_planning is not None
        assert "secret-token" not in repr(server_side_planning)
        snapshot_id = table.current_snapshot().snapshot_id
        row_filter = parse("col_a >= 0")
        indexer = cloudpickle.loads(
            cloudpickle.dumps(
                IcebergFileIndexer(
                    table_metadata=table.metadata,
                    io=table.io,
                    scan_kwargs={},
                    snapshot_id=snapshot_id,
                    row_filter=row_filter,
                    server_side_planning=server_side_planning,
                )
            )
        )
        fake_rest_catalog.requests.clear()

        listed = [
            path
            for manifest in indexer.list_files(None, filesystem=None)
            for path in manifest.paths.tolist()
        ]

        assert listed == [fake_rest_catalog.planned_path]
        plans = [
            PlanTableScanRequest.model_validate(body)
            for method, path, body in fake_rest_catalog.requests
            if method == "POST" and path.endswith("/plan")
        ]
        assert len(plans) == 1
        assert plans[0].snapshot_id == snapshot_id
        assert plans[0].filter == row_filter

    @_requires_server_side_planning
    def test_no_server_side_planning_unless_the_catalog_plans(self, fake_rest_catalog):
        """Listing plans locally for a REST catalog with planning off, or a SQL one."""
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_indexer import (  # noqa: E501
            ServerSidePlanning,
        )

        rest_table = self._rest_table(fake_rest_catalog.uri)
        assert ServerSidePlanning.for_table(rest_table) is None
        assert ServerSidePlanning.for_table(self._load_table()) is None

    def test_listing_sizes_files_by_the_columns_read(self):
        """Without footer reads, a file is its read columns' bytes times 5, as in V1."""

        def sizes(columns):
            return {
                path: chunk.size_bytes
                for path, chunk in self._listed_chunks(
                    columns, reads_footers=False
                ).items()
            }

        files = [task.file for task in self._load_table().scan().plan_files()]
        assert sizes(["col_a"]) == {
            file.file_path: file.column_sizes[1]
            * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
            for file in files
        }
        assert sizes(None) == {
            file.file_path: sum(file.column_sizes.values())
            * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
            for file in files
        }

    def test_listing_sizes_parquet_files_from_their_footers(self):
        """A Parquet file is sized at its decoded bytes, with the memory decoding takes."""
        chunks = self._listed_chunks(["col_a"])
        assert chunks, "fixture table should plan at least one file"
        for path, chunk in chunks.items():
            parquet = pq.ParquetFile(path.removeprefix("file://"))
            # A fixed-width column, so the estimate is exact.
            assert chunk.size_bytes == parquet.read(columns=["col_a"]).nbytes
            assert chunk.memory_bytes > chunk.size_bytes

    def test_listing_falls_back_for_a_renamed_column(self):
        """A column the files store under an old name is sized without its footer."""
        table = self._load_table()
        table.update_schema().rename_column("col_b", "col_b_renamed").commit()

        files = {task.file.file_path: task.file for task in table.scan().plan_files()}
        renamed = self._listed_chunks(["col_b_renamed"])
        assert {path: chunk.size_bytes for path, chunk in renamed.items()} == {
            path: file.column_sizes[2] * PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT
            for path, file in files.items()
        }
        assert all(chunk.memory_bytes is None for chunk in renamed.values())
        assert all(
            chunk.memory_bytes is not None
            for chunk in self._listed_chunks(["col_a"]).values()
        )

    def test_read_unit_requests_the_memory_its_files_take(self):
        """A packed read unit carries its files' summed decode memory to its task."""
        from ray.data._internal.datasource_v2.common.online_bin_packer import (
            OnlineBinPacker,
        )
        from ray.data.block import CONSUMER_MEMORY_BYTES_METADATA_KEY

        packer = OnlineBinPacker(max_bin_bytes=1 << 30)
        for manifest in self._indexer(reads_footers=True).list_files(
            None, filesystem=None, projected_columns=["col_a"]
        ):
            packer.add_input(manifest)
        packer.finalize()
        (read_unit,) = [
            packer.next_partition() for _ in iter(packer.has_partition, False)
        ]

        metadata = read_unit.as_block().schema.metadata
        memory = sum(
            chunk.memory_bytes for chunk in self._listed_chunks(["col_a"]).values()
        )
        assert int(metadata[CONSUMER_MEMORY_BYTES_METADATA_KEY]) == memory

    def test_select_columns_keeps_the_requested_order(self, restore_data_context):
        """Rows come back in the order ``select_columns`` asked for.

        V1 reports this order in the schema but returns rows in table order.
        """
        restore_data_context.use_datasource_v2 = True

        ds = read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        ).select_columns(["col_c", "col_a"])

        assert ds.schema().names == ["col_c", "col_a"]
        assert list(ds.take(1)[0]) == ["col_c", "col_a"]

    def test_planning_samples_no_file(self, monkeypatch, restore_data_context):
        """Plan time must not open -- or even list -- a single data file.

        ``schema_needs_file_sample = False`` is what buys this: the catalog
        already names every column, so ``_read_datasource_v2`` skips
        ``sample_files`` outright. ``test_read_table_with_no_data_files``
        covers the *consequence* (a table with no files still plans); this
        covers the *mechanism*, by making any sampling attempt fail loudly
        instead of merely being unnecessary.

        ``sample_files`` is imported inside ``_read_datasource_v2``, so the
        patch has to land on the module that defines it.
        """
        from ray.data._internal.datasource_v2.common import listing_utils

        restore_data_context.use_datasource_v2 = True

        def explode(*args, **kwargs):
            raise AssertionError("plan time sampled a file")

        monkeypatch.setattr(listing_utils, "sample_files", explode)

        ds = read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        # ``schema()`` is what forces plan-time inference, so if anything were
        # going to sample, it would happen here.
        assert ds.schema().names == ["col_a", "col_b", "col_c"]

    def test_schema_hooks_accept_a_missing_sample(self):
        """The two ``Optional[FileManifest]`` hooks, called the way the planner
        calls them for this source.

        Passing ``None`` explicitly pins both halves of the no-sample contract:
        the schema really does come from the catalog, and Iceberg claims no
        path-derived partitioning. The second matters because Iceberg
        partitioning is hidden behind transforms -- ``col_c`` is an identity
        partition here, but a ``day(ts)`` column would leave a path component
        that is not a column value at all, so there is nothing a path parser
        should be handed.
        """
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_datasource_v2 import (  # noqa: E501
            IcebergDatasourceV2,
        )

        datasource = IcebergDatasourceV2(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )

        assert datasource.schema_needs_file_sample is False
        assert datasource.infer_schema(None).names == ["col_a", "col_b", "col_c"]
        assert datasource.resolve_partitioning(None) is None

    def test_count_is_answered_from_iceberg_metadata(self, restore_data_context):
        """``count()`` must read no data when nothing can reduce the row count.

        Iceberg writes each data file's ``record_count`` into its own
        manifests, so the whole answer is already in the listing task's output
        -- unlike the Parquet path, which still has to fetch a footer per file.
        """
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_indexer import (  # noqa: E501
            IcebergFileIndexer,
        )
        from ray.data._internal.logical.operators.map_operator import MapBatches
        from ray.data._internal.logical.operators.read_operator import (
            ListFiles,
            ReadFiles,
        )

        restore_data_context.use_datasource_v2 = True
        expected = len(self._load_table().scan().to_arrow())

        ds = read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        dag = _optimized_count_plan(ds).dag

        assert isinstance(dag, MapBatches)
        assert not any(isinstance(op, ReadFiles) for op in _walk(dag))

        # The rule rebuilds ``ListFiles`` with whatever ``as_whole_file_indexer``
        # returns. For Parquet that is a *different*, plainer indexer, because
        # the footer one bin-packs row groups and can emit a file once per bin.
        # Ours must stay an Iceberg indexer, since it plans the table, but one
        # that reads no Parquet footers.
        (list_files,) = [op for op in _walk(dag) if isinstance(op, ListFiles)]
        assert isinstance(list_files.file_indexer, IcebergFileIndexer)
        assert not list_files.file_indexer._reads_footers
        assert list_files.file_partitioner is None

        # The fixture's delete rewrote its files, so their ``record_count``
        # already excludes the deleted rows.
        assert expected < len(create_pa_table())
        assert ds.count() == expected

    @pytest.mark.parametrize(
        "predicate,free",
        [
            (col("col_c") == 3, True),
            (col("col_a") < 50, False),
            (col("col_b") == "a", False),
        ],
        ids=["partition_column", "data_column", "unprojected_data_column"],
    )
    def test_count_pushdown_survives_a_filter(
        self, restore_data_context, predicate, free
    ):
        """A filtered ``count()`` is still answered without a read.

        Iceberg plans a residual per file, the part of the filter the file's
        partition values don't already satisfy, so a filter on the partition
        column leaves every listed file countable from its ``record_count``.
        Files with a data-column residual are decoded instead.
        """
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
            scan_tasks_from_manifest,
        )
        from ray.data._internal.logical.operators.read_operator import (
            ListFiles,
            ReadFiles,
        )

        restore_data_context.use_datasource_v2 = True
        ds = read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        ).filter(expr=predicate)

        count_plan = _optimized_count_plan(ds)
        assert not any(isinstance(op, ReadFiles) for op in _walk(count_plan.dag))
        # Listing must prune by the same predicate the count relies on.
        (list_files,) = [
            op for op in _walk(count_plan.dag) if isinstance(op, ListFiles)
        ]
        assert list_files.predicate.structurally_equals(predicate)
        assert list_files.pushdown_is_final

        # ``count()`` first, on a fresh ``Dataset``: a materialized one answers
        # from the blocks it already holds.
        counted = ds.count()
        expected = len(ds.take_all())
        assert expected > 0, "predicate must match something to be a real test"
        assert counted == expected

        # The count was free exactly when every file's residual is ``AlwaysTrue``.
        residuals = [
            isinstance(task.residual, pyi_expr.AlwaysTrue)
            for manifest in self._indexer().list_files(
                None, filesystem=None, predicate=predicate
            )
            for task in scan_tasks_from_manifest(manifest)
        ]
        assert residuals and all(residuals) == free

    def test_count_pushdown_declines_a_limit(self, restore_data_context):
        """A limit is applied while reading; no metadata says where it stops."""
        from ray.data._internal.logical.operators.read_operator import ReadFiles

        restore_data_context.use_datasource_v2 = True
        ds = read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        ).limit(7)

        assert any(
            isinstance(op, ReadFiles) for op in _walk(_optimized_count_plan(ds).dag)
        )
        assert ds.count() == 7

    @pytest.mark.parametrize(
        "row_filter",
        [
            pyi_expr.NotEqualTo("col_c", 3),
            pyi_expr.Not(pyi_expr.EqualTo("col_c", 3)),
        ],
        ids=["not_equal", "not_of_equal"],
    )
    def test_count_agrees_with_the_read_on_a_null_partition(
        self, restore_data_context, clean_table, row_filter
    ):
        """``count()`` doesn't take ``record_count`` for a null partition under ``!=``.

        Iceberg gives that file an ``AlwaysTrue`` residual, but ``ArrowScan``
        drops its row, since ``null != 3`` is null in PyArrow.
        """
        _, table = clean_table
        _append_files(
            table,
            [
                {"col_a": 1, "col_b": "a", "col_c": 3},
                {"col_a": 2, "col_b": "b", "col_c": None},
                {"col_a": 3, "col_b": "c", "col_c": 4},
            ],
        )
        restore_data_context.use_datasource_v2 = True

        def read():
            return read_iceberg(
                table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
                catalog_kwargs=_CATALOG_KWARGS.copy(),
                row_filter=row_filter,
            )

        assert len(read().take_all()) == 1
        assert read().count() == 1

    def test_count_takes_record_count_when_bounds_prove_the_filter(
        self, restore_data_context, clean_table
    ):
        """A data-column filter that a file's bounds prove marks it ``fully_matched``.

        Only the file whose every ``col_a`` passes is counted from metadata;
        one that straddles the filter, or holds a null, is decoded.
        """
        from pyiceberg.conversions import from_bytes

        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
            scan_tasks_from_manifest,
        )
        from ray.data._internal.datasource_v2.interfaces.file_manifest import (
            FileChunk,
        )

        _, table = clean_table
        _append_files(table, [{"col_a": 1, "col_c": 1}, {"col_a": 2, "col_c": 1}])
        _append_files(table, [{"col_a": 5, "col_c": 2}, {"col_a": 6, "col_c": 2}])
        _append_files(table, [{"col_a": None, "col_c": 3}, {"col_a": 7, "col_c": 3}])
        restore_data_context.use_datasource_v2 = True
        predicate = col("col_a") >= 2

        matched_lower_bounds = []
        field = self._load_table().schema().find_field("col_a")
        for manifest in self._indexer().list_files(
            None, filesystem=None, predicate=predicate
        ):
            tasks = scan_tasks_from_manifest(manifest)
            for task, chunk in zip(tasks, manifest.file_chunk_metadatas):
                if FileChunk.from_metadata(chunk).fully_matched:
                    lower = task.file.lower_bounds[field.field_id]
                    matched_lower_bounds.append(from_bytes(field.field_type, lower))
        assert matched_lower_bounds == [5]

        def read():
            return read_iceberg(
                table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
                catalog_kwargs=_CATALOG_KWARGS.copy(),
            ).filter(expr=predicate)

        assert read().count() == len(read().take_all()) == 4

    @pytest.mark.parametrize(
        "row_filter,overrides,expected",
        [
            (pyi_expr.GreaterThanOrEqual("x", 2), {}, True),
            (pyi_expr.GreaterThanOrEqual("x", 3), {}, False),
            (pyi_expr.GreaterThanOrEqual("x", 2), {"null_value_counts": {1: 1}}, False),
            (pyi_expr.GreaterThanOrEqual("x", 2), {"null_value_counts": {}}, False),
            (pyi_expr.GreaterThanOrEqual("f", 1.0), {}, True),
            (pyi_expr.GreaterThanOrEqual("f", 1.0), {"nan_value_counts": {}}, False),
            (pyi_expr.NotEqualTo("x", 1), {}, False),
            (pyi_expr.NotEqualTo("x", 1), {"residual": pyi_expr.AlwaysTrue()}, False),
            (pyi_expr.EqualTo("x", 9), {"residual": pyi_expr.AlwaysTrue()}, True),
            (pyi_expr.GreaterThanOrEqual("x", 2), {"delete_files": {"d"}}, False),
            (pyi_expr.GreaterThanOrEqual("x", 2), {"filters_after_read": True}, False),
        ],
        ids=[
            "bounds_prove",
            "bounds_straddle",
            "null_in_column",
            "no_null_count",
            "float_with_nan_count",
            "float_without_nan_count",
            "not_equal",
            "not_equal_with_true_residual",
            "true_residual",
            "delete_files",
            "after_read_filter",
        ],
    )
    def test_every_row_matches(self, row_filter, overrides, expected):
        """``RowMatchCheck`` proves a file only when nothing could drop a row.

        ``!=`` fails closed even when PyIceberg's strict evaluator, or an
        ``AlwaysTrue`` residual, says every row matches: Iceberg treats
        ``null != 1`` as true and PyArrow drops the row.
        """
        from pyiceberg.conversions import to_bytes

        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_row_match import (  # noqa: E501
            RowMatchCheck,
        )

        long_type, double_type = pyi_types.LongType(), pyi_types.DoubleType()
        schema = pyi_schema.Schema(
            pyi_types.NestedField(1, "x", long_type, required=False),
            pyi_types.NestedField(2, "f", double_type, required=False),
        )
        data_file = SimpleNamespace(
            file_path="data.parquet",
            record_count=4,
            value_counts={1: 4, 2: 4},
            null_value_counts=overrides.get("null_value_counts", {1: 0, 2: 0}),
            nan_value_counts=overrides.get("nan_value_counts", {2: 0}),
            lower_bounds={1: to_bytes(long_type, 2), 2: to_bytes(double_type, 1.0)},
            upper_bounds={1: to_bytes(long_type, 5), 2: to_bytes(double_type, 3.0)},
        )
        task = SimpleNamespace(
            file=data_file,
            residual=overrides.get("residual", row_filter),
            delete_files=overrides.get("delete_files", set()),
        )
        check = RowMatchCheck(
            schema=schema,
            row_filter=row_filter,
            case_sensitive=True,
            filters_after_read=overrides.get("filters_after_read", False),
        )

        assert check.every_row_matches(task) == expected

    def test_read_metadata_subtracts_deleted_rows(self, tmp_path):
        """A file with delete files is counted by reading it, not by metadata.

        ``record_count`` counts rows the file holds, including ones a delete
        file removes, so trusting it on a merge-on-read table would over-report.
        This is the one branch of ``read_metadata`` that touches data, and it is
        the branch PyIceberg's own ``count()`` takes for the same reason.
        """
        from pyiceberg.manifest import DataFileContent

        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_reader import (  # noqa: E501
            IcebergFileReader,
        )
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_manifest import (
            manifest_from_scan_tasks,
            scan_tasks_from_manifest,
        )

        table = self._load_table()
        task = list(table.scan().plan_files())[0]
        reader = IcebergFileReader(
            table_metadata=table.metadata,
            io=table.io,
            projected_schema=table.scan().projection(),
            row_filter=pyi_expr.AlwaysTrue(),
        )

        def count(scan_task) -> int:
            manifest = manifest_from_scan_tasks([scan_task])
            (metadata,) = reader.read_metadata(manifest)
            return metadata.num_rows

        clean_count = count(task)
        assert clean_count == task.file.record_count

        deleted_positions = [0, 2]
        delete_path = tmp_path / "count-pos-deletes.parquet"
        pq.write_table(
            pa.table(
                {
                    "file_path": pa.array(
                        [task.file.file_path] * len(deleted_positions), pa.string()
                    ),
                    "pos": pa.array(deleted_positions, pa.int64()),
                }
            ),
            delete_path,
        )
        delete_file = DataFile.from_args(
            content=DataFileContent.POSITION_DELETES,
            file_path=f"file://{delete_path}",
            file_format=FileFormat.PARQUET,
            partition=task.file.partition,
            record_count=len(deleted_positions),
            file_size_in_bytes=delete_path.stat().st_size,
        )
        dirty = FileScanTask(task.file, delete_files={delete_file})

        assert count(dirty) == clean_count - len(deleted_positions)
        # The carried task still has the pre-delete number, so the subtraction
        # is the reader's doing, not the encoder's.
        (carried,) = scan_tasks_from_manifest(manifest_from_scan_tasks([dirty]))
        assert carried.file.record_count == clean_count

    def test_reader_declines_metadata_under_a_limit(self):
        """The reader's own guard, independent of the scanner's.

        ``metadata_row_count_is_exact`` already stops the rule from asking, so
        this is the second of two fail-closed layers: a caller that reaches
        ``read_metadata`` another way still cannot be handed a partial count.
        A row filter is deliberately *not* refused here -- ``read_metadata``
        accounts for one per file.
        """
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_file_reader import (  # noqa: E501
            IcebergFileReader,
        )
        from ray.data._internal.datasource_v2.interfaces.supports_metadata import (
            MetadataType,
        )

        table = self._load_table()
        kwargs = {
            "table_metadata": table.metadata,
            "io": table.io,
            "projected_schema": table.scan().projection(),
        }
        limited = IcebergFileReader(**kwargs, row_filter=pyi_expr.AlwaysTrue(), limit=5)
        assert limited.available_metadata() == set()

        filtered = IcebergFileReader(
            **kwargs, row_filter=pyi_expr.LessThan("col_a", 50)
        )
        assert filtered.available_metadata() == {MetadataType.NUM_ROWS}

    @pytest.mark.parametrize("one_file_per_call", [True, False])
    @pytest.mark.parametrize(
        "predicate",
        [
            ~(col("f") < 1.0),
            col("f") != 0.1,
            (col("f") > 0.15) & ~(col("f") < 1.0),
            ~col("x").is_in([1]),
            col("x").not_in([1]),
        ],
        ids=["not_less_than", "not_equal", "mixed", "not_is_in", "not_in"],
    )
    def test_pushed_filter_keeps_nan_and_null_rows(
        self, restore_data_context, nan_null_iceberg_table, predicate, one_file_per_call
    ):
        """A pushed filter returns exactly the rows Ray's own filter keeps.

        Ray keeps the NaN row for ``~(f < 1.0)`` and ``f != 0.1``, and the null
        row for ``~x.is_in([1])``; file bounds, row-group statistics and
        PyIceberg's one-value ``NotIn`` would each drop it.
        """
        identifier, full_table = nan_null_iceberg_table
        restore_data_context.use_datasource_v2 = True
        restore_data_context.iceberg_config.read_file_tasks_sequentially = (
            one_file_per_call
        )
        ds = read_iceberg(
            table_identifier=identifier, catalog_kwargs=_CATALOG_KWARGS.copy()
        ).filter(expr=predicate)

        _assert_filter_placement(LogicalOptimizer().optimize(ds._logical_plan))
        expected = BlockAccessor.for_block(full_table).filter(predicate).to_pandas()
        assert len(expected) > 0
        assert rows_same(ds.to_pandas(), expected)
        assert ds.count() == len(expected)

    def test_after_read_filter_columns_are_dropped(
        self, restore_data_context, nan_null_iceberg_table
    ):
        """A column read only for the after-read filter isn't returned."""
        identifier, full_table = nan_null_iceberg_table
        restore_data_context.use_datasource_v2 = True
        predicate = ~(col("f") < 1.0)
        ds = (
            read_iceberg(
                table_identifier=identifier, catalog_kwargs=_CATALOG_KWARGS.copy()
            )
            .filter(expr=predicate)
            .select_columns(["x"])
        )

        expected = BlockAccessor.for_block(full_table).filter(predicate).to_pandas()
        assert rows_same(ds.to_pandas(), expected[["x"]])
        assert ds.schema().names == ["x"]

    def test_after_read_filter_stops_at_the_limit(
        self, restore_data_context, nan_null_iceberg_table
    ):
        """The limit counts rows after the after-read filter, not before."""
        identifier, _ = nan_null_iceberg_table
        restore_data_context.use_datasource_v2 = True
        ds = (
            read_iceberg(
                table_identifier=identifier, catalog_kwargs=_CATALOG_KWARGS.copy()
            )
            .filter(expr=col("f") != 0.1)
            .limit(2)
        )

        rows = ds.take_all()
        assert len(rows) == 2
        assert all(row["f"] != 0.1 for row in rows)

    def test_filter_that_does_not_bind_stays_above_the_read(self, restore_data_context):
        """``col_a >= 1.5`` on an ``int`` column fails Iceberg's bind, so Ray keeps it."""
        restore_data_context.use_datasource_v2 = True
        predicate = (col("col_c") >= 3) & (col("col_a") >= 1.5)
        ds = read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        ).filter(expr=predicate)

        optimized_plan = LogicalOptimizer().optimize(ds._logical_plan)
        (filter_op,) = [
            op for op in _walk(optimized_plan.dag) if isinstance(op, Filter)
        ]
        assert filter_op.predicate_expr.structurally_equals(col("col_a") >= 1.5)
        expected = self._load_table().scan().to_pandas()
        expected = expected[(expected["col_c"] >= 3) & (expected["col_a"] >= 1.5)]
        assert len(expected) > 0
        assert rows_same(ds.to_pandas(), expected)

    def test_count_is_answered_from_the_snapshot_summary(self, restore_data_context):
        """``count()`` reads the snapshot's ``total-records`` and runs no task."""
        restore_data_context.use_datasource_v2 = True
        table = self._load_table()
        expected = len(table.scan().to_arrow())
        assert table.current_snapshot().summary[TOTAL_RECORDS] == str(expected)
        ds = read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )

        assert ds._meta_count() == expected
        assert ds.count() == expected
        assert ds.select_columns(["col_a"])._meta_count() == expected

    @pytest.mark.parametrize(
        "make_dataset",
        [
            lambda read: read().filter(expr=col("col_c") >= 3),
            lambda read: read(row_filter="col_c >= 3"),
        ],
        ids=["filter", "row_filter"],
    )
    def test_snapshot_count_declines_under_a_filter(
        self, restore_data_context, make_dataset
    ):
        """The summary counts the whole snapshot, so a filter declines it."""
        restore_data_context.use_datasource_v2 = True

        def read(**kwargs):
            return read_iceberg(
                table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
                catalog_kwargs=_CATALOG_KWARGS.copy(),
                **kwargs,
            )

        ds = make_dataset(read)
        assert ds._meta_count() is None
        expected = len(
            self._load_table()
            .scan(row_filter=pyi_expr.GreaterThanOrEqual("col_c", 3))
            .to_arrow()
        )
        assert expected > 0
        assert ds.count() == expected

    def test_snapshot_count_declines_under_a_limit(self, restore_data_context):
        """A scanner holding a limit doesn't report the snapshot's count."""
        from ray.data._internal.logical.operators.read_operator import ReadFiles

        restore_data_context.use_datasource_v2 = True
        ds = read_iceberg(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        (read_files,) = [
            op for op in _walk(ds._logical_plan.dag) if isinstance(op, ReadFiles)
        ]
        assert read_files.scanner.exact_row_count() is not None
        assert read_files.scanner.push_limit(7).exact_row_count() is None
        assert ds.limit(7).count() == 7

    @pytest.mark.parametrize(
        "overrides",
        [
            {TOTAL_RECORDS: None},
            {TOTAL_POSITION_DELETES: None},
            {TOTAL_EQUALITY_DELETES: None},
            {TOTAL_POSITION_DELETES: "3"},
            {TOTAL_EQUALITY_DELETES: "3"},
            {TOTAL_RECORDS: "not a number"},
        ],
        ids=[
            "no_records",
            "no_position_deletes",
            "no_equality_deletes",
            "position_deletes",
            "equality_deletes",
            "not_a_number",
        ],
    )
    def test_snapshot_count_declines_an_incomplete_summary(self, overrides):
        """A missing key, a delete, or a non-number in the summary declines."""
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_datasource_v2 import (  # noqa: E501
            IcebergDatasourceV2,
        )

        datasource = IcebergDatasourceV2(
            table_identifier=f"{_DB_NAME}.{_TABLE_NAME}",
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        assert datasource._snapshot_row_count() is not None
        snapshot_id = datasource.snapshot_id
        assert snapshot_id is not None
        snapshot = datasource.table.snapshot_by_id(snapshot_id)
        assert snapshot is not None and snapshot.summary is not None
        # The datasource's own loaded copy of the metadata; the catalog's is untouched.
        for key, value in overrides.items():
            if value is None:
                snapshot.summary._additional_properties.pop(key)
            else:
                snapshot.summary[key] = value
        assert datasource._snapshot_row_count() is None

    def test_snapshot_count_of_a_table_with_no_snapshot(self, empty_iceberg_table):
        """A table nothing was written to counts zero."""
        from ray.data._internal.datasource_v2.formats.iceberg.iceberg_datasource_v2 import (  # noqa: E501
            IcebergDatasourceV2,
        )

        datasource = IcebergDatasourceV2(
            table_identifier=empty_iceberg_table,
            catalog_kwargs=_CATALOG_KWARGS.copy(),
        )
        assert datasource.snapshot_id is None
        assert datasource._snapshot_row_count() == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
