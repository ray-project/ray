import json
import os
import time
import uuid
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray.data import Schema
from ray.data._internal.util import rows_same
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa

# deltalake's write_deltalake requires pyarrow >= 15 for the Arrow C Stream interface.
_pa_version = get_pyarrow_version()
assert _pa_version is not None, "pyarrow must be installed to run these tests"

pytestmark = pytest.mark.skipif(
    _pa_version < parse_version("15.0.0"),
    reason="deltalake write_deltalake requires pyarrow >= 15.0",
)


def _column_mapping_protocol():
    """Return protocol features that make delta-rs reject column mapping."""
    return {
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["columnMapping"],
        "writerFeatures": ["columnMapping"],
    }


def _column_mapping_metadata(
    schema, *, max_column_id, partition_columns=(), mode="name", table_id=None
):
    return {
        "id": table_id or str(uuid.uuid4()),
        "format": {"provider": "parquet", "options": {}},
        "schemaString": json.dumps(schema),
        "partitionColumns": list(partition_columns),
        "configuration": {
            "delta.columnMapping.mode": mode,
            "delta.columnMapping.maxColumnId": str(max_column_id),
        },
        "createdTime": int(time.time() * 1000),
    }


def _add_action(table, data_file, partition_values=None):
    return {
        "add": {
            "path": str(data_file.relative_to(table)),
            "partitionValues": partition_values or {},
            "size": data_file.stat().st_size,
            "modificationTime": int(data_file.stat().st_mtime * 1000),
            "dataChange": True,
        }
    }


def _write_delta_log(table, actions, version=0):
    (table / "_delta_log" / f"{version:020d}.json").write_text(
        "\n".join(json.dumps(action) for action in actions) + "\n"
    )


@pytest.mark.parametrize(
    "batch_size",
    [1, 100],
)
@pytest.mark.parametrize(
    "write_mode",
    ["append", "overwrite"],
)
def test_delta_read_basic(tmp_path, batch_size, write_mode):
    from deltalake import write_deltalake

    # Parse the data path.
    path = os.path.join(tmp_path, "tmp_test_delta")

    # Create a sample Delta Lake table
    df = pd.DataFrame(
        {"x": [42] * batch_size, "y": ["a"] * batch_size, "z": [3.14] * batch_size}
    )
    table = pa.Table.from_pandas(df)
    if write_mode == "append":
        write_deltalake(path, table, mode=write_mode)
        write_deltalake(path, table, mode=write_mode)
        expected = pd.concat([df, df], ignore_index=True)
    elif write_mode == "overwrite":
        write_deltalake(path, table, mode=write_mode)
        expected = df
    else:
        raise ValueError(f"Unexpected write_mode: {write_mode}")

    # Read the Delta Lake table
    ds = ray.data.read_delta(path)

    assert ds.schema() == Schema(
        pa.schema(
            {
                "x": pa.int64(),
                "y": pa.string(),
                "z": pa.float64(),
            }
        )
    )
    assert rows_same(ds.to_pandas(), expected)


@pytest.mark.parametrize(
    "columns, expected_columns",
    [
        (["a", "c"], ["a", "c"]),
        (["b"], ["b"]),
        (["a", "b", "c"], ["a", "b", "c"]),
    ],
)
def test_delta_read_column_selection(tmp_path, columns, expected_columns):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_cols")
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [1.0, 2.0, 3.0]})
    write_deltalake(path, pa.Table.from_pandas(df))

    ds = ray.data.read_delta(path, columns=columns)
    expected = df[expected_columns]

    assert ds.schema().names == expected_columns
    assert rows_same(ds.to_pandas(), expected)


@pytest.mark.parametrize(
    "version, expected_data",
    [
        (0, {"x": [1, 2]}),
        (1, {"x": [3, 4, 5]}),
        (None, {"x": [3, 4, 5]}),
    ],
)
def test_delta_read_version(tmp_path, version, expected_data):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_version")
    write_deltalake(path, pa.table({"x": [1, 2]}))
    write_deltalake(path, pa.table({"x": [3, 4, 5]}), mode="overwrite")

    ds = ray.data.read_delta(path, version=version)
    expected = pd.DataFrame(expected_data)

    assert rows_same(ds.to_pandas(), expected)


def test_delta_read_schema_evolution(tmp_path):
    """Older files missing newer columns should be null-filled."""
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_schema_evo")

    write_deltalake(path, pa.table({"x": [1, 2]}))
    write_deltalake(
        path,
        pa.table({"x": [3, 4], "y": ["a", "b"]}),
        mode="append",
        schema_mode="merge",  # pyrefly: ignore[unexpected-keyword]
    )

    ds = ray.data.read_delta(path)
    expected = pd.DataFrame(
        {"x": [1, 2, 3, 4], "y": [None, None, "a", "b"]},
    )
    # Match the Arrow-backed null sentinel produced by ``to_pandas()``.
    expected["y"] = expected["y"].astype("string")

    assert rows_same(ds.to_pandas(), expected)


@pytest.mark.parametrize(
    "storage_options",
    [{}, None],
)
def test_delta_read_storage_options(tmp_path, storage_options):
    """Verify that storage_options are forwarded to DeltaTable."""
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_storage_opts")
    df = pd.DataFrame({"x": [1, 2, 3]})
    write_deltalake(path, pa.Table.from_pandas(df))

    ds = ray.data.read_delta(path, storage_options=storage_options)
    assert rows_same(ds.to_pandas(), df)


def test_delta_read_empty_table(tmp_path):
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "tmp_test_delta_empty")
    write_deltalake(path, pa.table({"x": pa.array([], type=pa.int64())}))

    ds = ray.data.read_delta(path)
    assert ds.count() == 0


def test_delta_read_rejects_multiple_paths():
    with pytest.raises(ValueError, match="Only a single Delta Lake table path"):
        ray.data.read_delta(["path1", "path2"])


# ---------------------------------------------------------------------------
# Column-mapping adapter tests
# ---------------------------------------------------------------------------


def _create_name_mapped_delta_table(tmp_path, table_name="cm_table"):
    """Build a minimal Delta table with name column mapping via raw JSON log.

    Returns the table path and a dict of logical→physical column names.
    """
    import pyarrow.parquet as pq

    table = Path(tmp_path) / table_name
    (table / "_delta_log").mkdir(parents=True, exist_ok=True)

    logical_a = "logical_a"
    logical_b = "logical_b"
    physical_a = "col-aaaa-1111"
    physical_b = "col-bbbb-2222"

    # Write parquet files with physical column names.
    data_file = table / "part-00000.parquet"
    pq.write_table(
        pa.table({physical_a: [1, 2], physical_b: [10.0, 20.0]}),
        data_file,
    )

    schema = {
        "type": "struct",
        "fields": [
            {
                "name": logical_a,
                "type": "long",
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": physical_a,
                },
            },
            {
                "name": logical_b,
                "type": "double",
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 2,
                    "delta.columnMapping.physicalName": physical_b,
                },
            },
        ],
    }
    actions = [
        {"protocol": _column_mapping_protocol()},
        {"metaData": _column_mapping_metadata(schema, max_column_id=2)},
        _add_action(table, data_file),
    ]
    _write_delta_log(table, actions)

    return str(table), {logical_a: physical_a, logical_b: physical_b}


def test_delta_read_column_mapping_schema_and_values(tmp_path):
    """Column-mapped table should expose logical names, not physical UUIDs."""
    table_path, mapping = _create_name_mapped_delta_table(tmp_path)
    ds = ray.data.read_delta(table_path)

    # Schema should contain logical names.
    schema_names = ds.schema().names
    for logical_name in mapping:
        assert logical_name in schema_names, f"Missing logical name {logical_name!r}"
    for physical_name in mapping.values():
        assert (
            physical_name not in schema_names
        ), f"Physical name {physical_name!r} leaked into schema"

    # Data values should be correct.
    df = ds.to_pandas()
    assert list(df.columns) == ["logical_a", "logical_b"]
    assert list(df["logical_a"]) == [1, 2]
    assert list(df["logical_b"]) == [10.0, 20.0]


def test_delta_read_column_mapping_column_selection(tmp_path):
    """User can request logical column names; only those columns are read."""
    table_path, _ = _create_name_mapped_delta_table(tmp_path)

    ds = ray.data.read_delta(table_path, columns=["logical_a"])
    assert ds.schema().names == ["logical_a"]
    df = ds.to_pandas()
    assert list(df.columns) == ["logical_a"]
    assert list(df["logical_a"]) == [1, 2]


def test_delta_read_column_mapping_multiple_files(tmp_path):
    """Column mapping works across multiple parquet files."""
    import pyarrow.parquet as pq

    table = Path(tmp_path) / "multi_file"
    (table / "_delta_log").mkdir(parents=True)

    logical_x = "metric"
    physical_x = "col-dddd-3333"

    # Two data files with different rows.
    for i, fname in enumerate(["part-00000.parquet", "part-00001.parquet"]):
        f = table / fname
        pq.write_table(pa.table({physical_x: [100 * i + 1, 100 * i + 2]}), f)

    schema = {
        "type": "struct",
        "fields": [
            {
                "name": logical_x,
                "type": "long",
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": physical_x,
                },
            }
        ],
    }
    file_entries = []
    for fname in ["part-00000.parquet", "part-00001.parquet"]:
        f = table / fname
        file_entries.append(_add_action(table, f))
    actions = [
        {"protocol": _column_mapping_protocol()},
        {"metaData": _column_mapping_metadata(schema, max_column_id=1)},
        *file_entries,
    ]
    _write_delta_log(table, actions)

    ds = ray.data.read_delta(str(table))
    df = ds.to_pandas()
    assert sorted(df[logical_x].tolist()) == [1, 2, 101, 102]


def _create_partitioned_name_mapped_delta_table(
    tmp_path,
    table_name="partitioned_name_mapped",
    partition_name="country",
    partition_type="string",
    partition_values=(("US", [1, 2]), ("CA", [3])),
):
    """Create name-mapped data files whose partition values live in the Delta log."""
    import pyarrow.parquet as pq

    table = Path(tmp_path) / table_name
    (table / "_delta_log").mkdir(parents=True)
    logical_value, physical_value = "value", "col-value"
    logical_partition, physical_partition = partition_name, f"col-{partition_name}"

    add_actions = []
    for partition_value, values in partition_values:
        partition_dir = table / f"{physical_partition}={partition_value}"
        partition_dir.mkdir()
        data_file = partition_dir / f"part-{partition_value}.parquet"
        pq.write_table(pa.table({physical_value: values}), data_file)
        add_actions.append(
            _add_action(table, data_file, {physical_partition: partition_value})
        )

    schema = {
        "type": "struct",
        "fields": [
            {
                "name": logical_value,
                "type": "long",
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": physical_value,
                },
            },
            {
                "name": logical_partition,
                "type": partition_type,
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 2,
                    "delta.columnMapping.physicalName": physical_partition,
                },
            },
        ],
    }
    actions = [
        {"protocol": _column_mapping_protocol()},
        {
            "metaData": _column_mapping_metadata(
                schema,
                max_column_id=2,
                # Delta metadata stores logical partition names; file paths and
                # Add.partitionValues use their physical name in name mode.
                partition_columns=[logical_partition],
            )
        },
        *add_actions,
    ]
    _write_delta_log(table, actions)
    return str(table)


def test_delta_read_column_mapping_partition_projection_and_filter(tmp_path):
    """Partition columns come from active Delta Add actions and stay logical."""
    from ray.data.expressions import col

    table_path = _create_partitioned_name_mapped_delta_table(tmp_path)

    ds = ray.data.read_delta(table_path)
    assert ds.schema().names == ["value", "country"]
    assert rows_same(
        ds.to_pandas(),
        pd.DataFrame({"value": [1, 2, 3], "country": ["US", "US", "CA"]}),
    )

    projected = ray.data.read_delta(table_path, columns=["country"])
    assert projected.schema().names == ["country"]
    assert sorted(projected.to_pandas()["country"].tolist()) == ["CA", "US", "US"]

    filtered = ray.data.read_delta(table_path).filter(expr=col("country") == "US")
    assert filtered.to_pandas()["value"].tolist() == [1, 2]


def test_delta_read_column_mapping_casts_partition_values(tmp_path):
    """String Delta Add partition values cast to their logical Arrow type."""
    table_path = _create_partitioned_name_mapped_delta_table(
        tmp_path,
        table_name="integer_partition",
        partition_name="year",
        partition_type="long",
        partition_values=(("2024", [1, 2]), ("2025", [3])),
    )

    ds = ray.data.read_delta(table_path)
    assert ds.schema().names == ["value", "year"]
    assert ds.to_pandas()["year"].tolist() == [2024, 2024, 2025]


def test_delta_read_column_mapping_schema_evolution(tmp_path):
    """Current Delta schema null-fills columns missing from older files."""
    import pyarrow.parquet as pq

    table = Path(tmp_path) / "evolving_name_mapped"
    (table / "_delta_log").mkdir(parents=True)
    physical_a, physical_b = "col-a", "col-b"
    first_file = table / "part-00000.parquet"
    second_file = table / "part-00001.parquet"
    pq.write_table(pa.table({physical_a: [1, 2]}), first_file)
    pq.write_table(pa.table({physical_a: [3], physical_b: ["new"]}), second_file)
    table_id = str(uuid.uuid4())

    def schema(include_b):
        fields = [
            {
                "name": "a",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    "delta.columnMapping.physicalName": physical_a,
                },
            }
        ]
        if include_b:
            fields.append(
                {
                    "name": "b",
                    "type": "string",
                    "nullable": True,
                    "metadata": {
                        "delta.columnMapping.id": 2,
                        "delta.columnMapping.physicalName": physical_b,
                    },
                }
            )
        return {"type": "struct", "fields": fields}

    first_actions = [
        {"protocol": _column_mapping_protocol()},
        {
            "metaData": _column_mapping_metadata(
                schema(False), max_column_id=1, table_id=table_id
            )
        },
        _add_action(table, first_file),
    ]
    second_actions = [
        {
            "metaData": _column_mapping_metadata(
                schema(True), max_column_id=2, table_id=table_id
            )
        },
        _add_action(table, second_file),
    ]
    _write_delta_log(table, first_actions)
    _write_delta_log(table, second_actions, version=1)

    ds = ray.data.read_delta(str(table))
    assert ds.schema().names == ["a", "b"]
    assert rows_same(
        ds.to_pandas(),
        pd.DataFrame({"a": [1, 2, 3], "b": [None, None, "new"]}),
    )


def test_delta_read_column_mapping_rejects_id_mode(tmp_path):
    """Tables using 'id' column mapping should raise NotImplementedError."""
    import pyarrow.parquet as pq

    table = Path(tmp_path) / "id_mapped"
    (table / "_delta_log").mkdir(parents=True)

    data_file = table / "part-00000.parquet"
    pq.write_table(pa.table({"col1": [1]}), data_file)

    schema = {
        "type": "struct",
        "fields": [
            {
                "name": "col1",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "delta.columnMapping.id": 1,
                    # delta-rs validates physical names before Ray can reject
                    # unsupported id mapping.
                    "delta.columnMapping.physicalName": "col1",
                },
            }
        ],
    }
    actions = [
        {"protocol": _column_mapping_protocol()},
        {"metaData": _column_mapping_metadata(schema, max_column_id=1, mode="id")},
        _add_action(table, data_file),
    ]
    _write_delta_log(table, actions)

    with pytest.raises(NotImplementedError, match="id.*column mapping"):
        ray.data.read_delta(str(table))


def test_delta_read_non_mapped_table_still_works(tmp_path):
    """Non-mapped tables should still work via the normal path."""
    from deltalake import write_deltalake

    path = os.path.join(tmp_path, "no_mapping")
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    write_deltalake(path, pa.Table.from_pandas(df))

    ds = ray.data.read_delta(path)
    assert ds.schema().names == ["a", "b"]
    assert rows_same(ds.to_pandas(), df)


_DATABRICKS_INTEGRATION_ENV_VARS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "RAY_DATABRICKS_TEST_TABLE",
    "RAY_DATABRICKS_TEST_COLUMNS",
)


@pytest.mark.skipif(
    any(not os.getenv(name) for name in _DATABRICKS_INTEGRATION_ENV_VARS),
    reason="requires Databricks credentials and a name-mapped Unity Catalog table",
)
def test_delta_read_name_mapped_unity_catalog_table():
    """Read a real name-mapped Delta table through Unity Catalog.

    This integration test is opt-in. Set ``DATABRICKS_HOST``,
    ``DATABRICKS_TOKEN``, ``RAY_DATABRICKS_TEST_TABLE`` (a three-part Unity
    Catalog name), and ``RAY_DATABRICKS_TEST_COLUMNS`` (comma-separated logical
    column names). Set ``AWS_REGION`` too for an AWS-backed table.
    """
    expected_columns = [
        column.strip()
        for column in os.environ["RAY_DATABRICKS_TEST_COLUMNS"].split(",")
    ]
    catalog = ray.data.DatabricksUnityCatalog(
        url=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
        region=os.getenv("AWS_REGION"),
    )

    ds = ray.data.read_delta(os.environ["RAY_DATABRICKS_TEST_TABLE"], catalog=catalog)

    assert ds.schema().names == expected_columns
    assert ds.take(1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
