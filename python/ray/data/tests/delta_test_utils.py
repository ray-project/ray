"""Shared Delta Lake test helpers for the read and write tests."""

import json
import os

import pyarrow as pa
import pyarrow.parquet as pq


def write_column_mapped_table(path: str, mode: str) -> None:
    """Write a one-file reader 2 / writer 5 Delta table with column mapping.

    deltalake can't write a column-mapped table: it stores the property but
    keeps the column names in the data files and a reader 1 protocol. So the
    log is written by hand, with the data file storing column ``id`` (values 1
    and 2) under the physical name ``col-a1``, as Spark and Databricks write it.

    Args:
        path: Directory to create the table in. Must not exist yet.
        mode: The ``delta.columnMapping.mode`` to set, e.g. ``"name"`` or ``"id"``.
    """
    os.makedirs(os.path.join(path, "_delta_log"))
    data_file = os.path.join(path, "part-0.parquet")
    pq.write_table(pa.table({"col-a1": pa.array([1, 2], pa.int64())}), data_file)

    field = {
        "name": "id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "delta.columnMapping.id": 1,
            "delta.columnMapping.physicalName": "col-a1",
        },
    }
    actions = [
        {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
        {
            "metaData": {
                "id": "3c3a3d8e-0000-4000-8000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": [field]}),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": mode,
                    "delta.columnMapping.maxColumnId": "1",
                },
                "createdTime": 0,
            }
        },
        {
            "add": {
                "path": "part-0.parquet",
                "partitionValues": {},
                "size": os.path.getsize(data_file),
                "modificationTime": 0,
                "dataChange": True,
            }
        },
    ]
    with open(os.path.join(path, "_delta_log", f"{0:020d}.json"), "w") as f:
        f.write("\n".join(json.dumps(action) for action in actions) + "\n")
