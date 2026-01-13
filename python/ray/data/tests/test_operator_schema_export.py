import json
import os
import sys

import pyarrow as pa
import pytest

import ray
from ray._private import ray_constants
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data.context import DataContext


def _get_exported_data():
    exported_file = os.path.join(
        ray._private.worker._global_node.get_session_dir_path(),
        "logs",
        "export_events",
        "event_EXPORT_DATASET_OPERATOR_SCHEMA.log",
    )
    assert os.path.isfile(exported_file)

    with open(exported_file, "r") as f:
        data = f.readlines()

    return [json.loads(line) for line in data]


def test_export_operator_schema():
    ray.init()
    ray_constants.RAY_ENABLE_EXPORT_API_WRITE = True
    ctx = DataContext.get_current()

    op = InputDataBuffer(ctx, [])
    executor = StreamingExecutor(ctx)

    # Do not export schema if it's None
    executor._export_operator_schema(op)
    data = _get_exported_data()
    assert len(data) == 0

    # Export if it's a valid schema
    schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    executor._op_schema[op] = schema
    executor._export_operator_schema(op)
    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["event_data"]["schema_fields"] == {"name": "string", "id": "int32"}

    # Export updated schema of the same operator
    schema2 = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("age", pa.int32()),
        ]
    )
    executor._op_schema[op] = schema2
    executor._export_operator_schema(op)
    data = _get_exported_data()
    assert len(data) == 2
    assert (
        data[0]["event_data"]["operator_uuid"] == data[1]["event_data"]["operator_uuid"]
    )
    assert data[0]["event_data"]["schema_fields"] == {"name": "string", "id": "int32"}
    assert data[1]["event_data"]["schema_fields"] == {
        "name": "string",
        "id": "int32",
        "age": "int32",
    }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
