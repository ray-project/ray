import json
import os
import sys

import pyarrow as pa
import pytest

import ray
from ray._private import ray_constants
from ray.data._internal.execution.interfaces.ref_bundle import RefBundle
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.streaming_executor_state import OpState
from ray.data.block import BlockMetadata
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
    ctx.enforce_schemas = False

    op = InputDataBuffer(ctx, [])
    block_ref = ray.ObjectRef(b"0" * ray_constants.ID_SIZE)
    block_metadata = BlockMetadata(
        num_rows=None, size_bytes=1, exec_stats=None, input_files=None
    )
    schema = pa.schema([pa.field("id", pa.int32()), pa.field("name", pa.string())])
    ref_bundle = RefBundle(
        blocks=[(block_ref, block_metadata)], schema=schema, owns_blocks=True
    )
    op_state = OpState(op, [])
    op_state.add_output(ref_bundle)

    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["event_data"]["schema_fields"] == {"name": "string", "id": "int32"}

    schema2 = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field("name", pa.string()),
            pa.field("age", pa.int32()),
        ]
    )
    ref_bundle2 = RefBundle(
        blocks=[(block_ref, block_metadata)], schema=schema2, owns_blocks=True
    )
    op_state.add_output(ref_bundle2)

    # enforce_schemas = False, so the updated schema will not be exported
    data = _get_exported_data()
    assert len(data) == 1
    assert data[0]["event_data"]["schema_fields"] == {"name": "string", "id": "int32"}

    # enforce_schemas = True, so the updated schema will be exported
    ctx.enforce_schemas = True
    op_state.add_output(ref_bundle2)
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
