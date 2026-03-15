import os

import fastavro
import pytest

import ray
from ray.data.block import BlockAccessor
from ray.data.context import DataContext

schema = {
    "type": "record",
    "name": "TestRecord",
    "fields": [{"name": "test_field", "type": "string"}],
}


def test_read_basic_avro_file(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "sample.avro")
    records = [{"test_field": "test_value1"}, {"test_field": "test_value2"}]
    with open(path, "wb") as out:
        fastavro.writer(out, schema, records)

    ds = ray.data.read_avro(path)

    expected = [{"test_field": "test_value1"}, {"test_field": "test_value2"}]
    assert ds.take_all() == expected


def test_read_empty_avro_files(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "empty.avro")
    # Write an empty Avro file with the schema
    with open(path, "wb") as out:
        # Write the schema with no records
        fastavro.writer(out, schema, [])

    ds = ray.data.read_avro(path)

    assert ds.count() == 0


def test_read_avro_target_max_block_rows(
    ray_start_regular_shared, tmp_path, restore_data_context
):
    ctx = DataContext.get_current()
    ctx.target_max_block_size = None
    ctx.target_max_block_rows = 10

    path = os.path.join(tmp_path, "sample_100.avro")
    records = [{"test_field": f"test_value{i}"} for i in range(100)]
    with open(path, "wb") as out:
        fastavro.writer(out, schema, records)

    ds = ray.data.read_avro(path).materialize()

    assert ds.num_blocks() >= 10
    for block_ref in ds.get_internal_block_refs():
        block = ray.get(block_ref)
        assert BlockAccessor.for_block(block).num_rows() <= 10


if __name__ == "__main__":
    pytest.main(["-v", __file__])
