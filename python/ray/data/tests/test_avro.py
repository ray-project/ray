import os

import pytest

import ray

schema = {
    "type": "record",
    "name": "TestRecord",
    "fields": [{"name": "test_field", "type": "string"}],
}


def create_sample_avro_file(filepath):
    """A placeholder function to create a sample Avro file for testing.
    Implement this function based on your Avro schema and data requirements."""
    import fastavro

    records = [{"test_field": "test_value1"}, {"test_field": "test_value2"}]
    with open(filepath, "wb") as out:
        fastavro.writer(out, schema, records)


def test_read_basic_avro_file(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "sample.avro")
    create_sample_avro_file(path)
    ds = ray.data.read_avro(path)
    expected = [{"test_field": "test_value1"}, {"test_field": "test_value2"}]
    assert ds.take() == expected


def test_empty_avro_files(ray_start_regular_shared, tmp_path):
    import fastavro

    path = os.path.join(tmp_path, "empty.avro")
    # Write an empty Avro file with the schema
    with open(path, "wb") as out:
        # Write the schema with no records
        fastavro.writer(out, schema, [])
    ds = ray.data.read_avro(path)
    assert ds.count() == 0


if __name__ == "__main__":
    pytest.main(["-v", __file__])
