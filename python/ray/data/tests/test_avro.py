import os

import pytest

import ray
from ray.data.datasource import AvroDatasource


@pytest.fixture(scope="session")
def ray_start_regular_shared():
    if not ray.is_initialized():
        # Initialize Ray if it's not already running. Adjust num_cpus as necessary.
        ray.init(num_cpus=2)
    else:
        # Ray is already initialized, so no further action is required.
        pass
    yield None
    # No need to call ray.shutdown() here since it would terminate the existing session.


def create_sample_avro_file(filepath):
    """A placeholder function to create a sample Avro file for testing.
    Implement this function based on your Avro schema and data requirements."""
    import fastavro

    schema = {
        "type": "record",
        "name": "TestRecord",
        "fields": [{"name": "test_field", "type": "string"}],
    }
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
    path = os.path.join(tmp_path, "empty.avro")
    open(path, "a").close()  # Just touch the file, no Avro content
    ds = ray.data.read_avro(path)
    assert ds.count() == 0


if __name__ == "__main__":
    # Running the tests
    pytest.main(["-v", __file__])
