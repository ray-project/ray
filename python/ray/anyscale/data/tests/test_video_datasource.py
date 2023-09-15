import pyarrow as pa
import pytest

import ray
from ray.anyscale.data import VideoDatasource
from ray.anyscale.data.video_datasource import _VideoDatasourceReader


def test_video_datasource():
    uri = "s3://anonymous@ray-example-data/basketball.mp4"
    ds = ray.data.read_datasource(VideoDatasource(), paths=uri)

    assert ds.count() == 333
    assert ds.schema().names == ["frame", "frame_index"]

    frame_indices = ds.select_columns(["frame_index"]).take_all()
    assert sorted(frame_indices, key=lambda item: item["frame_index"]) == [
        {"frame_index": i} for i in range(333)
    ]

    frame_type, frame_index_type = ds.schema().types
    assert frame_type.shape == (720, 1280, 3)
    assert frame_type.scalar_type == pa.uint8()
    assert frame_index_type == pa.int64()


def test_in_memory_size_estimation():
    uri = "s3://anonymous@antoni-test/sewer-videos/sewer_example_0.mp4"
    datasource = VideoDatasource()

    reader = _VideoDatasourceReader(datasource, paths=uri)
    estimated_size = reader.estimate_inmemory_data_size()

    ds = ray.data.read_datasource(datasource, paths=uri).materialize()
    actual_size = ds.size_bytes()

    percent_error = (abs(estimated_size - actual_size) / actual_size) * 100
    assert percent_error < 5  # This threshold is completely arbitrary.


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
