import numpy as np
import pyarrow as pa
import pytest

import ray


def test_read_videos():
    uri = "s3://anonymous@ray-example-data/basketball.mp4"
    ds = ray.data.read_videos(uri, include_timestamps=True)

    assert ds.count() == 333
    assert ds.schema().names == ["frame", "frame_index", "frame_timestamp"]

    frame_indices = ds.select_columns(["frame_index"]).take_all()
    assert sorted(frame_indices, key=lambda item: item["frame_index"]) == [
        {"frame_index": i} for i in range(333)
    ]

    frame_timestamps = ds.select_columns(["frame_timestamp"]).take_all()
    for t in frame_timestamps:
        assert isinstance(t["frame_timestamp"], np.ndarray)
        assert t["frame_timestamp"].shape[0] == 2

    frame_type, frame_index_type, _ = ds.schema().types

    assert frame_type.shape == (720, 1280, 3)
    assert frame_type.scalar_type == pa.uint8()
    assert frame_index_type == pa.int64()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
