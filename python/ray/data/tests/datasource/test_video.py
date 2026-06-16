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
    assert frame_type.value_type == pa.uint8()
    assert frame_index_type == pa.int64()


def test_read_videos_fps():
    # basketball.mp4 is well above 5 fps (confirmed via get_avg_fps()), so a
    # target of 5 fps subsamples with a stride > 1. Frames are kept at a fixed
    # stride and `frame_index` stays the original source index.
    uri = "s3://anonymous@ray-example-data/basketball.mp4"
    ds = ray.data.read_videos(uri, fps=5)

    frame_indices = sorted(
        item["frame_index"] for item in ds.select_columns(["frame_index"]).take_all()
    )
    assert len(frame_indices) > 1
    stride = frame_indices[1] - frame_indices[0]
    assert stride > 1, "expected subsampling when target fps is below source fps"
    assert frame_indices == list(range(0, 333, stride))


def test_read_videos_resize():
    # `resize` is (height, width), mirroring read_images' `size`.
    uri = "s3://anonymous@ray-example-data/basketball.mp4"
    ds = ray.data.read_videos(uri, resize=(120, 160))

    assert ds.count() == 333
    frame_type = ds.schema().types[0]
    assert frame_type.shape == (120, 160, 3)
    assert ds.take(1)[0]["frame"].shape == (120, 160, 3)


def test_read_videos_invalid_params():
    uri = "s3://anonymous@ray-example-data/basketball.mp4"

    # `resize` must contain exactly two positive integers (0 is rejected).
    with pytest.raises(ValueError):
        ray.data.read_videos(uri, resize=(0, 160))
    with pytest.raises(ValueError):
        ray.data.read_videos(uri, resize=(120, 0))
    with pytest.raises(ValueError):
        ray.data.read_videos(uri, resize=(120,))  # pyrefly: ignore[bad-argument-type]

    # `fps` must be positive.
    with pytest.raises(ValueError):
        ray.data.read_videos(uri, fps=0)
    with pytest.raises(ValueError):
        ray.data.read_videos(uri, fps=-1)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
