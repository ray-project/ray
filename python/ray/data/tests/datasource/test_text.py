import os

import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _to_lines(rows):
    return [row["text"] for row in rows]


def test_empty_text_files(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    # 2 empty files.
    _ = open(os.path.join(path, "file1.txt"), "w")
    _ = open(os.path.join(path, "file2.txt"), "w")
    ds = ray.data.read_text(path)
    assert ds.count() == 0
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 0


def test_read_text(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    with open(os.path.join(path, "file1.txt"), "w") as f:
        f.write("hello\n")
        f.write("world")
    with open(os.path.join(path, "file2.txt"), "w") as f:
        f.write("goodbye")
    with open(os.path.join(path, "file3.txt"), "w") as f:
        f.write("ray\n")
    ds = ray.data.read_text(path)
    assert sorted(_to_lines(ds.take())) == ["goodbye", "hello", "ray", "world"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 4


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
