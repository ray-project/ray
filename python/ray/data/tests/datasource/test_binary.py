import os
from io import BytesIO

import pytest
import snappy

import ray
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.util import extract_values, gen_bin_files
from ray.tests.conftest import *  # noqa

pytestmark = pytest.mark.usefixtures("datasource_v2")


def test_read_binary_files(ray_start_regular_shared):
    with gen_bin_files(10) as (_, paths):
        ds = ray.data.read_binary_files(paths)
        for i, item in enumerate(ds.iter_rows()):
            expected = open(paths[i], "rb").read()
            assert expected == item["bytes"]
        # Test metadata ops.
        assert ds.count() == 10
        assert "bytes" in str(ds.schema()), ds
        assert "bytes" in str(ds), ds


def test_read_binary_snappy(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_binary_snappy")
    os.mkdir(path)
    with open(os.path.join(path, "file"), "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(
        path,
        arrow_open_stream_args=dict(compression="snappy"),
    )
    assert sorted(extract_values("bytes", ds.take())) == [byte_str]


def test_read_binary_snappy_inferred(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "test_binary_snappy_inferred")
    os.mkdir(path)
    with open(os.path.join(path, "file.snappy"), "wb") as f:
        byte_str = "hello, world".encode()
        bytes = BytesIO(byte_str)
        snappy.stream_compress(bytes, f)
    ds = ray.data.read_binary_files(path)
    assert sorted(extract_values("bytes", ds.take())) == [byte_str]


def test_read_binary_files_empty_file_is_one_empty_row(
    ray_start_regular_shared, tmp_path
):
    with open(os.path.join(tmp_path, "empty.bin"), "wb"):
        pass
    with open(os.path.join(tmp_path, "data.bin"), "wb") as f:
        f.write(b"a")

    ds = ray.data.read_binary_files(str(tmp_path))

    assert sorted(extract_values("bytes", ds.take_all())) == [b"", b"a"]


@pytest.fixture
def partitioned_bin_dir(tmp_path):
    for year, content in (("2023", b"one"), ("2024", b"two")):
        os.mkdir(os.path.join(tmp_path, f"year={year}"))
        with open(os.path.join(tmp_path, f"year={year}", "data.bin"), "wb") as f:
            f.write(content)
    partitioning = Partitioning(
        PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
    )
    return str(tmp_path), partitioning


def test_read_binary_files_partitioned_with_paths(
    ray_start_regular_shared, partitioned_bin_dir
):
    path, partitioning = partitioned_bin_dir

    ds = ray.data.read_binary_files(path, partitioning=partitioning, include_paths=True)

    assert ds.schema().names == ["bytes", "year", "path"]
    rows = sorted(ds.take_all(), key=lambda row: row["year"])
    assert [(row["bytes"], row["year"]) for row in rows] == [
        (b"one", 2023),
        (b"two", 2024),
    ]
    assert all(row["path"].endswith(f"year={row['year']}/data.bin") for row in rows)


def test_read_binary_files_pushdowns(ray_start_regular_shared, partitioned_bin_dir):
    path, partitioning = partitioned_bin_dir

    ds = ray.data.read_binary_files(path, partitioning=partitioning)

    assert extract_values("bytes", ds.filter(expr=col("year") == 2024).take_all()) == [
        b"two"
    ]
    assert ds.select_columns(["year"]).count() == 2
    assert ds.limit(1).count() == 1
    assert ds.count() == 2


def test_read_binary_files_file_extensions(ray_start_regular_shared, tmp_path):
    with open(os.path.join(tmp_path, "data.bin"), "wb") as f:
        f.write(b"a")
    with open(os.path.join(tmp_path, "notes.txt"), "wb") as f:
        f.write(b"b")

    ds = ray.data.read_binary_files(str(tmp_path), file_extensions=["bin"])

    assert extract_values("bytes", ds.take_all()) == [b"a"]


def test_read_binary_files_ignore_missing_paths(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.bin")
    with open(path, "wb") as f:
        f.write(b"a")
    missing = os.path.join(tmp_path, "missing.bin")

    ds = ray.data.read_binary_files([path, missing], ignore_missing_paths=True)

    assert extract_values("bytes", ds.take_all()) == [b"a"]
    with pytest.raises(FileNotFoundError):
        ray.data.read_binary_files([path, missing]).take_all()


def test_read_binary_files_shuffle(ray_start_regular_shared, tmp_path):
    for i in range(5):
        with open(os.path.join(tmp_path, f"{i}.bin"), "wb") as f:
            f.write(bytes([i]))

    ds = ray.data.read_binary_files(
        str(tmp_path), shuffle=ray.data.FileShuffleConfig(seed=0)
    )

    assert sorted(extract_values("bytes", ds.take_all())) == [
        bytes([i]) for i in range(5)
    ]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
