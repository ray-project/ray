import os
import pickle

import pyarrow as pa
import pytest
from pyarrow import orc

import ray
from ray.data._internal.object_extensions.arrow import (
    ARROW_PYTHON_OBJECT_EXTENSION_NAME,
    raise_on_pickle_object_columns,
)


def _write_orc(path, table):
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink)


def _pickle_object_table(value):
    storage = pa.array([pickle.dumps(value)], type=pa.large_binary())
    field = pa.field(
        "col",
        pa.large_binary(),
        metadata={
            b"ARROW:extension:name": ARROW_PYTHON_OBJECT_EXTENSION_NAME.encode(),
            b"ARROW:extension:metadata": b"",
        },
    )
    return pa.Table.from_arrays([storage], schema=pa.schema([field]))


def test_read_orc_rejects_pickle_object_metadata_before_block_transport(tmp_path):
    path = os.path.join(tmp_path, "metadata.orc")
    _write_orc(path, _pickle_object_table({"key": "value"}))

    with pa.OSFile(path, "rb") as source:
        orc_file = orc.ORCFile(source)
        table = pa.Table.from_batches([orc_file.read_stripe(0)])

    with pytest.raises(ValueError, match="arrow_pickled_object"):
        raise_on_pickle_object_columns(table)


def test_read_orc_allows_pickle_object_columns_with_env_var(
    tmp_path, shutdown_only, monkeypatch
):
    # Set the environment variable on both the driver and worker processes.
    monkeypatch.setenv("RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR", "1")
    ray.init(runtime_env={"env_vars": {"RAY_DATA_AUTOLOAD_PICKLE_OBJECT_SCALAR": "1"}})

    path = os.path.join(tmp_path, "trusted.orc")
    _write_orc(path, _pickle_object_table({"key": "value"}))

    rows = ray.data.read_orc(path).take_all()

    assert rows == [{"col": {"key": "value"}}]


def test_read_orc_rejects_pickle_object_columns(tmp_path, ray_start_regular_shared):
    marker = tmp_path / "exploit_marker"

    class Exploit:
        def __reduce__(self):
            return (os.system, (f"touch {marker}",))

    path = os.path.join(tmp_path, "exploit.orc")
    _write_orc(path, _pickle_object_table(Exploit()))

    ds = ray.data.read_orc(path)
    with pytest.raises(Exception, match="arrow_pickled_object"):
        ds.take_all()

    assert not marker.exists(), "pickle.load executed attacker code"


def test_read_orc_basic(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.orc")
    table = pa.table({"id": [0, 1, 2], "name": ["a", "b", "c"]})
    _write_orc(path, table)

    ds = ray.data.read_orc(path)

    assert ds.count() == 3
    assert set(ds.schema().names) == {"id", "name"}
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1, 2]


def test_read_orc_multiple_files(ray_start_regular_shared, tmp_path):
    for i in range(3):
        _write_orc(os.path.join(tmp_path, f"part_{i}.orc"), pa.table({"id": [i]}))

    ds = ray.data.read_orc(str(tmp_path))

    assert ds.count() == 3
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1, 2]


def test_read_orc_include_paths(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.orc")
    _write_orc(path, pa.table({"id": [0]}))

    ds = ray.data.read_orc(path, include_paths=True)

    rows = ds.take_all()
    assert all("path" in row for row in rows)
    assert all(row["path"].endswith("data.orc") for row in rows)


def test_read_orc_ignore_missing_paths(ray_start_regular_shared, tmp_path):
    existing = os.path.join(tmp_path, "data.orc")
    _write_orc(existing, pa.table({"id": [0, 1]}))
    missing = os.path.join(tmp_path, "does_not_exist.orc")

    ds = ray.data.read_orc([existing, missing], ignore_missing_paths=True)
    assert ds.count() == 2

    with pytest.raises(FileNotFoundError):
        ray.data.read_orc([existing, missing], ignore_missing_paths=False).materialize()


def test_read_orc_file_extensions_filtering(ray_start_regular_shared, tmp_path):
    _write_orc(os.path.join(tmp_path, "data.orc"), pa.table({"id": [0, 1]}))
    # A non-ORC file in the same directory should be filtered out by default.
    with open(os.path.join(tmp_path, "_SUCCESS"), "w") as f:
        f.write("")

    ds = ray.data.read_orc(str(tmp_path))
    assert ds.count() == 2

    # A directory with no matching files raises a clear error.
    empty_dir = os.path.join(tmp_path, "empty")
    os.makedirs(empty_dir)
    with open(os.path.join(empty_dir, "_SUCCESS"), "w") as f:
        f.write("")
    with pytest.raises(ValueError):
        ray.data.read_orc(empty_dir)


def test_read_orc_override_num_blocks(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.orc")
    _write_orc(path, pa.table({"id": list(range(100))}))

    ds = ray.data.read_orc(path, override_num_blocks=1)

    assert ds.count() == 100
    assert ds.materialize().num_blocks() == 1


def test_read_orc_partitioned(ray_start_regular_shared, tmp_path):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    os.makedirs(os.path.join(tmp_path, "year=2024"))
    _write_orc(
        os.path.join(tmp_path, "year=2024", "data.orc"),
        pa.table({"data": [0, 1]}),
    )

    ds = ray.data.read_orc(
        str(tmp_path), partitioning=Partitioning(PartitionStyle.HIVE)
    )

    rows = ds.take_all()
    assert sorted(row["data"] for row in rows) == [0, 1]
    assert all(row["year"] == "2024" for row in rows)


def test_read_orc_partitioned_with_partition_filter(ray_start_regular_shared, tmp_path):
    from ray.data.datasource.partitioning import (
        Partitioning,
        PartitionStyle,
        PathPartitionFilter,
    )

    for year in ("2023", "2024"):
        os.makedirs(os.path.join(tmp_path, f"year={year}"))
        _write_orc(
            os.path.join(tmp_path, f"year={year}", "data.orc"),
            pa.table({"data": [0, 1]}),
        )

    partition_filter = PathPartitionFilter.of(
        filter_fn=lambda partitions: partitions["year"] == "2024",
        style=PartitionStyle.HIVE,
    )
    ds = ray.data.read_orc(
        str(tmp_path),
        partitioning=Partitioning(PartitionStyle.HIVE),
        partition_filter=partition_filter,
    )

    rows = ds.take_all()
    assert len(rows) == 2
    assert all(row["year"] == "2024" for row in rows)


def test_read_orc_multiple_stripes(ray_start_regular_shared, tmp_path):

    path = os.path.join(tmp_path, "multi.orc")
    table = pa.table({"id": list(range(10000))})
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink, stripe_size=64 * 1024)

    ds = ray.data.read_orc(path)

    assert ds.count() == 10000
    assert sorted(row["id"] for row in ds.take_all()) == list(range(10000))


def test_read_orc_empty_file(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "empty.orc")
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "name": pa.array([], type=pa.string()),
        }
    )
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink)

    ds = ray.data.read_orc(path)

    assert ds.count() == 0
    assert ds.take_all() == []


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
