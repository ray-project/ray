import os

import pyarrow as pa
import pytest
from pyarrow import orc

import ray


def _write_orc(path, table):
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink)


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


# ---------------------------------------------------------------------------
# Projection pushdown tests
# ---------------------------------------------------------------------------


def test_read_orc_projection_pushdown_physical(ray_start_regular_shared, tmp_path):
    """Pure-project projection: select_columns(["value","id"]) on a file
    with schema [id,name,value] must return only the requested columns in
    the requested order, and the pure Project must be removed from the
    optimized plan."""
    if ray.data.DataContext.get_current().use_datasource_v2:
        pytest.skip("Plan-string assertion is V1-specific (ReadORC vs V2 ListFiles-ReadFiles chain).")
    path = os.path.join(tmp_path, "proj.orc")
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"], "value": [10.0, 20.0, 30.0]})
    _write_orc(path, table)

    ds = ray.data.read_orc(path, override_num_blocks=1).select_columns(["value", "id"])

    assert ds.schema().names == ["value", "id"]
    rows = ds.take_all()
    assert [dict(r) for r in rows] == [
        {"value": 10.0, "id": 1},
        {"value": 20.0, "id": 2},
        {"value": 30.0, "id": 3},
    ]

    from ray.data._internal.util import explain_plan
    assert "Project" not in explain_plan(ds._logical_plan).strip().splitlines()


def test_read_orc_projection_pushdown_multi_stripe(ray_start_regular_shared, tmp_path):
    """Multi-stripe file projected to select only one column still preserves
    all rows, and reads below the stripe count."""
    path = os.path.join(tmp_path, "multi.orc")
    table = pa.table({"id": list(range(10000)), "x": [1] * 10000, "y": [2.0] * 10000})
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink, stripe_size=64 * 1024)

    ds = ray.data.read_orc(path).select_columns(["x"])
    assert ds.count() == 10000
    assert ds.schema().names == ["x"]


def test_read_orc_projection_pushdown_empty(ray_start_regular_shared, tmp_path):
    """select_columns([]) must preserve row counts across multiple stripes
    and not expose the internal stub to schema/columns."""
    path = os.path.join(tmp_path, "empty.orc")
    table = pa.table({"id": list(range(5000)), "extra": [1] * 5000, "other": ["x"] * 5000})
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink, stripe_size=64 * 1024)

    ds = ray.data.read_orc(path).select_columns([])
    assert ds.count() == 5000
    assert ds.take_all() == []


def test_read_orc_projection_pushdown_partitioned(ray_start_regular_shared, tmp_path):
    """Pure partition-only projection selects only partition columns,
    not physical data."""
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    os.makedirs(os.path.join(tmp_path, "year=2024"))
    _write_orc(
        os.path.join(tmp_path, "year=2024", "data.orc"),
        pa.table({"data": [0, 1]}),
    )

    ds = ray.data.read_orc(
        str(tmp_path), partitioning=Partitioning(PartitionStyle.HIVE)
    ).select_columns(["year"])

    assert ds.count() == 2
    assert ds.schema().names == ["year"]
    rows = ds.take_all()
    assert all(isinstance(r["year"], str) and r["year"] == "2024" for r in rows)


def test_read_orc_projection_pushdown_mixed(ray_start_regular_shared, tmp_path):
    """select_columns(["year","data"]) returns the requested order even when
    data is from the file and year is from the partition."""
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    os.makedirs(os.path.join(tmp_path, "year=2024"))
    _write_orc(
        os.path.join(tmp_path, "year=2024", "data.orc"),
        pa.table({"data": [0, 1]}),
    )

    ds = ray.data.read_orc(
        str(tmp_path), partitioning=Partitioning(PartitionStyle.HIVE)
    ).select_columns(["year", "data"])

    assert ds.schema().names == ["year", "data"]
    rows = ds.take_all()
    assert [dict(r) for r in rows] == [
        {"year": "2024", "data": 0},
        {"year": "2024", "data": 1},
    ]


def test_read_orc_projection_pushdown_include_paths(ray_start_regular_shared, tmp_path):
    """select_columns(["path"]) with include_paths=True returns only the
    synthetic path column, not the file data."""
    path = os.path.join(tmp_path, "data.orc")
    _write_orc(path, pa.table({"id": [0, 1, 2]}))
    ds = ray.data.read_orc(path, include_paths=True).select_columns(["path"])
    assert ds.schema().names == ["path"]
    rows = ds.take_all()
    assert all("path" in r and r["path"].endswith("data.orc") for r in rows)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
