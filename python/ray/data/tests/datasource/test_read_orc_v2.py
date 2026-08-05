"""Integration tests for ``ray.data.read_orc()`` on DataSourceV2."""

import pyarrow as pa
import pyarrow.orc as orc
import pytest

import ray
from ray.data._internal.datasource_v2.partitioners.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data.expressions import col


def _write(path, table):
    orc.write_table(table, str(path), stripe_size=64, batch_size=2)


def test_read_orc_builds_list_files_read_files_chain(tmp_path):
    _write(tmp_path / "data.orc", pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    ds = ray.data.read_orc(str(tmp_path))

    assert isinstance(ds._logical_plan.dag, ReadFiles)
    assert isinstance(ds._logical_plan.dag.input_dependencies[0], ListFiles)
    assert ds.schema().names == ["a", "b"]
    assert sorted(ds.take_all(), key=lambda row: row["a"]) == [
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    ]


def test_read_orc_hive_partitioned(tmp_path):
    for color in ["red", "blue"]:
        directory = tmp_path / f"color={color}"
        directory.mkdir()
        _write(directory / "data.orc", pa.table({"id": [1, 2]}))

    ds = ray.data.read_orc(str(tmp_path))

    rows = sorted(ds.take_all(), key=lambda row: (row["color"], row["id"]))
    assert rows == [
        {"id": 1, "color": "blue"},
        {"id": 2, "color": "blue"},
        {"id": 1, "color": "red"},
        {"id": 2, "color": "red"},
    ]


def test_read_orc_include_paths(tmp_path):
    _write(tmp_path / "data.orc", pa.table({"a": [1, 2, 3]}))

    ds = ray.data.read_orc(str(tmp_path), include_paths=True)
    rows = ds.take_all()

    assert len(rows) == 3
    assert all(row["path"].endswith("data.orc") for row in rows)


def test_read_orc_filter_and_projection(tmp_path):
    _write(
        tmp_path / "data.orc",
        pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [10, 20, 30]}),
    )

    ds = (
        ray.data.read_orc(str(tmp_path)).filter(expr=col("a") > 1).select_columns(["b"])
    )

    assert sorted(ds.take_all(), key=lambda row: row["b"]) == [
        {"b": "y"},
        {"b": "z"},
    ]


def test_read_orc_empty_projection_preserves_rows(tmp_path):
    _write(tmp_path / "data.orc", pa.table({"a": [1, 2, 3]}))

    ds = ray.data.read_orc(str(tmp_path)).select_columns([])

    assert ds.count() == 3
    assert ds.schema().names == []


def test_read_orc_override_num_blocks_drives_partitioner(tmp_path):
    _write(tmp_path / "data.orc", pa.table({"a": [1, 2, 3]}))

    ds = ray.data.read_orc(str(tmp_path), override_num_blocks=7)

    list_files_op = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files_op, ListFiles)
    assert isinstance(list_files_op.file_partitioner, RoundRobinPartitioner)
    assert list_files_op.file_partitioner.num_buckets == 7


def test_read_orc_empty_dir_raises(tmp_path):
    with pytest.raises(ValueError, match="no files found"):
        ray.data.read_orc(str(tmp_path))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
