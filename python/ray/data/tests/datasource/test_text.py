import os

import pytest
from fsspec.implementations.http import HTTPFileSystem

import ray
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
from ray.tests.conftest import *  # noqa


@pytest.fixture(params=[False, True], ids=["v1", "v2"])
def text_reader_version(request, monkeypatch):
    from ray.data.context import DataContext

    monkeypatch.setattr(DataContext.get_current(), "use_datasource_v2", request.param)
    return request.param


def _to_lines(rows):
    return [row["text"] for row in rows]


def test_empty_text_files(ray_start_regular_shared, tmp_path, text_reader_version):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    # 2 empty files.
    _ = open(os.path.join(path, "file1.txt"), "w")
    _ = open(os.path.join(path, "file2.txt"), "w")
    ds = ray.data.read_text(path)
    assert ds.count() == 0
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 0


def test_read_text(ray_start_regular_shared, tmp_path, text_reader_version):
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
    assert ds.schema().names == ["text"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 4


def test_read_text_partitioned_with_paths_and_pushdowns(
    ray_start_regular_shared, tmp_path, text_reader_version
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle
    from ray.data.expressions import col

    for year, content in (("2023", "one\n\n"), ("2024", "two\nthree\n")):
        os.mkdir(os.path.join(tmp_path, f"year={year}"))
        with open(os.path.join(tmp_path, f"year={year}", "data.txt"), "w") as f:
            f.write(content)
    partitioning = Partitioning(
        PartitionStyle.HIVE, base_dir=str(tmp_path), field_types={"year": int}
    )

    ds = ray.data.read_text(
        str(tmp_path), partitioning=partitioning, include_paths=True
    )
    assert ds.schema().names == ["text", "year", "path"]
    rows = sorted(ds.take_all(), key=lambda row: row["text"])
    assert [(row["text"], row["year"]) for row in rows] == [
        ("one", 2023),
        ("three", 2024),
        ("two", 2024),
    ]
    assert all(row["path"].endswith(f"year={row['year']}/data.txt") for row in rows)

    pruned = ds.filter(expr=col("year") == 2024)
    assert sorted(_to_lines(pruned.take_all())) == ["three", "two"]
    assert sorted(_to_lines(ds.filter(expr=col("text") != "two").take_all())) == [
        "one",
        "three",
    ]
    assert ds.select_columns(["year"]).count() == 3
    assert ds.limit(1).count() == 1
    assert ds.count() == 3


def test_read_text_root_files_next_to_partitions(
    ray_start_regular_shared, tmp_path, text_reader_version
):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle
    from ray.data.expressions import col

    if not text_reader_version:
        pytest.skip(
            "V1 blocks from files outside partition directories lack the column"
        )

    with open(os.path.join(tmp_path, "root.txt"), "w") as f:
        f.write("root\n")
    os.mkdir(os.path.join(tmp_path, "year=2024"))
    with open(os.path.join(tmp_path, "year=2024", "data.txt"), "w") as f:
        f.write("nested\n")
    partitioning = Partitioning(PartitionStyle.HIVE, base_dir=str(tmp_path))

    ds = ray.data.read_text(
        str(tmp_path), partitioning=partitioning, include_paths=True
    )

    rows = sorted(ds.take_all(), key=lambda row: row["text"])
    assert [(row["text"], row["year"]) for row in rows] == [
        ("nested", "2024"),
        ("root", None),
    ]
    assert _to_lines(ds.filter(expr=col("year").is_null()).take_all()) == ["root"]
    assert _to_lines(ds.filter(expr=col("year") != "2024").take_all()) == []
    assert _to_lines(ds.filter(expr=col("year") == "2024").take_all()) == ["nested"]
    root_path = os.path.join(tmp_path, "root.txt")
    assert _to_lines(ds.filter(expr=col("path") == root_path).take_all()) == ["root"]


def test_read_text_preserve_order_keeps_files_in_order(
    ray_start_regular_shared, tmp_path, text_reader_version, monkeypatch
):
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    monkeypatch.setattr(ctx.execution_options, "preserve_order", True)
    # Two read tasks for five files: round-robin would deal files 0, 2, 4 to
    # the first task and 1, 3 to the second.
    monkeypatch.setattr(ctx, "read_op_min_num_blocks", 2)
    monkeypatch.setattr(ctx, "target_min_block_size", 1)
    paths = []
    for i in range(5):
        path = os.path.join(tmp_path, f"{i}.txt")
        with open(path, "w") as f:
            f.write(f"{i}a\n{i}b\n")
        paths.append(path)

    ds = ray.data.read_text(paths)

    assert _to_lines(ds.take_all()) == [f"{i}{s}" for i in range(5) for s in "ab"]


def test_read_text_compressed(ray_start_regular_shared, tmp_path, text_reader_version):
    import gzip

    with gzip.open(os.path.join(tmp_path, "data.txt.gz"), "wt") as f:
        f.write("hello\nworld\n")

    ds = ray.data.read_text(str(tmp_path))

    assert sorted(_to_lines(ds.take_all())) == ["hello", "world"]


def test_read_text_remote_args(ray_start_cluster, tmp_path, text_reader_version):
    cluster = ray_start_cluster
    cluster.add_node(
        resources={"foo": 100},
        num_cpus=1,
        _system_config={"max_direct_call_object_size": 0},
    )
    cluster.add_node(resources={"bar": 100}, num_cpus=1)

    ray.shutdown()
    ray.init(cluster.address)

    @ray.remote
    def get_node_id():
        return ray.get_runtime_context().get_node_id()

    bar_node_id = ray.get(get_node_id.options(resources={"bar": 1}).remote())

    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    with open(os.path.join(path, "file1.txt"), "w") as f:
        f.write("hello\n")
        f.write("world")
    with open(os.path.join(path, "file2.txt"), "w") as f:
        f.write("goodbye")

    ds = ray.data.read_text(
        path, override_num_blocks=2, ray_remote_args={"resources": {"bar": 1}}
    )

    block_refs = _ref_bundles_iterator_to_block_refs_list(
        ds.iter_internal_ref_bundles()
    )
    ray.wait(block_refs, num_returns=len(block_refs), fetch_local=False)
    location_data = ray.experimental.get_object_locations(block_refs)
    locations = []
    for block in block_refs:
        locations.extend(location_data[block]["node_ids"])
    assert set(locations) == {bar_node_id}, locations
    assert sorted(_to_lines(ds.take())) == ["goodbye", "hello", "world"]


def test_fsspec_http_file_system(ray_start_regular_shared, http_server, http_file):
    # HTTP reads keep the V1 reader whatever ``use_datasource_v2`` says.
    ds = ray.data.read_text(http_file, filesystem=HTTPFileSystem())
    assert ds.count() > 0
    # Test auto-resolve of HTTP file system when it is not provided.
    ds = ray.data.read_text(http_file)
    assert ds.count() > 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
