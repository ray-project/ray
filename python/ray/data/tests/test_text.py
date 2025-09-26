import os

import pytest

import ray
from ray.data._internal.execution.interfaces.ref_bundle import (
    _ref_bundles_iterator_to_block_refs_list,
)
from ray.data.datasource import (
    BaseFileMetadataProvider,
    FastFileMetadataProvider,
)
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.mock_http_server import *  # noqa
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


def test_read_text_meta_provider(
    ray_start_regular_shared,
    tmp_path,
):
    path = os.path.join(tmp_path, "test_text")
    os.mkdir(path)
    path = os.path.join(path, "file1.txt")
    with open(path, "w") as f:
        f.write("hello\n")
        f.write("world\n")
        f.write("goodbye\n")
        f.write("ray\n")
    ds = ray.data.read_text(path, meta_provider=FastFileMetadataProvider())
    assert sorted(_to_lines(ds.take())) == ["goodbye", "hello", "ray", "world"]
    ds = ray.data.read_text(path, drop_empty_lines=False)
    assert ds.count() == 4

    with pytest.raises(NotImplementedError):
        ray.data.read_text(
            path,
            meta_provider=BaseFileMetadataProvider(),
        )


def test_read_text_remote_args(ray_start_cluster, tmp_path):
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
