import io
import tarfile
import os

import pytest
import shutil
import tempfile

from ray.exceptions import RayTaskError

from ray.tune.utils.file_transfer import (
    _sync_dir_between_different_nodes,
    delete_on_node,
    _sync_dir_on_same_node,
)
import ray.util


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def temp_data_dirs():
    tmp_source = os.path.realpath(tempfile.mkdtemp())
    tmp_target = os.path.realpath(tempfile.mkdtemp())

    os.makedirs(os.path.join(tmp_source, "subdir", "nested"))
    os.makedirs(os.path.join(tmp_source, "subdir_exclude", "something"))

    files = [
        "level0.txt",
        "level0_exclude.txt",
        "subdir/level1.txt",
        "subdir/level1_exclude.txt",
        "subdir/nested/level2.txt",
        "subdir_nested_level2_exclude.txt",
        "subdir_exclude/something/somewhere.txt",
    ]

    for file in files:
        with open(os.path.join(tmp_source, file), "w") as f:
            f.write("Data")

    yield tmp_source, tmp_target

    shutil.rmtree(tmp_source)
    shutil.rmtree(tmp_target)


def assert_file(exists: bool, root: str, path: str):
    full_path = os.path.join(root, path)

    if exists:
        assert os.path.exists(full_path)
    else:
        assert not os.path.exists(full_path)


def test_sync_nodes(ray_start_2_cpus, temp_data_dirs):
    """Check that syncing between nodes works (data is found in target directory)"""
    tmp_source, tmp_target = temp_data_dirs

    assert_file(True, tmp_source, "level0.txt")
    assert_file(True, tmp_source, "subdir/level1.txt")
    assert_file(False, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "subdir/level1.txt")

    node_ip = ray.util.get_node_ip_address()
    _sync_dir_between_different_nodes(
        source_ip=node_ip,
        source_path=tmp_source,
        target_ip=node_ip,
        target_path=tmp_target,
    )

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")


def test_sync_nodes_only_diff(ray_start_2_cpus, temp_data_dirs):
    """Check that only differing files are synced between nodes"""
    tmp_source, tmp_target = temp_data_dirs

    # Sanity check
    assert_file(True, tmp_source, "level0.txt")
    assert_file(True, tmp_source, "subdir/level1.txt")
    assert_file(False, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_new.txt")

    node_ip = ray.util.get_node_ip_address()
    _sync_dir_between_different_nodes(
        source_ip=node_ip,
        source_path=tmp_source,
        target_ip=node_ip,
        target_path=tmp_target,
    )

    assert_file(True, tmp_source, "level0.txt")
    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "level0_new.txt")

    # Add new file
    with open(os.path.join(tmp_source, "level0_new.txt"), "w") as f:
        f.write("Data\n")

    # Modify existing file
    with open(os.path.join(tmp_source, "subdir", "level1.txt"), "w") as f:
        f.write("New data\n")

    unpack, pack_actor, files_stats = _sync_dir_between_different_nodes(
        source_ip=node_ip,
        source_path=tmp_source,
        target_ip=node_ip,
        target_path=tmp_target,
        return_futures=True,
    )

    files_stats = ray.get(files_stats)
    tarball = ray.get(pack_actor.get_full_data.remote())

    assert "./level0.txt" in files_stats
    assert "./level0_new.txt" not in files_stats  # Was not in target dir
    assert "subdir/level1.txt" in files_stats

    with tarfile.open(fileobj=io.BytesIO(tarball)) as tar:
        files_in_tar = tar.getnames()
        assert "./level0.txt" not in files_in_tar
        assert "./level0_new.txt" in files_in_tar
        assert "subdir/level1.txt" in files_in_tar
        assert len(files_in_tar) == 7  # 3 files, 4 dirs (including root)

    ray.get(unpack)  # Wait until finished for teardown


def test_delete_on_node(ray_start_2_cpus, temp_data_dirs):
    """Check that delete on node works."""
    tmp_source, tmp_target = temp_data_dirs

    assert_file(True, tmp_source, "level0.txt")
    assert_file(True, tmp_source, "subdir/level1.txt")

    node_ip = ray.util.get_node_ip_address()
    delete_on_node(
        node_ip=node_ip,
        path=tmp_source,
    )

    assert_file(False, tmp_source, "level0.txt")
    assert_file(False, tmp_source, "subdir/level1.txt")

    # Re-create dir for teardown
    os.makedirs(tmp_source, exist_ok=True)


@pytest.mark.parametrize("num_workers", [1, 8])
def test_multi_sync_same_node(ray_start_2_cpus, temp_data_dirs, num_workers):
    """Check that multiple competing syncs to the same node+dir don't interfere"""
    tmp_source, tmp_target = temp_data_dirs

    assert_file(True, tmp_source, "level0.txt")
    assert_file(True, tmp_source, "subdir/level1.txt")

    node_ip = ray.util.get_node_ip_address()
    futures = [
        _sync_dir_on_same_node(
            ip=node_ip,
            source_path=tmp_source,
            target_path=tmp_target,
            return_futures=True,
        )
        for _ in range(num_workers)
    ]
    ray.get(futures)

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")


@pytest.mark.parametrize("num_workers", [1, 8])
def test_multi_sync_different_node(ray_start_2_cpus, temp_data_dirs, num_workers):
    """Check that multiple competing syncs to the same dir don't interfere"""
    tmp_source, tmp_target = temp_data_dirs

    assert_file(True, tmp_source, "level0.txt")
    assert_file(True, tmp_source, "subdir/level1.txt")

    node_ip = ray.util.get_node_ip_address()
    futures = [
        _sync_dir_between_different_nodes(
            source_ip=node_ip,
            source_path=tmp_source,
            target_ip=node_ip,
            target_path=tmp_target,
            return_futures=True,
        )[0]
        for _ in range(num_workers)
    ]
    ray.get(futures)

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")


def test_max_size_exceeded(ray_start_2_cpus, temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    node_ip = ray.util.get_node_ip_address()
    with pytest.raises(RayTaskError):
        _sync_dir_between_different_nodes(
            source_ip=node_ip,
            source_path=tmp_source,
            target_ip=node_ip,
            target_path=tmp_target,
            max_size_bytes=2,
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
