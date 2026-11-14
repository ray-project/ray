import io
import os
import shutil
import tarfile
import tempfile

import pytest

import ray.util
from ray.exceptions import RayTaskError
from ray.tune.utils.file_transfer import (
    _copy_dir,
    _pack_dir,
    _sync_dir_between_different_nodes,
    _sync_dir_on_same_node,
)


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


@pytest.mark.parametrize("exclude", [["subdir/*"], ["*/level1.txt"]])
def test_sync_nodes_exclude_different_node(ray_start_2_cpus, temp_data_dirs, exclude):
    """Check that excluding files works"""
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
        exclude=exclude,
    )

    assert_file(True, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "subdir/level1.txt")


@pytest.mark.parametrize("exclude", [["subdir/*"], ["*/level1.txt"]])
def test_sync_nodes_exclude_same_node(ray_start_2_cpus, temp_data_dirs, exclude):
    """Check that excluding files works"""
    tmp_source, tmp_target = temp_data_dirs

    assert_file(True, tmp_source, "level0.txt")
    assert_file(True, tmp_source, "subdir/level1.txt")
    assert_file(False, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "subdir/level1.txt")

    node_ip = ray.util.get_node_ip_address()
    _sync_dir_on_same_node(
        ip=node_ip, source_path=tmp_source, target_path=tmp_target, exclude=exclude
    )

    assert_file(True, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "subdir/level1.txt")


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


def test_pack_dir_excludes_directories_and_handles_missing_files(
    temp_data_dirs, monkeypatch
):
    tmp_source, _ = temp_data_dirs
    lstat_paths = []
    orig_lstat = os.lstat

    def tracking_lstat(path, *args, **kwargs):
        lstat_paths.append(path)
        return orig_lstat(path, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", tracking_lstat)
    stream = _pack_dir(
        tmp_source,
        exclude=["subdir_exclude", "*_exclude.txt"],
    )
    stream.seek(0)
    with tarfile.open(fileobj=stream) as tar:
        names = tar.getnames()
    assert not any("subdir_exclude" in name for name in names)
    assert not any(name.endswith("_exclude.txt") for name in names)
    assert not any("subdir_exclude" in p for p in lstat_paths)

    def disappearing_lstat(path, *args, **kwargs):
        if path.endswith("level1.txt"):
            raise FileNotFoundError(path)
        return orig_lstat(path, *args, **kwargs)

    monkeypatch.setattr(os, "lstat", disappearing_lstat)
    stream_missing = _pack_dir(tmp_source, files_stats={"./level0.txt": (0.0, 0)})
    stream_missing.seek(0)
    with tarfile.open(fileobj=stream_missing) as tar:
        names_missing = tar.getnames()
    assert "./level0.txt" in names_missing
    assert "subdir/level1.txt" not in names_missing


def test_copy_dir_retry_preserves_exclude(temp_data_dirs, monkeypatch):
    tmp_source, tmp_target = temp_data_dirs
    shutil.rmtree(tmp_target)

    import ray.tune.utils.file_transfer as ft

    calls = []
    orig_lock = ft.TempFileLock

    class FirstAttemptTimeoutLock:
        def __init__(self, path, **kwargs):
            self._timeout = kwargs.get("timeout")
            calls.append(self._timeout)
            self._lock = orig_lock(path, **kwargs)

        def __enter__(self):
            if self._timeout == 0 and len(calls) == 1:
                raise TimeoutError("Simulated lock contention")
            return self._lock.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            return self._lock.__exit__(exc_type, exc_val, exc_tb)

    monkeypatch.setattr(ft, "TempFileLock", FirstAttemptTimeoutLock)
    _copy_dir(
        tmp_source,
        tmp_target,
        exclude=["subdir_exclude", "level0_exclude.txt", "subdir/*"],
    )

    assert_file(True, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_exclude.txt")
    assert_file(False, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "subdir_exclude")
    assert_file(False, tmp_target, "subdir_exclude/something/somewhere.txt")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
