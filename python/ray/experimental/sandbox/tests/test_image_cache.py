import fcntl
import io
import json
import os
import subprocess
import sys
import tarfile
from unittest.mock import MagicMock, patch

import pytest

from ray.experimental.sandbox._internal import image_utils
from ray.experimental.sandbox.backend.gvisor import GVisorSandboxBackend
from ray.experimental.sandbox.config import SandboxConfig
from ray.experimental.sandbox.image_manager import ImageManager


def _cached_image(images_dir, name, size=100, mtime=1):
    image_dir = images_dir / name
    (image_dir / "rootfs").mkdir(parents=True)
    (image_dir / "rootfs" / "data").write_bytes(b"x" * size)
    marker = image_dir / ".extracted"
    marker.write_text(str(size))
    os.utime(marker, (mtime, mtime))
    return image_dir


def _local_tar(path, data=b"hello"):
    with tarfile.open(path, "w") as archive:
        member = tarfile.TarInfo("hello.txt")
        member.size = len(data)
        archive.addfile(member, io.BytesIO(data))


def _assert_lock_held(path, shared=False):
    with open(path, "a") as lock:
        if shared:
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        operation = fcntl.LOCK_EX if shared else fcntl.LOCK_SH
        with pytest.raises(BlockingIOError):
            fcntl.flock(lock, operation | fcntl.LOCK_NB)


@pytest.fixture
def runsc_list(monkeypatch):
    run = MagicMock(return_value=subprocess.CompletedProcess([], 0, "null"))
    monkeypatch.setattr(image_utils.subprocess, "run", run)
    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "1")
    return run


@pytest.mark.parametrize("relative_root", [False, True])
@pytest.mark.parametrize("status", ["created", "running", "stopped"])
def test_eviction_uses_runsc_bundles(
    tmp_path, monkeypatch, runsc_list, relative_root, status
):
    images_dir = tmp_path / "images"
    old = _cached_image(images_dir, "old")
    busy = _cached_image(images_dir, "busy", mtime=2)
    kept = _cached_image(images_dir, "kept", mtime=3)
    newer = _cached_image(images_dir, "newer", mtime=4)
    newest = _cached_image(images_dir, "newest", mtime=5)
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    root = str(busy / "rootfs")
    if relative_root:
        (bundle / "rootfs").symlink_to(root, target_is_directory=True)
        root = "rootfs"
    (bundle / "config.json").write_text(json.dumps({"root": {"path": root}}))
    runsc_list.return_value.stdout = json.dumps(
        [{"id": "sandbox", "bundle": str(bundle), "status": status}]
    )

    def list_containers(*args, **kwargs):
        # The entire candidate set must be locked before reading gVisor
        # state, including candidates that turn out to be in use.
        for name in ("old", "busy", "newer", "newest"):
            _assert_lock_held(images_dir / f"{name}.startup.lock")
        return runsc_list.return_value

    runsc_list.side_effect = list_containers
    rmtree = image_utils.shutil.rmtree

    def remove_image(path):
        _assert_lock_held(f"{path}.startup.lock")
        rmtree(path)

    monkeypatch.setattr(image_utils.shutil, "rmtree", remove_image)
    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "300")
    with patch.object(
        image_utils.os, "walk", side_effect=AssertionError("size rescan")
    ):
        with image_utils.image_cache_context(str(images_dir), "kept"):
            pass
    assert not old.exists()
    assert busy.exists()
    assert kept.exists()
    assert not newer.exists()
    assert newest.exists()
    runsc_list.assert_called_once_with(
        ["runsc", "--root", image_utils.RUNSC_ROOT, "list", "--format=json"],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )


@pytest.mark.parametrize(
    "response",
    [
        FileNotFoundError("runsc"),
        subprocess.CalledProcessError(1, "runsc"),
        subprocess.TimeoutExpired("runsc", 10),
        "invalid json",
        "{}",
        '[{"bundle": "/missing/bundle"}]',
        '[{"status": "running"}]',
    ],
)
def test_eviction_preserves_images_when_state_is_unavailable(
    tmp_path, runsc_list, response
):
    image = _cached_image(tmp_path, "image")
    if isinstance(response, Exception):
        runsc_list.side_effect = response
    else:
        runsc_list.return_value.stdout = response
    with image_utils.image_cache_context(str(tmp_path), "other"):
        pass
    assert image.exists()
    # A failed pass must release both the eviction and candidate locks.
    for name in (".cache.lock", "image.startup.lock"):
        with open(tmp_path / name, "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)


@pytest.mark.parametrize("raw", [None, "", "invalid", "0", "-1", "200"])
def test_cache_limit(tmp_path, monkeypatch, runsc_list, raw):
    old = _cached_image(tmp_path, "old")
    new = _cached_image(tmp_path, "new", mtime=2)
    if raw is None:
        monkeypatch.delenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES")
    else:
        monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", raw)
    monkeypatch.setattr(
        image_utils.shutil, "disk_usage", lambda _: MagicMock(total=200)
    )
    with image_utils.image_cache_context(str(tmp_path), "new"):
        pass
    assert new.exists()
    assert old.exists() == (raw in ("0", "-1", "200"))
    if old.exists():
        runsc_list.assert_not_called()


def test_cache_hit_reuses_size_and_refreshes_recency(tmp_path, runsc_list):
    archive = tmp_path / "sample.tar"
    _local_tar(archive)
    os.utime(archive, (1, 1))
    images_dir = tmp_path / "images"
    manager = ImageManager(str(images_dir))
    with patch.object(
        image_utils, "_dir_size_bytes", wraps=image_utils._dir_size_bytes
    ) as measure:
        image_dir = manager.pull_image(str(archive))
        marker = images_dir / "sample" / ".extracted"
        assert marker.read_text() == "5"
        os.utime(marker, (2, 2))
        with patch.object(
            image_utils.os, "walk", side_effect=AssertionError("size rescan")
        ):
            assert ImageManager(str(images_dir)).pull_image(str(archive)) == image_dir
        assert marker.stat().st_mtime > 2
        assert marker.read_text() == "5"
        assert measure.call_count == 1

        # Updating the local source archive still invalidates the cache.
        _local_tar(archive, b"updated image")
        os.utime(archive, (marker.stat().st_mtime + 1,) * 2)
        manager.pull_image(str(archive))
        assert marker.read_text() == "13"
        assert measure.call_count == 2


@pytest.mark.parametrize("fail_startup", [False, True])
def test_startup_lock_protects_image_and_releases_on_exit(
    tmp_path, monkeypatch, runsc_list, fail_startup
):
    archive = tmp_path / "sample.tar"
    _local_tar(archive)
    images_dir = tmp_path / "images"
    manager = ImageManager(str(images_dir))
    backend = GVisorSandboxBackend(manager)
    monkeypatch.setattr(
        "ray.experimental.sandbox.backend.gvisor.shutil.which", lambda _: "runsc"
    )
    extract = image_utils.extract_tar_layer

    def extract_image(*args):
        _assert_lock_held(images_dir / "sample.startup.lock", shared=True)
        extract(*args)

    monkeypatch.setattr(image_utils, "extract_tar_layer", extract_image)

    def start(config):
        manager.pull_image(config.image)
        _assert_lock_held(images_dir / "sample.startup.lock", shared=True)
        unused = _cached_image(images_dir, "unused")
        # A competing pull can evict other images while this one starts.
        with image_utils.image_cache_context(str(images_dir), "competing"):
            assert manager.is_image_extracted(config.image)
            assert not unused.exists()
        runsc_list.assert_called_once()
        if fail_startup:
            raise RuntimeError("startup failed")
        return "sandbox"

    monkeypatch.setattr(backend, "_create_sandbox", start)
    config = SandboxConfig(image=str(archive))
    if fail_startup:
        with pytest.raises(RuntimeError, match="startup failed"):
            backend.create_sandbox(config)
    else:
        assert backend.create_sandbox(config) == "sandbox"

    # Once the context exits, an unreferenced image can be reclaimed.
    runsc_list.reset_mock()
    with image_utils.image_cache_context(str(images_dir), "other"):
        pass
    assert not manager.is_image_extracted(str(archive))
    runsc_list.assert_called_once()


def test_eviction_serializes_passes_without_blocking_other_startups(
    tmp_path, runsc_list
):
    image = _cached_image(tmp_path, "image")

    def list_containers(*args, **kwargs):
        _assert_lock_held(tmp_path / ".cache.lock")
        _assert_lock_held(tmp_path / "image.startup.lock")
        # Another image can start while this pass holds the cache lock.
        # Its attempt at eviction must skip the already-running pass.
        with image_utils.image_cache_context(str(tmp_path), "new"):
            _assert_lock_held(tmp_path / "new.startup.lock", shared=True)
            with image_utils.image_cache_context(str(tmp_path), "new"):
                assert image.exists()
        return runsc_list.return_value

    runsc_list.side_effect = list_containers
    with image_utils.image_cache_context(str(tmp_path), "other"):
        pass
    assert not image.exists()
    runsc_list.assert_called_once()


def test_eviction_during_image_publication(tmp_path, monkeypatch, runsc_list):
    archive = tmp_path / "sample.tar"
    _local_tar(archive)
    images_dir = tmp_path / "images"
    replace = os.replace

    def publish_image(source, destination):
        unused = _cached_image(images_dir, "unused")
        with image_utils.image_cache_context(str(images_dir), "competing"):
            assert not unused.exists()
            assert os.path.isdir(source)
        replace(source, destination)

    monkeypatch.setattr(os, "replace", publish_image)
    manager = ImageManager(str(images_dir))
    manager.pull_image(str(archive))
    assert manager.is_image_extracted(str(archive))
    assert (images_dir / "sample" / ".extracted").read_text() == "5"
    runsc_list.assert_called_once()


@pytest.mark.parametrize("updated_size", [50, 100])
def test_eviction_refreshes_metadata_after_locking(
    tmp_path, monkeypatch, runsc_list, updated_size
):
    old = _cached_image(tmp_path, "old")
    new = _cached_image(tmp_path, "new", mtime=2)
    monkeypatch.setenv("RAY_SANDBOX_IMAGE_CACHE_MAX_BYTES", "150")
    flock = fcntl.flock

    def lock_image(lock, operation):
        if lock.name == str(tmp_path / "old.startup.lock"):
            # A pull completed between the scan and taking the startup lock.
            marker = old / ".extracted"
            marker.write_text(str(updated_size))
            os.utime(marker, (3, 3))
        flock(lock, operation)

    monkeypatch.setattr(fcntl, "flock", lock_image)
    with image_utils.image_cache_context(str(tmp_path), "other"):
        pass
    assert old.exists()
    assert new.exists() == (updated_size == 50)
    if updated_size == 50:
        runsc_list.assert_not_called()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
