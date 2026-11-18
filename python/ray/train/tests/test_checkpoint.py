import glob
import os
import shutil
import threading
from pathlib import Path
from typing import Dict

import pyarrow.fs
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.train import _checkpoint
from ray.train._checkpoint import (
    _CHECKPOINT_TEMP_DIR_PREFIX,
    _DELETING_SUFFIX,
    _METADATA_FILE_NAME,
    _STAGING_SUFFIX,
    Checkpoint,
    _get_reader_path,
    _list_readers,
)
from ray.train._internal.storage import _exists_at_fs_path, _upload_to_fs_path
from ray.train.tests.test_new_persistence import _create_mock_custom_fs

_CHECKPOINT_CONTENT_FILE = "dummy.txt"


@pytest.fixture(params=["local", "mock", "custom_fs"])
def checkpoint(request, tmp_path):
    """Fixture that sets up a checkpoint on different filesystems."""
    checkpoint_fs_type = request.param

    checkpoint_path = tmp_path / "ckpt_dir"
    checkpoint_path.mkdir(exist_ok=True)
    (checkpoint_path / _CHECKPOINT_CONTENT_FILE).write_text("dummy")

    if checkpoint_fs_type == "local":
        yield Checkpoint.from_directory(str(checkpoint_path))
    elif checkpoint_fs_type == "mock":
        _checkpoint = Checkpoint(path="mock:///mock_bucket/ckpt_dir")
        _upload_to_fs_path(
            local_path=str(checkpoint_path),
            fs=_checkpoint.filesystem,
            fs_path=_checkpoint.path,
        )
        # The "mock://" URI doesn't persist across different instances of
        # the pyarrow.fs.MockFileSystem, so we must make sure to return
        # the checkpoint with the same filesystem instance that we uploaded
        # some mock content to.
        yield _checkpoint
    elif checkpoint_fs_type == "custom_fs":
        custom_storage_fs = _create_mock_custom_fs(tmp_path / "custom_fs")
        _upload_to_fs_path(
            local_path=str(checkpoint_path),
            fs=custom_storage_fs,
            fs_path="mock_bucket/ckpt_dir",
        )
        yield Checkpoint(path="mock_bucket/ckpt_dir", filesystem=custom_storage_fs)


def test_to_directory(checkpoint: Checkpoint):
    checkpoint_path = Path(checkpoint.to_directory())

    assert (checkpoint_path / _CHECKPOINT_CONTENT_FILE).exists()
    assert _CHECKPOINT_TEMP_DIR_PREFIX in checkpoint_path.name


def test_to_directory_with_user_specified_path(checkpoint: Checkpoint, tmp_path):
    # Test with a string
    checkpoint_path = Path(checkpoint.to_directory(str(tmp_path / "special_dir")))
    assert (checkpoint_path / _CHECKPOINT_CONTENT_FILE).exists()
    assert checkpoint_path.name == "special_dir"

    # Test with a PathLike
    checkpoint_path = Path(checkpoint.to_directory(tmp_path / "special_dir"))
    assert (checkpoint_path / _CHECKPOINT_CONTENT_FILE).exists()
    assert checkpoint_path.name == "special_dir"


def test_multiprocess_to_directory(checkpoint: Checkpoint):
    """Test the case where multiple processes are trying to checkpoint.

    Only one process should download the checkpoint, and the others should
    wait until it's finished. In the end, a single checkpoint dir should be
    shared by all processes.
    """
    if checkpoint.filesystem.type_name == "mock":
        pytest.skip("Mock filesystem cannot be pickled for use with Ray.")

    @ray.remote
    def download_checkpoint(checkpoint: Checkpoint) -> str:
        return checkpoint.to_directory()

    paths = [ray.get(download_checkpoint.remote(checkpoint)) for _ in range(5)]
    # Check that all the paths are the same (no duplicates).
    assert len(set(paths)) == 1


def test_as_directory(checkpoint: Checkpoint):
    with checkpoint.as_directory() as checkpoint_path:
        checkpoint_path = Path(checkpoint_path)
        assert (checkpoint_path / _CHECKPOINT_CONTENT_FILE).exists()

        if isinstance(checkpoint.filesystem, pyarrow.fs.LocalFileSystem):
            # We should have directly returned the local path
            assert str(checkpoint_path) == checkpoint.path
        else:
            # We should have downloaded to a temp dir.
            assert _CHECKPOINT_TEMP_DIR_PREFIX in checkpoint_path.name

    if isinstance(checkpoint.filesystem, pyarrow.fs.LocalFileSystem):
        # Checkpoint should not be deleted, if we directly gave the local path.
        assert (checkpoint_path / _CHECKPOINT_CONTENT_FILE).exists()
    else:
        # Should have been deleted, if the checkpoint downloaded to a temp dir.
        assert not checkpoint_path.exists()


def test_multiprocess_as_directory(checkpoint: Checkpoint, monkeypatch):
    """Tests that reader files are created and deleted correctly,
    when multiple processes are accessing a checkpoint with `as_directory`"""
    # If this is pointing to a local path, no lockfiles will be created.
    is_local_checkpoint = isinstance(checkpoint.filesystem, pyarrow.fs.LocalFileSystem)

    monkeypatch.setattr(os, "getpid", lambda: 11111)

    def readers_for_pid(checkpoint_dir: str, pid: int):
        return glob.glob(_get_reader_path(checkpoint_dir, reader_id=f"{pid}_*"))

    with checkpoint.as_directory() as checkpoint_dir_1:
        # Pretend that the 2nd `as_directory` is called from a different process.
        monkeypatch.setattr(os, "getpid", lambda: 22222)

        with checkpoint.as_directory() as checkpoint_dir_2:
            # Should point both processes to the same canonical temp directory.
            assert checkpoint_dir_1 == checkpoint_dir_2

            if is_local_checkpoint:
                # No reader files are created for local checkpoints.
                assert not _list_readers(checkpoint_dir_1)
            else:
                # Check that 2 different reader files were created.
                assert len(_list_readers(checkpoint_dir_1)) == 2
                assert len(readers_for_pid(checkpoint_dir_1, 11111)) == 1
                assert len(readers_for_pid(checkpoint_dir_1, 22222)) == 1

        if not is_local_checkpoint:
            # Check that the 2nd reader file was deleted.
            assert len(_list_readers(checkpoint_dir_1)) == 1
            assert not readers_for_pid(checkpoint_dir_1, 22222)

    if not is_local_checkpoint:
        # Check that the 1st reader file was deleted.
        assert len(_list_readers(checkpoint_dir_1)) == 0

        # Check that the temp checkpoint directory was deleted.
        assert not Path(checkpoint_dir_1).exists()


def test_as_directory_lock_cleanup(checkpoint: Checkpoint):
    """Errors when accessing a checkpoint with `as_directory`
    shouldn't leave behind reader files.
    """
    with pytest.raises(RuntimeError):
        with checkpoint.as_directory() as checkpoint_dir:
            raise RuntimeError

    assert not _list_readers(checkpoint_dir)

    is_local_checkpoint = isinstance(checkpoint.filesystem, pyarrow.fs.LocalFileSystem)
    if not is_local_checkpoint:
        assert not Path(checkpoint_dir).exists()


def test_as_directory_download_error(checkpoint: Checkpoint, monkeypatch):
    """Errors during a checkpoint download should be raised directly when accessing
    it with the `as_directory` context manager."""
    if isinstance(checkpoint.filesystem, pyarrow.fs.LocalFileSystem):
        pytest.skip(
            "Local filesystem checkpoints don't download to a temp dir, so "
            "there's no error handling to test."
        )

    error_text = "original error"

    def download_error(*args, **kwargs):
        raise RuntimeError(error_text)

    monkeypatch.setattr(_checkpoint, "_download_from_fs_path", download_error)

    with pytest.raises(RuntimeError, match=error_text):
        with checkpoint.as_directory() as _:
            pass

    # The failed reader unregistered itself.
    assert not _list_readers(checkpoint._get_temp_dirname())


def _skip_if_not_downloaded(checkpoint: Checkpoint):
    if isinstance(checkpoint.filesystem, pyarrow.fs.LocalFileSystem):
        pytest.skip("Local checkpoints are not downloaded to a temp dir.")


def _read_content(checkpoint_dir: str) -> str:
    return (Path(checkpoint_dir) / _CHECKPOINT_CONTENT_FILE).read_text()


def test_get_reader_path_is_unique_per_call():
    """Each reader gets its own reader file, even within one process."""
    path = "/tmp/checkpoint_tmp"
    assert _get_reader_path(path) != _get_reader_path(path)
    assert _get_reader_path(path).startswith(f"{path}.reader_{os.getpid()}_")


def test_as_directory_waits_for_reader_in_other_process(
    checkpoint: Checkpoint, tmp_path
):
    """A reader in another process holds the temporary directory until it exits
    `as_directory()`, even after this process's reader exits."""
    _skip_if_not_downloaded(checkpoint)
    if checkpoint.filesystem.type_name == "mock":
        pytest.skip("Mock filesystem cannot be pickled for use with Ray.")

    entered = tmp_path / "entered"
    release = tmp_path / "release"

    @ray.remote
    def hold_directory(checkpoint: Checkpoint) -> str:
        import time

        with checkpoint.as_directory() as checkpoint_dir:
            entered.touch()
            while not release.exists():
                time.sleep(0.05)
        return checkpoint_dir

    ref = hold_directory.remote(checkpoint)
    wait_for_condition(entered.exists, timeout=60)

    with checkpoint.as_directory() as checkpoint_dir:
        pass
    assert _read_content(checkpoint_dir) == "dummy"

    release.touch()
    assert ray.get(ref) == checkpoint_dir
    assert not Path(checkpoint_dir).exists()


def test_as_directory_reader_arrives_during_cleanup(checkpoint: Checkpoint, tmp_path):
    """A reader that arrives while the last reader is deleting the shared
    temporary directory waits for the deletion and downloads the checkpoint
    again, instead of failing because the directory no longer exists."""
    _skip_if_not_downloaded(checkpoint)
    if checkpoint.filesystem.type_name == "mock":
        pytest.skip("Mock filesystem cannot be pickled for use with Ray.")

    deleting_marker = tmp_path / "deleting"

    @ray.remote
    def read_with_slow_cleanup(checkpoint: Checkpoint):
        import os
        import shutil
        import time

        original_rmtree = shutil.rmtree

        def slow_rmtree(path, *args, **kwargs):
            # The `as_directory()` cleanup renames the directory, then deletes it
            # while holding the lock.
            if str(path).endswith(_DELETING_SUFFIX) and os.path.exists(path):
                deleting_marker.touch()
                time.sleep(2)
            original_rmtree(path, *args, **kwargs)

        shutil.rmtree = slow_rmtree
        try:
            with checkpoint.as_directory():
                pass
        finally:
            shutil.rmtree = original_rmtree

    ref = read_with_slow_cleanup.remote(checkpoint)
    wait_for_condition(deleting_marker.exists, timeout=60)

    with checkpoint.as_directory() as checkpoint_dir:
        assert _read_content(checkpoint_dir) == "dummy"
    ray.get(ref)


def test_as_directory_nested_in_one_process(checkpoint: Checkpoint):
    """Exiting a nested `as_directory()` in the same process must not delete the
    directory while the outer block still uses it."""
    _skip_if_not_downloaded(checkpoint)

    with checkpoint.as_directory() as outer_dir:
        with checkpoint.as_directory() as inner_dir:
            assert inner_dir == outer_dir
            assert len(_list_readers(outer_dir)) == 2
        assert _read_content(outer_dir) == "dummy"
        assert len(_list_readers(outer_dir)) == 1

    assert not Path(outer_dir).exists()
    assert not _list_readers(outer_dir)


def test_as_directory_threads_in_one_process(checkpoint: Checkpoint):
    """Threads in one process each register as separate readers, so the first
    thread to exit doesn't delete the directory under the other."""
    _skip_if_not_downloaded(checkpoint)

    first_entered = threading.Event()
    first_exited = threading.Event()
    results = {}

    def first_reader():
        with checkpoint.as_directory():
            first_entered.set()
            assert second_entered.wait(timeout=30)
        first_exited.set()

    second_entered = threading.Event()

    def second_reader():
        assert first_entered.wait(timeout=30)
        with checkpoint.as_directory() as checkpoint_dir:
            second_entered.set()
            assert first_exited.wait(timeout=30)
            results["content"] = _read_content(checkpoint_dir)

    threads = [
        threading.Thread(target=first_reader),
        threading.Thread(target=second_reader),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert results["content"] == "dummy"
    assert not Path(checkpoint._get_temp_dirname()).exists()


def _count_downloads(monkeypatch) -> Dict[str, int]:
    """Count calls to `_download_from_fs_path` in `ray.train._checkpoint`."""
    counter = {"downloads": 0}
    original_download = _checkpoint._download_from_fs_path

    def counting_download(*args, **kwargs):
        counter["downloads"] += 1
        return original_download(*args, **kwargs)

    monkeypatch.setattr(_checkpoint, "_download_from_fs_path", counting_download)
    return counter


def test_as_directory_reuses_complete_download(checkpoint: Checkpoint, monkeypatch):
    """A reader that arrives while the shared temporary directory is complete and
    in use reuses it, instead of downloading again over files that other readers
    may be reading. Once the last reader exits, the next reader downloads again."""
    _skip_if_not_downloaded(checkpoint)
    counter = _count_downloads(monkeypatch)

    with checkpoint.as_directory() as checkpoint_dir:
        with checkpoint.as_directory() as checkpoint_dir_2:
            assert checkpoint_dir_2 == checkpoint_dir
        assert counter["downloads"] == 1

    assert not Path(checkpoint_dir).exists()
    with checkpoint.as_directory() as checkpoint_dir:
        assert _read_content(checkpoint_dir) == "dummy"
    assert counter["downloads"] == 2


def test_to_directory_survives_as_directory_cleanup(
    checkpoint: Checkpoint, monkeypatch
):
    """`to_directory()` and `as_directory()` share one download, and the
    `as_directory()` cleanup never deletes a directory that `to_directory()`
    returned, whichever is called first."""
    _skip_if_not_downloaded(checkpoint)
    counter = _count_downloads(monkeypatch)

    with checkpoint.as_directory() as checkpoint_dir:
        assert checkpoint.to_directory() == checkpoint_dir
    assert _read_content(checkpoint_dir) == "dummy"

    with checkpoint.as_directory() as checkpoint_dir_2:
        assert checkpoint_dir_2 == checkpoint_dir
    assert _read_content(checkpoint_dir) == "dummy"
    assert counter["downloads"] == 1


def test_to_directory_reuses_complete_download(checkpoint: Checkpoint, monkeypatch):
    """A `to_directory()` call that finds the checkpoint already downloaded
    returns it without downloading again."""
    _skip_if_not_downloaded(checkpoint)
    counter = _count_downloads(monkeypatch)

    assert checkpoint.to_directory() == checkpoint.to_directory()
    assert counter["downloads"] == 1


def test_to_directory_failed_download(checkpoint: Checkpoint, monkeypatch):
    """A download that fails partway leaves nothing behind at the shared
    temporary directory, and the next caller downloads again."""
    _skip_if_not_downloaded(checkpoint)

    from ray.train._internal import storage

    original_copy = storage._pyarrow_fs_copy_files

    def failing_copy(fs_path, local_path, **kwargs):
        (Path(local_path) / "partial_file").write_text("partial")
        raise RuntimeError("download failed")

    monkeypatch.setattr(storage, "_pyarrow_fs_copy_files", failing_copy)
    with pytest.raises(RuntimeError, match="download failed"):
        checkpoint.to_directory()

    checkpoint_dir = checkpoint._get_temp_dirname()
    assert not Path(checkpoint_dir).exists()
    assert not Path(checkpoint_dir + _STAGING_SUFFIX).exists()

    monkeypatch.setattr(storage, "_pyarrow_fs_copy_files", original_copy)
    assert checkpoint.to_directory() == checkpoint_dir
    assert _read_content(checkpoint_dir) == "dummy"


def test_to_directory_after_user_deletes_directory(checkpoint: Checkpoint):
    """If the caller deletes the directory that `to_directory()` returned, the
    next call downloads the checkpoint again."""
    _skip_if_not_downloaded(checkpoint)

    checkpoint_dir = checkpoint.to_directory()
    shutil.rmtree(checkpoint_dir)

    assert checkpoint.to_directory() == checkpoint_dir
    assert _read_content(checkpoint_dir) == "dummy"


def test_to_directory_ignores_crashed_download(checkpoint: Checkpoint):
    """A staging directory left by a process that crashed mid-download is
    discarded, and its partial contents never appear in the checkpoint."""
    _skip_if_not_downloaded(checkpoint)

    checkpoint_dir = checkpoint._get_temp_dirname()
    staging_dir = Path(checkpoint_dir + _STAGING_SUFFIX)
    staging_dir.mkdir()
    (staging_dir / "partial_file").write_text("partial")

    with checkpoint.as_directory() as checkpoint_dir:
        assert _read_content(checkpoint_dir) == "dummy"
        assert not (Path(checkpoint_dir) / "partial_file").exists()
    assert not staging_dir.exists()


def test_as_directory_ignores_crashed_deletion(checkpoint: Checkpoint):
    """A directory left by a process that crashed mid-deletion sits under the
    deleting name, never the checkpoint's name, and the next cleanup removes it."""
    _skip_if_not_downloaded(checkpoint)

    checkpoint_dir = checkpoint._get_temp_dirname()
    deleting_dir = Path(checkpoint_dir + _DELETING_SUFFIX)
    deleting_dir.mkdir()
    (deleting_dir / "leftover_file").write_text("leftover")

    with checkpoint.as_directory() as checkpoint_dir:
        assert _read_content(checkpoint_dir) == "dummy"
    assert not Path(checkpoint_dir).exists()
    assert not deleting_dir.exists()


def test_metadata(checkpoint: Checkpoint):
    assert checkpoint.get_metadata() == {}

    # No metadata file by default.
    assert not _exists_at_fs_path(
        fs=checkpoint.filesystem,
        fs_path=str(Path(checkpoint.path) / _METADATA_FILE_NAME),
    )

    checkpoint.update_metadata({"foo": "bar"})
    assert checkpoint.get_metadata() == {"foo": "bar"}

    checkpoint.update_metadata({"foo": "baz"})
    assert checkpoint.get_metadata() == {"foo": "baz"}

    checkpoint.update_metadata({"x": 1})
    assert checkpoint.get_metadata() == {"foo": "baz", "x": 1}

    # Set metadata completely resets the metadata.
    checkpoint.set_metadata({"y": [1, 2, 3]})
    assert checkpoint.get_metadata() == {"y": [1, 2, 3]}

    # There should be a metadata file in the checkpoint directory.
    assert _exists_at_fs_path(
        fs=checkpoint.filesystem,
        fs_path=str(Path(checkpoint.path) / _METADATA_FILE_NAME),
    )

    # Non JSON serializable metadata should raise an error.
    class Test:
        pass

    with pytest.raises(TypeError):
        checkpoint.set_metadata({"non_json_serializable": Test()})


@pytest.mark.parametrize("fs_type", ["local", "custom_fs"])
def test_upload_single_file(fs_type, tmp_path):
    """Uploading a single file should place the file at ``fs_path`` (as a file,
    not under a directory), mirroring ``_download_from_fs_path`` for files."""
    local_file = tmp_path / "model.pt"
    local_file.write_text("weights-data")

    if fs_type == "local":
        fs = pyarrow.fs.LocalFileSystem()
        fs_path = str(tmp_path / "uploaded" / "model.pt")
    else:
        fs = _create_mock_custom_fs(tmp_path / "custom_fs")
        fs_path = "model.pt"

    _upload_to_fs_path(local_path=str(local_file), fs=fs, fs_path=fs_path)

    file_info = fs.get_file_info(fs_path)
    assert (
        file_info.type == pyarrow.fs.FileType.File
    ), f"Expected a file at {fs_path}, got {file_info.type}"

    if fs_type == "local":
        with open(fs_path) as f:
            assert f.read() == "weights-data"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
