import os
from pathlib import Path

import pyarrow.fs
import pytest

import ray
from ray.train._internal.storage import _exists_at_fs_path, _upload_to_fs_path
from ray.train.checkpoint import (
    _CHECKPOINT_TEMP_DIR_PREFIX,
    _METADATA_FILE_NAME,
    _get_del_lock_path,
    _list_existing_del_locks,
    Checkpoint,
)

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
    """Tests that deletion lock files are created and deleted correctly,
    when multiple processes are accessing a checkpoint with `as_directory`"""
    # If this is pointing to a local path, no lockfiles will be created.
    is_local_checkpoint = isinstance(checkpoint.filesystem, pyarrow.fs.LocalFileSystem)

    monkeypatch.setattr(os, "getpid", lambda: 11111)

    with checkpoint.as_directory() as checkpoint_dir_1:
        lock_file_1 = Path(_get_del_lock_path(checkpoint_dir_1))

        # Pretend that the 2nd `as_directory` is called from a different process.
        monkeypatch.setattr(os, "getpid", lambda: 22222)

        with checkpoint.as_directory() as checkpoint_dir_2:
            lock_file_2 = Path(_get_del_lock_path(checkpoint_dir_2))

            # Should point both processes to the same canonical temp directory.
            assert checkpoint_dir_1 == checkpoint_dir_2

            if is_local_checkpoint:
                # Check that 2 different lock files were created.
                assert not lock_file_1.exists()
                assert not lock_file_2.exists()
            else:
                # Check that 2 different lock files were created.
                assert lock_file_1.exists()
                assert lock_file_2.exists()

                assert len(_list_existing_del_locks(checkpoint_dir_1)) == 2

        if not is_local_checkpoint:
            # Check that the 2nd lock file was deleted.
            assert len(_list_existing_del_locks(checkpoint_dir_1)) == 1
            assert not lock_file_2.exists()

    if not is_local_checkpoint:
        # Check that the 1st lock file was deleted.
        assert len(_list_existing_del_locks(checkpoint_dir_1)) == 0
        assert not lock_file_1.exists()

        # Check that the temp checkpoint directory was deleted.
        assert not Path(checkpoint_dir_1).exists()


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
