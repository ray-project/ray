from pathlib import Path

import pyarrow.fs
import pytest

from ray.train.checkpoint import _CHECKPOINT_DIR_PREFIX, Checkpoint
from ray.train._internal.storage import (
    _create_directory,
    _list_at_fs_path,
    _upload_to_fs_path,
    _download_from_fs_path,
)


from ray.train.tests.test_new_persistence import _create_mock_custom_fs


_CHECKPOINT_CONTENT_FILE = "dummy.txt"


@pytest.fixture(params=["local", "mock", "custom_fs"])
def checkpoint(request, tmp_path):
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
    assert _CHECKPOINT_DIR_PREFIX in checkpoint_path.name


def test_to_directory_with_user_specified_path(checkpoint: Checkpoint, tmp_path):
    # Test with a string
    checkpoint_path = Path(checkpoint.to_directory(str(tmp_path / "special_dir")))
    assert (checkpoint_path / _CHECKPOINT_CONTENT_FILE).exists()
    assert checkpoint_path.name == "special_dir"

    # Test with a PathLike
    checkpoint_path = Path(checkpoint.to_directory(tmp_path / "special_dir"))
    assert (checkpoint_path / _CHECKPOINT_CONTENT_FILE).exists()
    assert checkpoint_path.name == "special_dir"


def test_multiprocess_to_directory():
    pass


def test_as_directory():
    pass


def test_metadata():
    pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
