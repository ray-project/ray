import os
from pathlib import Path

import pytest
import pyarrow.fs

from ray.train._internal.storage import StorageContext


def test_custom_fs_validation(tmp_path):
    """Tests that invalid storage_path inputs give reasonable errors when a
    custom filesystem is used."""
    exp_name = "test"
    StorageContext(
        storage_path=str(tmp_path),
        experiment_dir_name=exp_name,
        storage_filesystem=pyarrow.fs.LocalFileSystem(),
    )
    with pytest.raises(pyarrow.lib.ArrowInvalid) as excinfo:
        StorageContext(
            storage_path=f"file://{tmp_path}",
            experiment_dir_name=exp_name,
            storage_filesystem=pyarrow.fs.LocalFileSystem(),
        )
    assert "Expected a local filesystem path, got a URI:" in str(excinfo.value)

    mock_fs, _ = pyarrow.fs.FileSystem.from_uri("mock://a")
    with pytest.raises(pyarrow.lib.ArrowInvalid) as excinfo:
        StorageContext(
            storage_path=f"mock:///a",
            experiment_dir_name=exp_name,
            storage_filesystem=mock_fs,
        )
    assert "Expected a filesystem path, got a URI:" in str(excinfo.value)

    StorageContext(
        storage_path="a",
        experiment_dir_name=exp_name,
        storage_filesystem=mock_fs,
    )


def test_storage_path_inputs():
    exp_name = "test_storage_path"

    # Relative paths don't work
    with pytest.raises(pyarrow.lib.ArrowInvalid) as excinfo:
        StorageContext(storage_path="./results", experiment_dir_name=exp_name)
    assert "URI has empty scheme" in str(excinfo.value)

    with pytest.raises(pyarrow.lib.ArrowInvalid) as excinfo:
        StorageContext(storage_path="results", experiment_dir_name=exp_name)
    assert "URI has empty scheme" in str(excinfo.value)

    # Tilde paths work
    StorageContext(storage_path="~/ray_results", experiment_dir_name=exp_name)

    # Paths with lots of extra . .. and /
    path = os.path.expanduser("~/ray_results")
    path = os.path.join(path, ".", "..", "ray_results", ".")
    path.replace(os.path.sep, os.path.sep * 2)
    storage = StorageContext(storage_path=path, experiment_dir_name=exp_name)

    storage.storage_filesystem.create_dir(
        os.path.join(storage.storage_fs_path, "test_dir")
    )
    assert Path("~/ray_results/test_dir").expanduser().exists()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
