import os
import uuid
from pathlib import Path

import pyarrow.fs
import pytest

import ray
import ray.cloudpickle as ray_pickle
from ray.train import Checkpoint
from ray.train.tests.test_new_persistence import _resolve_storage_type
from ray.train.v2._internal.constants import VALIDATE_STORAGE_MARKER_FILENAME
from ray.train.v2._internal.execution.storage import StorageContext, _list_at_fs_path


@pytest.fixture(params=["nfs", "cloud", "custom_fs"])
def storage(request, tmp_path) -> StorageContext:
    storage_type = request.param
    with _resolve_storage_type(storage_type, tmp_path) as (
        storage_path,
        storage_filesystem,
    ):
        yield StorageContext(
            storage_path=storage_path,
            experiment_dir_name=f"storage_type={storage_type}-{uuid.uuid4().hex}",
            storage_filesystem=storage_filesystem,
        )


@pytest.fixture(autouse=True, scope="module")
def ray_init():
    # NOTE: This is needed to set the `/tmp/ray/session_*` directory.
    ray.init()
    yield
    ray.shutdown()


def test_custom_fs_validation(tmp_path):
    """Tests that invalid storage_path inputs give reasonable errors when a
    custom filesystem is used."""
    exp_name = "test"
    StorageContext(
        storage_path=str(tmp_path),
        experiment_dir_name=exp_name,
        storage_filesystem=pyarrow.fs.LocalFileSystem(),
    )

    mock_fs, _ = pyarrow.fs.FileSystem.from_uri("mock://a")
    with pytest.raises(pyarrow.lib.ArrowInvalid) as excinfo:
        StorageContext(
            storage_path="mock:///a",
            experiment_dir_name=exp_name,
            storage_filesystem=mock_fs,
        )
    print("Custom fs with URI storage path error: ", excinfo.value)

    StorageContext(
        storage_path="a",
        experiment_dir_name=exp_name,
        storage_filesystem=mock_fs,
    )


def test_storage_path_inputs():
    """Tests storage path input edge cases."""
    exp_name = "test_storage_path"

    # Relative paths don't work
    with pytest.raises(pyarrow.lib.ArrowInvalid) as excinfo:
        StorageContext(storage_path="./results", experiment_dir_name=exp_name)
    assert "URI has empty scheme" in str(excinfo.value)

    with pytest.raises(pyarrow.lib.ArrowInvalid) as excinfo:
        StorageContext(storage_path="results", experiment_dir_name=exp_name)
    assert "URI has empty scheme" in str(excinfo.value)

    # Tilde paths sometimes raise... They do not work on the CI machines.
    try:
        StorageContext(storage_path="~/ray_results", experiment_dir_name=exp_name)
    except pyarrow.lib.ArrowInvalid as e:
        print(e)
        pass

    # Paths with lots of extra . .. and /
    path = os.path.expanduser("~/ray_results")
    path = os.path.join(path, ".", "..", "ray_results", ".")
    path = path.replace(os.path.sep, os.path.sep * 2)
    storage = StorageContext(storage_path=path, experiment_dir_name=exp_name)

    storage.storage_filesystem.create_dir(
        os.path.join(storage.storage_fs_path, "test_dir")
    )
    assert Path("~/ray_results/test_dir").expanduser().exists()

    # Path objects work
    StorageContext(storage_path=Path(path), experiment_dir_name=exp_name)


def test_storage_validation_marker(storage: StorageContext):
    # A marker should have been created at initialization
    storage._check_validation_file()

    # Remove the marker to simulate being on a new node w/o access to the shared storage
    storage.storage_filesystem.delete_file(
        os.path.join(storage.experiment_fs_path, VALIDATE_STORAGE_MARKER_FILENAME)
    )

    # Simulate passing the storage context around through the object store
    # The constructor is NOT called again -- so the marker should not be checked here
    # and we shouldn't raise an error
    storage = ray_pickle.loads(ray_pickle.dumps(storage))

    # Create a checkpoint dir name to storage the checkpoint
    checkpoint_dir_name = storage.make_default_checkpoint_dir_name()

    # We should raise an error when we try to checkpoint now.
    with pytest.raises(RuntimeError) as excinfo:
        storage.persist_current_checkpoint(
            Checkpoint.from_directory("/tmp/dummy"), checkpoint_dir_name
        )
    assert "Unable to set up cluster storage" in str(excinfo.value)


def test_persist_current_checkpoint(storage: StorageContext, tmp_path):
    # Create a checkpoint dir name and filesystem path to storage the checkpoint
    checkpoint_dir_name = storage.make_default_checkpoint_dir_name()
    checkpoint_fs_path = Path(
        storage.experiment_fs_path, checkpoint_dir_name
    ).as_posix()

    # Uploading a non-existent checkpoint directory should fail
    with pytest.raises(FileNotFoundError):
        storage.persist_current_checkpoint(
            Checkpoint.from_directory("/tmp/nonexistent/checkpoint"),
            checkpoint_dir_name,
        )

    # Uploading an empty checkpoint directory
    (tmp_path / "empty").mkdir()
    storage.persist_current_checkpoint(
        Checkpoint.from_directory(tmp_path / "empty"), checkpoint_dir_name
    )
    assert _list_at_fs_path(storage.storage_filesystem, checkpoint_fs_path) == []

    # Normal use case: Uploading an checkpoint directory with files
    (tmp_path / "regular").mkdir()
    (tmp_path / "regular" / "1.txt").touch()
    storage.persist_current_checkpoint(
        Checkpoint.from_directory(tmp_path / "regular"), checkpoint_dir_name
    )
    assert _list_at_fs_path(storage.storage_filesystem, checkpoint_fs_path) == ["1.txt"]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
