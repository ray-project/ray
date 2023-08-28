import os
from pathlib import Path

import pytest
import pyarrow.fs

import ray.cloudpickle as ray_pickle
from ray.train import Checkpoint, SyncConfig
from ray.train._internal.storage import (
    _VALIDATE_STORAGE_MARKER_FILENAME,
    StorageContext,
    _list_at_fs_path,
)

from ray.train.tests.test_new_persistence import _resolve_storage_type


@pytest.fixture(params=[None, "nfs", "cloud", "custom_fs"])
def storage(request, tmp_path) -> StorageContext:
    storage_type = request.param
    with _resolve_storage_type(storage_type, tmp_path) as (
        storage_path,
        storage_filesystem,
    ):
        yield StorageContext(
            storage_path=storage_path,
            experiment_dir_name="exp_name",
            storage_filesystem=storage_filesystem,
            trial_dir_name="trial_name",
            sync_config=SyncConfig(
                sync_artifacts=True, sync_artifacts_on_checkpoint=True, sync_period=1000
            ),
        )


@pytest.fixture(autouse=True)
def local_path(tmp_path, monkeypatch):
    local_dir = str(tmp_path / "ray_results")
    monkeypatch.setenv("RAY_AIR_LOCAL_CACHE_DIR", local_dir)
    yield local_dir


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


def test_no_syncing_needed(local_path):
    """Tests that we don't create a syncer if no syncing is needed."""
    storage = StorageContext(storage_path=local_path, experiment_dir_name="test_dir")
    assert storage.syncer is None


def test_storage_validation_marker(storage: StorageContext):
    # A marker should have been created at initialization
    storage._check_validation_file()

    # Remove the marker to simulate being on a new node w/o access to the shared storage
    storage.storage_filesystem.delete_file(
        os.path.join(storage.experiment_fs_path, _VALIDATE_STORAGE_MARKER_FILENAME)
    )

    # Simulate passing the storage context around through the object store
    # The constructor is NOT called again -- so the marker should not be checked here
    # and we shouldn't raise an error
    storage = ray_pickle.loads(ray_pickle.dumps(storage))

    # We should raise an error when we try to checkpoint now.
    with pytest.raises(RuntimeError) as excinfo:
        storage.persist_current_checkpoint(Checkpoint.from_directory("/tmp/dummy"))
    assert "Unable to set up cluster storage" in str(excinfo.value)


def test_persist_current_checkpoint(storage: StorageContext, tmp_path):
    # Uploading a non-existent checkpoint directory should fail
    with pytest.raises(FileNotFoundError):
        storage.persist_current_checkpoint(
            Checkpoint.from_directory("/tmp/nonexistent/checkpoint")
        )

    # Uploading an empty checkpoint directory
    (tmp_path / "empty").mkdir()
    storage.persist_current_checkpoint(Checkpoint.from_directory(tmp_path / "empty"))
    assert (
        _list_at_fs_path(storage.storage_filesystem, storage.checkpoint_fs_path) == []
    )

    # Normal use case: Uploading an checkpoint directory with files
    (tmp_path / "regular").mkdir()
    (tmp_path / "regular" / "1.txt").touch()
    storage.persist_current_checkpoint(Checkpoint.from_directory(tmp_path / "regular"))
    assert _list_at_fs_path(storage.storage_filesystem, storage.checkpoint_fs_path) == [
        "1.txt"
    ]

    storage.current_checkpoint_index += 1

    # Persisting a checkpoint that is already at the correct path (for local fs case)
    if isinstance(storage.storage_filesystem, pyarrow.fs.LocalFileSystem):
        final_checkpoint_dir = Path(storage.checkpoint_fs_path)
        final_checkpoint_dir.mkdir(parents=True)
        (final_checkpoint_dir / "2.txt").touch()
        storage.persist_current_checkpoint(
            Checkpoint.from_directory(final_checkpoint_dir)
        )


def test_persist_artifacts(storage: StorageContext):
    """Tests typical `StorageContext.persist_artifacts(force=True/False)` usage."""
    trial_local_path = Path(storage.trial_local_path)
    trial_local_path.mkdir(parents=True)
    trial_local_path.joinpath("1.txt").touch()

    storage.persist_artifacts()

    if not storage.syncer:
        # No syncing is needed -- pass early if storage_path == storage_local_path
        assert _list_at_fs_path(storage.storage_filesystem, storage.trial_fs_path) == [
            "1.txt"
        ]
        return

    storage.syncer.wait()

    assert sorted(
        _list_at_fs_path(storage.storage_filesystem, storage.trial_fs_path)
    ) == ["1.txt"]

    trial_local_path.joinpath("2.txt").touch()

    # A new sync should not be triggered because sync_period is 1000 seconds
    storage.persist_artifacts()
    storage.syncer.wait()

    # -> No change in the persisted files
    assert sorted(
        _list_at_fs_path(storage.storage_filesystem, storage.trial_fs_path)
    ) == ["1.txt"]

    # This is what happens on `train.report` when a checkpoint is reported
    # and `sync_artifacts_on_checkpoint=True`
    storage.persist_artifacts(force=storage.sync_config.sync_artifacts_on_checkpoint)
    assert sorted(
        _list_at_fs_path(storage.storage_filesystem, storage.trial_fs_path)
    ) == ["1.txt", "2.txt"]


def test_persist_artifacts_failures(storage: StorageContext):
    """Tests `StorageContext.persist_artifacts` edge cases (empty directory)."""
    if not storage.syncer:
        # Should be a no-op if storage_path == storage_local_path (no syncing needed)
        storage.persist_artifacts()
        return

    # Uploading before the trial directory has been created should fail
    with pytest.raises(FileNotFoundError):
        storage.persist_artifacts()
        if storage.syncer:
            storage.syncer.wait()

    with pytest.raises(FileNotFoundError):
        storage.persist_artifacts(force=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
