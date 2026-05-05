import uuid
from pathlib import Path
from typing import Optional
from unittest.mock import create_autospec

import pyarrow.fs
import pytest

import ray
from ray.train import Checkpoint, CheckpointConfig
from ray.train._internal.session import _TrainingResult
from ray.train.v2._internal.exceptions import CheckpointManagerInitializationError
from ray.train.v2._internal.execution.checkpoint.checkpoint_manager import (
    CheckpointManager,
    _is_in_band,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.training_report import _TrainingReport
from ray.train.v2._internal.execution.worker_group import Worker
from ray.train.v2.tests.util import create_dummy_training_reports


@pytest.fixture(autouse=True, scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def _checkpoint_managers_equal(cm1: CheckpointManager, cm2: CheckpointManager) -> bool:
    """
    Compare two checkpoint managers for equality.
    Ignore uuid differences of all the checkpoints recorded.
    """

    def _training_results_equal(
        tr1: Optional[_TrainingResult], tr2: Optional[_TrainingResult]
    ) -> bool:
        if not tr1 and not tr2:
            return True
        if not tr1 or not tr2:
            return False
        return (
            tr1.metrics == tr2.metrics
            and tr1.checkpoint.path == tr2.checkpoint.path
            and tr1.checkpoint.filesystem == tr2.checkpoint.filesystem
        )

    def _checkpoint_to_report_indices_equal(
        cm1: CheckpointManager, cm2: CheckpointManager
    ) -> bool:
        # Do this because Checkpoint and Filesystem are not hashable.
        cm1_dict = {
            checkpoint.path: report_index
            for checkpoint, report_index in cm1._checkpoint_to_report_index.items()
        }
        cm2_dict = {
            checkpoint.path: report_index
            for checkpoint, report_index in cm2._checkpoint_to_report_index.items()
        }
        return cm1_dict == cm2_dict

    if cm1._checkpoint_config != cm2._checkpoint_config:
        return False
    if not _training_results_equal(
        cm1.latest_checkpoint_result, cm2.latest_checkpoint_result
    ):
        return False
    if not _training_results_equal(
        cm1.best_checkpoint_result, cm2.best_checkpoint_result
    ):
        return False
    if len(cm1.best_checkpoint_results) != len(cm2.best_checkpoint_results):
        return False
    for tr1, tr2 in zip(cm1.best_checkpoint_results, cm2.best_checkpoint_results):
        if not _training_results_equal(tr1, tr2):
            return False
    if cm1._current_report_index != cm2._current_report_index:
        return False
    if not _checkpoint_to_report_indices_equal(cm1, cm2):
        return False
    return True


@pytest.mark.parametrize(
    "checkpoint_config",
    [
        CheckpointConfig(),
        CheckpointConfig(
            num_to_keep=1,
            checkpoint_score_attribute="score",
            checkpoint_score_order="max",
        ),
    ],
)
@pytest.mark.asyncio
async def test_save_load_state_equivalence(
    monkeypatch, tmp_path, checkpoint_config: CheckpointConfig
):
    # Use async here because register_checkpoint creates an async task

    # Mock the delete function as we don't want report checkpoints to be deleted.
    monkeypatch.setattr(
        ray.train.v2._internal.execution.checkpoint.checkpoint_manager,
        "delete_fs_path",
        lambda *args, **kwargs: None,
    )
    exp_name = f"checkpoint_manager_test-{uuid.uuid4().hex}"

    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name=exp_name,
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=checkpoint_config,
    )
    training_reports = create_dummy_training_reports(
        num_results=2, storage_context=storage_context
    ) + create_dummy_training_reports(
        num_results=1,
        storage_context=storage_context,
        include_validation=True,
        starting_checkpoint_index=2,
    )

    # Register the training results into checkpoint manager
    for i, tr in enumerate(training_reports):
        checkpoint_manager.register_checkpoint(tr)
        assert checkpoint_manager._current_report_index == i + 1
        loaded_checkpoint_manager = CheckpointManager(
            storage_context=storage_context,
            checkpoint_config=checkpoint_config,
        )
        assert _checkpoint_managers_equal(checkpoint_manager, loaded_checkpoint_manager)


@pytest.mark.parametrize(
    "json_state,match",
    [
        (
            '{"dummy": "1", "dummy_dict": {"key": "value"}}',
            "You are loading a checkpoint manager snapshot saved with an unknown Ray version but",
        ),
        ('{"ray_version": "2.0.0", "dummy": "1", "dummy_dict": {"key": "value"', None),
        (
            '{"ray_version": "2.0.0", "dummy": "1", "dummy_dict": {"key": "value"}}',
            "You are loading a checkpoint manager snapshot saved with Ray version 2.0.0 but",
        ),
    ],
)
def test_load_state_error(tmp_path, json_state, match):

    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="load_state_error_experiment",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    with pytest.raises(
        CheckpointManagerInitializationError,
        match=match,
    ):
        checkpoint_manager._load_state(json_state)


@pytest.mark.asyncio
async def test_before_init_train_context(tmp_path):

    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="my_experiment_name",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    workers = [create_autospec(Worker, instance=True) for _ in range(4)]

    # Assert without a checkpoint.
    assert checkpoint_manager.before_init_train_context(workers) == {
        "checkpoint": [None] * 4,
        "current_report_index": [0] * 4,
    }

    # Assert with a checkpoint
    latest_checkpoint_report = create_dummy_training_reports(1, storage_context)[0]
    checkpoint_manager.register_checkpoint(latest_checkpoint_report)
    assert checkpoint_manager.before_init_train_context(workers) == {
        "checkpoint": [latest_checkpoint_report.checkpoint] * 4,
        "current_report_index": [1] * 4,
    }


@pytest.mark.asyncio
async def test_pending_checkpoint_management(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="pending_checkpoint_management_experiment",
    )
    checkpoint_config = CheckpointConfig(
        num_to_keep=1,
        checkpoint_score_attribute="score",
        checkpoint_score_order="max",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=checkpoint_config,
    )
    (
        low_initial_high_final_training_report,
        high_initial_low_final_training_report,
        final_training_report,
    ) = create_dummy_training_reports(
        num_results=3, storage_context=storage_context, include_validation=True
    )
    final_training_report.validation = False
    scoreless_training_report = create_dummy_training_reports(
        num_results=1,
        storage_context=storage_context,
        include_metrics=False,
        starting_checkpoint_index=3,
    )[0]

    # Register pending/final/unknown checkpoints and verify their storage
    checkpoint_manager.register_checkpoint(low_initial_high_final_training_report)
    checkpoint_manager.register_checkpoint(final_training_report)
    checkpoint_manager.register_checkpoint(scoreless_training_report)
    checkpoint_manager.register_checkpoint(high_initial_low_final_training_report)
    assert [tr.checkpoint for tr in checkpoint_manager._checkpoint_results] == [
        low_initial_high_final_training_report.checkpoint,  # keep pending
        high_initial_low_final_training_report.checkpoint,  # keep pending/latest
        final_training_report.checkpoint,  # keep highest final score so far
    ]

    # Assert checkpoint state after all tasks are done
    checkpoint_manager.update_checkpoints_with_metrics(
        {
            low_initial_high_final_training_report.checkpoint: {"score": 200},
            high_initial_low_final_training_report.checkpoint: {"score": 100},
        }
    )
    assert [tr.checkpoint for tr in checkpoint_manager._checkpoint_results] == [
        high_initial_low_final_training_report.checkpoint,  # keep latest checkpoint
        low_initial_high_final_training_report.checkpoint,  # keep highest score checkpoint
    ]


@pytest.mark.asyncio
async def test_pending_checkpoint_management_break_ties_by_report_index(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="pending_checkpoint_management_break_ties_by_report_index_experiment",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    training_reports = create_dummy_training_reports(
        num_results=2, storage_context=storage_context, include_validation=True
    )
    checkpoint_manager.register_checkpoint(training_reports[0])
    checkpoint_manager.register_checkpoint(training_reports[1])
    assert [tr.checkpoint for tr in checkpoint_manager._checkpoint_results] == [
        training_reports[0].checkpoint,
        training_reports[1].checkpoint,
    ]
    checkpoint_manager.update_checkpoints_with_metrics(
        {
            training_reports[1].checkpoint: {},
        }
    )
    assert [tr.checkpoint for tr in checkpoint_manager._checkpoint_results] == [
        training_reports[0].checkpoint,
        training_reports[1].checkpoint,
    ]
    checkpoint_manager.update_checkpoints_with_metrics(
        {
            training_reports[0].checkpoint: {},
        }
    )
    assert [tr.checkpoint for tr in checkpoint_manager._checkpoint_results] == [
        training_reports[0].checkpoint,
        training_reports[1].checkpoint,
    ]


@pytest.mark.asyncio
async def test_pending_checkpoint_management_finalized_checkpoint(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="pending_checkpoint_management_experiment",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(
            checkpoint_score_attribute="score",
            checkpoint_score_order="max",
        ),
    )
    training_reports = create_dummy_training_reports(
        num_results=2, storage_context=storage_context
    )
    checkpoint_manager.register_checkpoint(training_reports[0])
    checkpoint_manager.register_checkpoint(training_reports[1])
    assert [tr.checkpoint for tr in checkpoint_manager._checkpoint_results] == [
        training_reports[0].checkpoint,
        training_reports[1].checkpoint,
    ]
    checkpoint_manager.update_checkpoints_with_metrics(
        {
            training_reports[0].checkpoint: {"score": 100},
        }
    )
    assert [tr.checkpoint for tr in checkpoint_manager._checkpoint_results] == [
        training_reports[0].checkpoint,
        training_reports[1].checkpoint,
    ]


def test_update_checkpoints_with_metrics_not_in_checkpoint_results(tmp_path):
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="update_checkpoints_with_metrics_error_experiment",
    )
    checkpoint_manager = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    training_reports = create_dummy_training_reports(
        num_results=1, storage_context=storage_context
    )
    checkpoint_manager._pending_training_results[training_reports[0].checkpoint] = (
        _TrainingResult(training_reports[0].checkpoint, training_reports[0].metrics),
        training_reports[0].validation,
    )
    with pytest.raises(ValueError):
        checkpoint_manager.update_checkpoints_with_metrics(
            {training_reports[0].checkpoint: {"score": 100}}
        )


@pytest.mark.asyncio
async def test_save_load_out_of_band_checkpoint(monkeypatch, tmp_path):
    """Out-of-band checkpoints (path outside experiment_fs_path or on a
    different filesystem) should round-trip through the snapshot using their
    full path + filesystem type, alongside in-band checkpoints in the same run.
    """
    monkeypatch.setattr(
        ray.train.v2._internal.execution.checkpoint.checkpoint_manager,
        "delete_fs_path",
        lambda *args, **kwargs: None,
    )
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name=f"oob_test-{uuid.uuid4().hex}",
    )
    cm = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )

    # An in-band checkpoint (under experiment_fs_path on storage_filesystem).
    in_band_report = create_dummy_training_reports(
        num_results=1, storage_context=storage_context
    )[0]
    assert _is_in_band(in_band_report.checkpoint, storage_context)

    # An out-of-band checkpoint at a path NOT under experiment_fs_path
    # (on the same local filesystem, exercising same-FS-out-of-band).
    oob_dir = tmp_path / "external_checkpoint"
    oob_dir.mkdir()
    oob_path = Path(oob_dir).as_posix()
    oob_report = _TrainingReport(
        checkpoint=Checkpoint(path=oob_path, filesystem=pyarrow.fs.LocalFileSystem()),
        metrics={"score": 1},
        validation=False,
    )
    assert not _is_in_band(oob_report.checkpoint, storage_context)

    cm.register_checkpoint(in_band_report)
    cm.register_checkpoint(oob_report)

    # Reload from disk and confirm both checkpoints round-trip to the same
    # paths, with the out-of-band one still resolving outside the storage path.
    cm2 = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    assert _checkpoint_managers_equal(cm, cm2)

    cm2_paths = [tr.checkpoint.path for tr in cm2._checkpoint_results]
    assert cm2_paths == [in_band_report.checkpoint.path, oob_path]
    assert _is_in_band(cm2._checkpoint_results[0].checkpoint, storage_context)
    assert not _is_in_band(cm2._checkpoint_results[1].checkpoint, storage_context)


@pytest.mark.asyncio
async def test_load_v0_snapshot_assumes_in_band(monkeypatch, tmp_path):
    """A v0 snapshot (no version, no out_of_band fields) should load as in-band
    using the storage filesystem.
    """
    monkeypatch.setattr(
        ray.train.v2._internal.execution.checkpoint.checkpoint_manager,
        "delete_fs_path",
        lambda *args, **kwargs: None,
    )
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name=f"v0_test-{uuid.uuid4().hex}",
    )

    # Make a real checkpoint dir under the experiment so existence check passes.
    ckpt_dir = Path(storage_context.experiment_fs_path) / "checkpoint_0"
    ckpt_dir.mkdir(parents=True, exist_ok=True)

    cm = CheckpointManager(
        storage_context=storage_context,
        checkpoint_config=CheckpointConfig(),
    )
    # Hand-craft a v0-style snapshot: no `out_of_band`, no `checkpoint_path`,
    # `version: 0`, only `checkpoint_dir_name`.
    v0_snapshot = (
        '{"ray_version": "%s",'
        ' "checkpoint_results": [{"version": 0, "checkpoint_dir_name": "checkpoint_0",'
        ' "metrics": {"score": 1}}],'
        ' "checkpoint_report_indices": [0],'
        ' "latest_checkpoint_result": {"version": 0, "checkpoint_dir_name":'
        ' "checkpoint_0", "metrics": {"score": 1}},'
        ' "pending_training_results": [], "pending_validation_specs": [],'
        ' "current_report_index": 1, "validated_checkpoint_dir_names": []}'
    ) % ray.__version__
    cm._load_state(v0_snapshot)

    assert len(cm._checkpoint_results) == 1
    restored = cm._checkpoint_results[0].checkpoint
    assert restored.path == Path(ckpt_dir).as_posix()
    assert restored.filesystem.type_name == storage_context.storage_filesystem.type_name
    # v0 entries default to in-band.
    assert _is_in_band(restored, storage_context)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
