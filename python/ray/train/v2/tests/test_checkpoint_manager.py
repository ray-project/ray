import logging
import os
import tempfile
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
    _TrainingResultState,
    _get_state_from_training_result,
    _get_training_result_from_state,
)
from ray.train.v2._internal.execution.storage import StorageContext
from ray.train.v2._internal.execution.worker_group import Worker
from ray.train.v2.api.config import RunConfig
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.report_config import CheckpointUploadMode
from ray.train.v2.api.result import Result
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


def test_save_load_state_oob_uri_round_trip(tmp_path):
    """A checkpoint whose filesystem differs from the storage context's
    round-trips through the snapshot via the captured URI rather than the
    dir-name path.
    """
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="oob_uri_round_trip",
    )

    # `?region=...` keeps `from_uri` from making a network call to detect the
    # bucket's region.
    oob_uri = "s3://my-bucket/checkpoint-key?region=us-east-1"
    oob_checkpoint = Checkpoint(oob_uri)
    assert oob_checkpoint._source_uri == oob_uri
    training_result = _TrainingResult(
        checkpoint=oob_checkpoint,
        metrics={"score": 1.0},
    )

    state = _get_state_from_training_result(training_result, storage_context)
    assert state.checkpoint_uri == oob_uri

    restored = _get_training_result_from_state(state, storage_context)
    assert isinstance(restored.checkpoint.filesystem, pyarrow.fs.S3FileSystem)
    assert restored.checkpoint.path == "my-bucket/checkpoint-key"
    assert restored.metrics == {"score": 1.0}


def test_save_load_state_local_oob_via_from_directory(tmp_path):
    """`Checkpoint.from_directory` populates _source_uri so that even local
    out-of-band checkpoints (where fs == storage fs) on a *different* fs
    instance would still round-trip — and confirms same-fs local OOB still
    uses the cheaper dir-name path.
    """
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="oob_from_directory",
    )

    oob_dir = tmp_path / "outside_experiment" / "checkpoint-0"
    oob_dir.mkdir(parents=True)
    oob_checkpoint = Checkpoint.from_directory(oob_dir)
    # from_directory hands in a LocalFileSystem with an absolute path, so the
    # constructor derives a file:// URI even though no URI was passed.
    assert oob_checkpoint._source_uri == oob_dir.as_uri()

    training_result = _TrainingResult(
        checkpoint=oob_checkpoint,
        metrics={"score": 2.0},
    )
    state = _get_state_from_training_result(training_result, storage_context)
    # Same filesystem (local == local), so URI is not persisted; dir-name path
    # already round-trips correctly for absolute local OOB paths.
    assert state.checkpoint_uri is None

    restored = _get_training_result_from_state(state, storage_context)
    assert restored.checkpoint == oob_checkpoint


def test_save_state_warns_when_filesystem_differs_without_uri(tmp_path):
    """`Checkpoint(path=..., filesystem=remote_fs)` provides no URI to capture.
    The save helper emits a warning and marks the state unrestorable, but the
    run is allowed to continue — the failure surfaces at resumption time.
    """
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="oob_no_uri",
    )

    s3_fs = pyarrow.fs.S3FileSystem(anonymous=True, region="us-east-1")
    checkpoint = Checkpoint(path="my-bucket/key", filesystem=s3_fs)
    assert checkpoint._source_uri is None

    records: list[logging.LogRecord] = []

    class _ListHandler(logging.Handler):
        def emit(self, record):
            records.append(record)

    target_logger = logging.getLogger(
        "ray.train.v2._internal.execution.checkpoint.checkpoint_manager"
    )
    handler = _ListHandler(level=logging.WARNING)
    target_logger.addHandler(handler)
    try:
        training_result = _TrainingResult(checkpoint=checkpoint, metrics={})
        state = _get_state_from_training_result(training_result, storage_context)
    finally:
        target_logger.removeHandler(handler)

    assert state.is_unrestorable is True
    assert state.checkpoint_uri is None
    assert any("cannot be restored" in r.getMessage() for r in records)


def test_load_state_raises_for_unrestorable_checkpoint(tmp_path):
    """A snapshot entry flagged as unrestorable must not silently restore with
    the wrong filesystem — `_get_training_result_from_state` raises so the
    failure surfaces at `CheckpointManager.__init__` time.
    """
    storage_context = StorageContext(
        storage_path=tmp_path,
        experiment_dir_name="oob_load_unrestorable",
    )

    state = _TrainingResultState(
        checkpoint_dir_name="my-bucket/key",
        metrics={"score": 1.0},
        checkpoint_uri=None,
        is_unrestorable=True,
    )
    with pytest.raises(
        CheckpointManagerInitializationError, match="cannot be resumed"
    ):
        _get_training_result_from_state(state, storage_context)


def test_out_of_band_checkpointing(tmp_path):
    """Check that the checkpoint manager can handle out of band checkpointing.

    The checkpoint manager exists that the checkpoints are saved within the
    storage_path (experiment_dir). Out of band checkpoint are ones not saved
    in the storage_path, so tmp_path is used.

    Further, this test checks that a restored trainer still correctly handles
    in and out of band checkpointing.
    """

    def write_file(file_path, content: str):
        os.makedirs(file_path.parent, exist_ok=True)
        with open(file_path, "w") as f:
            f.write(content)

    with tempfile.TemporaryDirectory() as _experiment_dir:
        # For MacOS, the tempfile can be resolved differently, therefore, we force it
        experiment_dir = Path(_experiment_dir).resolve()

        expected_checkpoint_paths = [
            # this doesn't use a checkpoint_dir_name therefore we can't determine the saved checkpoint name
            # experiment_dir / "test-out-of-band-checkpointing",
            experiment_dir / "test-out-of-band-checkpointing" / "test",
            experiment_dir / "epoch-3",
            tmp_path / "epoch-4",
            experiment_dir / "epoch-5",
            tmp_path / "epoch-6",
            tmp_path / "test" / "epoch-7",
        ]

        def train_fn():
            # save
            write_file(tmp_path / "epoch-1" / "results.txt", "1")
            ray.train.report(
                metrics={"score": 1},
                checkpoint=Checkpoint(tmp_path / "epoch-1"),
            )
            # save with checkpoint dir name
            write_file(tmp_path / "epoch-2" / "test" / "results.txt", "2")
            ray.train.report(
                metrics={"score": 2},
                checkpoint=Checkpoint(tmp_path / "epoch-2"),
                checkpoint_dir_name="test",
            )

            # Save in-band with NO_UPLOAD
            write_file(experiment_dir / "epoch-3" / "test" / "results.txt", "3")
            ray.train.report(
                metrics={"score": 3},
                checkpoint=Checkpoint(experiment_dir / "epoch-3"),
                checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
            )
            # save out of band with NO_UPLOAD
            write_file(tmp_path / "epoch-4" / "results.txt", "4")
            ray.train.report(
                metrics={"score": 4},
                checkpoint=Checkpoint(tmp_path / "epoch-4"),
                checkpoint_upload_mode=CheckpointUploadMode.NO_UPLOAD,
            )

            # save in-band with custom checkpoint_upload_fn
            write_file(experiment_dir / "epoch-5" / "results.txt", "5")
            ray.train.report(
                metrics={"score": 5},
                checkpoint=Checkpoint(experiment_dir / "epoch-5"),
                checkpoint_upload_fn=lambda ckpt, name: ckpt,
            )
            # save out of band with custom checkpoint_upload_fn
            write_file(tmp_path / "epoch-6" / "results.txt", "6")
            ray.train.report(
                metrics={"score": 6},
                checkpoint=Checkpoint(tmp_path / "epoch-6"),
                checkpoint_upload_fn=lambda ckpt, name: ckpt,
            )
            write_file(tmp_path / "test" / "epoch-7" / "results.txt", "7")
            ray.train.report(
                metrics={"score": 7},
                checkpoint=Checkpoint(tmp_path / "test" / "epoch-7"),
                checkpoint_dir_name="test",
                checkpoint_upload_fn=lambda ckpt, name: ckpt,
            )

            # ensures that all the ray.train.report have finished.
            reported_checkpoints = ray.train.get_all_reported_checkpoints()
            assert len(reported_checkpoints) == 7

        data_trainer = DataParallelTrainer(
            train_fn,
            run_config=RunConfig(
                name="test-out-of-band-checkpointing",
                storage_path=str(experiment_dir),
                checkpoint_config=CheckpointConfig(num_to_keep=None),  # all
            ),
        )
        results: Result = data_trainer.fit()
        result_checkpoints = results.best_checkpoints

        assert result_checkpoints is not None and len(result_checkpoints) == 7
        # The checkpoint manager add the final folder meaning that you can't
        #   a priori know the final checkpoint path if no checkpoint dir name is used.
        assert (
            Path(result_checkpoints[0][0].path).parent
            == Path(experiment_dir) / "test-out-of-band-checkpointing"
        )
        # Check the rest of the checkpoint paths
        for checkpoint, score, expected_path in zip(
            result_checkpoints[1:], range(2, 8), expected_checkpoint_paths, strict=True
        ):
            assert checkpoint == (Checkpoint(expected_path), {"score": score})

        # Confirm that if you restore an experiment that contains out of band
        #   checkpoints that the checkpoint paths are all correct still.
        def restored_train_fn():
            reported_checkpoints = ray.train.get_all_reported_checkpoints()
            assert len(reported_checkpoints) == 7
            for (checkpoint, metric), reported_checkpoint in zip(
                result_checkpoints, reported_checkpoints, strict=True
            ):
                assert checkpoint == reported_checkpoint.checkpoint
                assert metric == reported_checkpoint.metrics

        restored_data_trainer = DataParallelTrainer(
            restored_train_fn,
            run_config=RunConfig(
                name="test-out-of-band-checkpointing",
                storage_path=str(experiment_dir),
            ),
        )
        restored_results = restored_data_trainer.fit()
        restored_checkpoints = restored_results.best_checkpoints
        assert restored_checkpoints is not None and len(restored_checkpoints) == 7


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
