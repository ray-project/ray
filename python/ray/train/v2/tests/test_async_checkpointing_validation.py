import multiprocessing
import os
import shutil
import signal
import time
from unittest.mock import create_autospec

import pytest

import ray
import ray.cloudpickle as ray_pickle
from ray._common.test_utils import simulate_s3_bucket
from ray.air._internal.uri_utils import URI
from ray.tests.client_test_utils import create_remote_signal_actor
from ray.train import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.exceptions import WorkerGroupError
from ray.train.v2.api.report_config import (
    CheckpointConsistencyMode,
    CheckpointUploadMode,
)
from ray.train.v2.api.reported_checkpoint import ReportedCheckpointStatus
from ray.train.v2.api.validation_config import (
    ValidationConfig,
    ValidationTaskConfig,
)


@pytest.fixture(scope="module", autouse=True)
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


def test_report_mixed_checkpoint_upload_modes(tmp_path):
    """Run all 10 possible pairs (e.g. (SYNC, ASYNC)) of checkpoint upload modes between 2 workers."""

    def get_checkpoint_iteration(checkpoint):
        if not checkpoint:
            return -1
        return int(checkpoint.path.split("_")[-1])

    def train_fn():
        # When reporting with async checkpointing, write the checkpoint to
        # tmp_path, which stays alive for the duration of the test, instead of
        # tempfile.TemporaryDirectory(), which might get deleted before the
        # async checkpoint upload completes.

        # Run all 10 possible pairs of checkpoint upload modes
        rank = ray.train.get_context().get_world_rank()
        if rank == 0:
            ASYNC_ITERATIONS = [0, 1, 2, 3]
            SYNC_ITERATIONS = [4, 5, 6]
            NO_UPLOAD_ITERATIONS = [7, 8]
            NO_CHECKPOINT_ITERATIONS = [9]
        else:
            ASYNC_ITERATIONS = [0]
            SYNC_ITERATIONS = [1, 4]
            NO_UPLOAD_ITERATIONS = [2, 5, 7]
            NO_CHECKPOINT_ITERATIONS = [3, 6, 8, 9]

        prev_latest_checkpoint_iteration = -1
        for i in range(10):
            # Set variables
            if i in ASYNC_ITERATIONS:
                checkpoint_upload_mode = CheckpointUploadMode.ASYNC
            elif i in SYNC_ITERATIONS:
                checkpoint_upload_mode = CheckpointUploadMode.SYNC
            else:
                checkpoint_upload_mode = CheckpointUploadMode.NO_UPLOAD
            metrics = {"metric": f"iteration_{i}_shard_{rank}"}

            # Create and report checkpoint
            if i in NO_CHECKPOINT_ITERATIONS:
                ray.train.report(
                    metrics=metrics,
                    checkpoint=None,
                    validation=False,
                )
                assert prev_latest_checkpoint_iteration <= get_checkpoint_iteration(
                    ray.train.get_checkpoint()
                )
            else:
                # Create remote or local checkpoint_dir
                checkpoint_dir_name = f"checkpoint_iteration_{i}"
                if i in NO_UPLOAD_ITERATIONS:
                    checkpoint_dir = (
                        ray.train.get_context()
                        .get_storage()
                        .build_checkpoint_path_from_name(checkpoint_dir_name)
                    )
                else:
                    checkpoint_dir = os.path.join(
                        tmp_path, checkpoint_dir_name, f"_{rank}"
                    )

                # Create and report that remote or local checkpoint
                os.makedirs(checkpoint_dir, exist_ok=True)
                with open(os.path.join(checkpoint_dir, f"shard_{rank}"), "wb") as f:
                    ray_pickle.dump(f"iteration_{i}_shard_{rank}", f)
                checkpoint = Checkpoint(checkpoint_dir)
                ray.train.report(
                    metrics=metrics,
                    checkpoint=checkpoint,
                    checkpoint_upload_mode=checkpoint_upload_mode,
                    checkpoint_dir_name=checkpoint_dir_name,
                )

                # Check the status of latest_checkpoint
                latest_checkpoint = ray.train.get_checkpoint()
                if i in NO_UPLOAD_ITERATIONS:
                    assert latest_checkpoint == checkpoint
                elif i in SYNC_ITERATIONS:
                    assert checkpoint_dir_name in latest_checkpoint.path
                else:
                    assert prev_latest_checkpoint_iteration <= get_checkpoint_iteration(
                        latest_checkpoint
                    )

                prev_latest_checkpoint_iteration = get_checkpoint_iteration(
                    latest_checkpoint
                )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    # Note that the (checkpoint=None, checkpoint=None) pair does not produce any checkpoint
    assert len(result.best_checkpoints) == 9
    for i, (checkpoint, metrics) in enumerate(result.best_checkpoints):
        assert checkpoint.path.endswith(f"checkpoint_iteration_{i}")
        assert metrics["metric"] == f"iteration_{i}_shard_0"


@pytest.mark.parametrize(
    "delete_local_checkpoint_after_upload,checkpoint_upload_mode",
    [
        (True, CheckpointUploadMode.ASYNC),
        (False, CheckpointUploadMode.ASYNC),
        (True, CheckpointUploadMode.SYNC),
        (False, CheckpointUploadMode.SYNC),
        (True, CheckpointUploadMode.NO_UPLOAD),
        (False, CheckpointUploadMode.NO_UPLOAD),
    ],
)
def test_report_delete_local_checkpoint_after_upload(
    tmp_path,
    delete_local_checkpoint_after_upload,
    checkpoint_upload_mode,
):
    """Check that the local checkpoint is deleted after upload."""

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        if rank == 0:
            if checkpoint_upload_mode == CheckpointUploadMode.NO_UPLOAD:
                checkpoint_dir = (
                    ray.train.get_context()
                    .get_storage()
                    .build_checkpoint_path_from_name("my_checkpoint_dir")
                )
            else:
                checkpoint_dir = os.path.join(
                    tmp_path,
                    "my_checkpoint_dir",
                )
            os.makedirs(checkpoint_dir, exist_ok=True)
            with open(os.path.join(checkpoint_dir, "shard_0"), "wb") as f:
                ray_pickle.dump("some_checkpoint_contents", f)
            checkpoint = Checkpoint(checkpoint_dir)
            ray.train.report(
                {},
                checkpoint,
                checkpoint_upload_mode=checkpoint_upload_mode,
                delete_local_checkpoint_after_upload=delete_local_checkpoint_after_upload,
            )
        else:
            ray.train.report(
                {},
                None,
            )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    trainer.fit()
    if (
        delete_local_checkpoint_after_upload
        or checkpoint_upload_mode == CheckpointUploadMode.NO_UPLOAD
    ):
        assert not os.path.exists(os.path.join(tmp_path, "my_checkpoint_dir"))
    else:
        assert os.path.exists(os.path.join(tmp_path, "my_checkpoint_dir"))


def test_report_checkpoint_upload_error(monkeypatch, tmp_path):
    """Check that the trainer shuts down when an error occurs during checkpoint upload."""

    def train_fn():

        if ray.train.get_context().get_world_rank() == 0:

            # Mock persist_current_checkpoint to raise an error
            mock_persist_current_checkpoint = create_autospec(
                ray.train.get_context().get_storage().persist_current_checkpoint
            )
            mock_persist_current_checkpoint.side_effect = ValueError("error")
            monkeypatch.setattr(
                ray.train.get_context().get_storage(),
                "persist_current_checkpoint",
                mock_persist_current_checkpoint,
            )

            # Report minimal valid checkpoint
            local_checkpoint_dir = os.path.join(tmp_path, "local_checkpoint_dir")
            os.makedirs(local_checkpoint_dir, exist_ok=True)
            ray.train.report(
                {},
                Checkpoint.from_directory(local_checkpoint_dir),
                checkpoint_upload_mode=CheckpointUploadMode.ASYNC,
            )
        else:
            ray.train.report(
                {}, None, checkpoint_upload_mode=CheckpointUploadMode.ASYNC
            )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    with pytest.raises(WorkerGroupError, match="error") as exc_info:
        trainer.fit()
    assert isinstance(exc_info.value.worker_failures[0], ValueError)


@pytest.mark.parametrize(
    "kwarg",
    (
        # both of these cases can cause the local checkpoint to be deleted after upload
        dict(delete_local_checkpoint_after_upload=True),
        dict(checkpoint_upload_mode=CheckpointUploadMode.ASYNC),
    ),
    ids=["delete_local_checkpoint_after_upload=True", "checkpoint_upload_mode=ASYNC"],
)
def test_report_checkpoint_delete_storage_path(kwarg, tmp_path):
    """Test that the trainer raises an error if the Checkpoint path is contains the storage_path."""
    # Test in `tmp_path` in case the test fails which means that the tmp_path.parent might be deleted
    base_dir = tmp_path / "test_base"
    storage_dir = base_dir / "storage"
    os.makedirs(storage_dir, exist_ok=True)

    def train_fn_equal_storage_path():
        ray.train.report(
            {},
            Checkpoint(str(storage_dir)),
            **kwarg,
        )

    def train_fn_within_storage_path():
        ray.train.report(
            {},
            Checkpoint(str(base_dir)),
            **kwarg,
        )

    for train_fn in [train_fn_equal_storage_path, train_fn_within_storage_path]:
        trainer = DataParallelTrainer(
            train_fn, run_config=RunConfig(storage_path=str(storage_dir))
        )
        with pytest.raises(WorkerGroupError, match="error") as exc_info:
            trainer.fit()
        assert isinstance(exc_info.value.worker_failures[0], ValueError)
        assert (
            exc_info.value.worker_failures[0]
            .args[0]
            .startswith("Ray Train's experiment directory")
        )
        # full error message:
        #   Ray Train's experiment directory (<file path>) is contained within the checkpoint path (<file path>)
        #   and `ray.train.report(delete_local_checkpoint_after_upload=True)`.
        #   As a result, this would delete the experiment directory.
        #   Please write the checkpoint to a subdirectory of the experiment directory
        #   or use `delete_local_checkpoint_after_upload=False`.


@pytest.mark.parametrize(
    "kwarg",
    (
        # both of these cases can cause the local checkpoint to be deleted after upload
        dict(delete_local_checkpoint_after_upload=True),
        dict(checkpoint_upload_mode=CheckpointUploadMode.ASYNC),
    ),
    ids=["delete_local_checkpoint_after_upload=True", "checkpoint_upload_mode=ASYNC"],
)
def test_report_checkpoint_delete_s3_storage_path(kwarg):
    """Test that the trainer raises an error if a s3 checkpoint path is contains a s3 storage_path."""
    port, region = 5002, "us-west-2"
    with simulate_s3_bucket(port=port, region=region) as s3_uri:
        import boto3

        s3 = boto3.client(
            "s3", region_name=region, endpoint_url=f"http://localhost:{port}"
        )
        # Bucket name will be autogenerated/unique per test
        bucket_name = URI(s3_uri).name
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
        # Use URI(s3_uri) / "storage" to correctly insert the path before query params.
        s3_storage_path = str(URI(s3_uri) / "storage")

        def train_fn_equal_storage_path():
            ray.train.report(
                {},
                Checkpoint(s3_storage_path),
                **kwarg,
            )

        def train_fn_within_storage_path():
            # s3_uri is the bucket root, which is a parent of s3_storage_path.
            ray.train.report(
                {},
                Checkpoint(s3_uri),
                **kwarg,
            )

        for train_fn in [train_fn_equal_storage_path, train_fn_within_storage_path]:
            trainer = DataParallelTrainer(
                train_fn, run_config=RunConfig(storage_path=s3_storage_path)
            )
            with pytest.raises(WorkerGroupError, match="error") as exc_info:
                trainer.fit()
            assert isinstance(exc_info.value.worker_failures[0], ValueError)
            assert (
                exc_info.value.worker_failures[0]
                .args[0]
                .startswith("Ray Train's experiment directory")
            )
            # full error message:
            #   Ray Train's experiment directory (<file path>) is contained within the checkpoint path (<file path>)
            #   and `ray.train.report(delete_local_checkpoint_after_upload=True)`.
            #   As a result, this would delete the experiment directory.
            #   Please write the checkpoint to a subdirectory of the experiment directory
            #   or use `delete_local_checkpoint_after_upload=False`.


def test_report_validation_without_validation_fn():
    def train_fn():
        with create_dict_checkpoint({}) as checkpoint:
            ray.train.report(metrics={}, checkpoint=checkpoint, validation=True)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=1),
    )
    with pytest.raises(
        WorkerGroupError,
        match="`validation_config` was not set on the trainer, but a validation was requested.",
    ) as exc_info:
        trainer.fit()
    assert isinstance(exc_info.value.worker_failures[0], ValueError)


def test_report_validation_without_checkpoint():
    def train_fn():
        ray.train.report(metrics={}, validation=True)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=1),
    )
    with pytest.raises(
        WorkerGroupError, match="Validation requires a checkpoint to be provided."
    ) as exc_info:
        trainer.fit()
    assert isinstance(exc_info.value.worker_failures[0], ValueError)


def test_report_validation_fn_keeps_correct_checkpoints(tmp_path):
    def validation_fn(checkpoint, new_score=None):
        if new_score:
            return {"score": new_score}
        else:
            return {}

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        checkpoint_dir = os.path.join(
            tmp_path,
            "my_checkpoint_dir",
        )
        os.makedirs(checkpoint_dir, exist_ok=True)
        with open(os.path.join(checkpoint_dir, f"shard_{rank}"), "wb") as f:
            ray_pickle.dump("some_checkpoint_contents", f)
        ray.train.report(
            metrics={"score": 1},
            checkpoint=Checkpoint(checkpoint_dir),
            checkpoint_upload_mode=CheckpointUploadMode.ASYNC,
            delete_local_checkpoint_after_upload=False,
            validation=ValidationTaskConfig(fn_kwargs={}),
        )
        with create_dict_checkpoint({}) as cp2:
            ray.train.report(
                metrics={"score": 3},
                checkpoint=cp2,
                checkpoint_upload_mode=CheckpointUploadMode.SYNC,
                validation=True,
            )
        with create_dict_checkpoint({}) as cp3:
            ray.train.report(
                metrics={"score": 2},
                checkpoint=cp3,
                checkpoint_upload_mode=CheckpointUploadMode.SYNC,
                validation=ValidationTaskConfig(fn_kwargs={"new_score": 5}),
            )

    trainer = DataParallelTrainer(
        train_fn,
        validation_config=ValidationConfig(fn=validation_fn),
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            checkpoint_config=CheckpointConfig(
                num_to_keep=2, checkpoint_score_attribute="score"
            ),
        ),
    )
    result = trainer.fit()
    assert result.error is None
    assert result.checkpoint == result.best_checkpoints[1][0]
    assert len(result.best_checkpoints) == 2
    assert result.best_checkpoints[0][1] == {"score": 3}
    assert result.best_checkpoints[1][1] == {"score": 5}


def test_report_validation_fn_overrides_default_kwargs(tmp_path):
    def validation_fn(checkpoint, validation_score, other_key):
        return {"validation_score": validation_score, "other_key": other_key}

    def train_fn():
        with create_dict_checkpoint({}) as cp:
            ray.train.report(
                metrics={},
                checkpoint=cp,
                validation=ValidationTaskConfig(fn_kwargs={"validation_score": 2}),
            )

    trainer = DataParallelTrainer(
        train_fn,
        validation_config=ValidationConfig(
            fn=validation_fn,
            task_config=ValidationTaskConfig(
                fn_kwargs={"validation_score": 1, "other_key": "other_value"}
            ),
        ),
        run_config=RunConfig(storage_path=str(tmp_path)),
        scaling_config=ScalingConfig(num_workers=1),
    )
    result = trainer.fit()
    assert result.best_checkpoints[0][1] == {
        "validation_score": 2,
        "other_key": "other_value",
    }


def test_report_validation_fn_error(tmp_path):
    def validation_fn(checkpoint, rank=None, iteration=None):
        if rank == 0 and iteration == 0:
            raise ValueError("validation failed")
        return {}

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        with create_dict_checkpoint({}) as cp1:
            ray.train.report(
                metrics={},
                checkpoint=cp1,
                validation=ValidationTaskConfig(
                    fn_kwargs={"rank": rank, "iteration": 0}
                ),
            )
        with create_dict_checkpoint({}) as cp2:
            ray.train.report(
                metrics={},
                checkpoint=cp2,
                validation=ValidationTaskConfig(
                    fn_kwargs={"rank": rank, "iteration": 1}
                ),
            )

    trainer = DataParallelTrainer(
        train_fn,
        validation_config=ValidationConfig(fn=validation_fn),
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert result.error is None
    assert result.checkpoint == result.best_checkpoints[1][0]
    assert len(result.best_checkpoints) == 2


def test_report_validation_fn_success_after_retry():
    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

    counter = Counter.remote()

    def validation_fn(checkpoint):
        if ray.get(counter.increment.remote()) < 2:
            raise ValueError("validation failed")
        return {"score": 100}

    def train_fn():
        with create_dict_checkpoint({}) as cp:
            ray.train.report(
                metrics={},
                checkpoint=cp,
                validation=True,
            )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=1),
        validation_config=ValidationConfig(
            fn=validation_fn,
            ray_remote_kwargs={"max_retries": 1, "retry_exceptions": [ValueError]},
        ),
    )
    result = trainer.fit()
    assert result.best_checkpoints[0][1] == {"score": 100}


def _run_first_trainer_for_resumption(storage_path, validation_task_config):
    """Subprocess target: run a trainer with a stalling validation, then get SIGINT'd."""
    # Lives outside the test because multiprocessing cannot pickle nested functions.
    ray.init(address="auto")

    def validation_fn_stall(checkpoint, score):
        signal_actor = ray.get_actor(
            "validation_resumption_signal", namespace="test_validation_resumption"
        )
        ray.get(signal_actor.send.remote())
        while True:
            time.sleep(1)

    def train_fn():
        with create_dict_checkpoint({}) as cp:
            ray.train.report(
                metrics={},
                checkpoint=cp,
                validation=validation_task_config,
            )

    trainer = DataParallelTrainer(
        train_fn,
        validation_config=ValidationConfig(
            fn=validation_fn_stall,
            task_config=ValidationTaskConfig(fn_kwargs={"score": 1}),
        ),
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            name="validation_fn_resumption", storage_path=storage_path
        ),
    )
    trainer.fit()


@pytest.mark.parametrize(
    "validation_task_config, expected_score",
    [
        (True, 1),
        (ValidationTaskConfig(fn_kwargs={"score": 2}), 2),
    ],
)
def test_report_validation_fn_resumption(
    tmp_path, validation_task_config, expected_score
):
    """A train_func call a validation_fn that stalls and the trainer is cancelled.
    Does the resumed trainer restart the validation?"""
    signal_actor = (
        create_remote_signal_actor(ray)
        .options(
            name="validation_resumption_signal",
            namespace="test_validation_resumption",
        )
        .remote()
    )

    multiprocessing.set_start_method("spawn", force=True)
    process = multiprocessing.Process(
        target=_run_first_trainer_for_resumption,
        args=(str(tmp_path), validation_task_config),
    )
    process.start()

    # Wait for validation to start, then SIGINT the trainer process.
    ray.get(signal_actor.wait.remote())
    os.kill(process.pid, signal.SIGINT)
    process.join()

    def validation_fn_finish(checkpoint, score):
        return {"score": score}

    def train_fn_second():
        rc = ray.train.get_all_reported_checkpoints(
            consistency_mode=CheckpointConsistencyMode.VALIDATED
        )
        assert len(rc) == 1
        assert rc[0].status == ReportedCheckpointStatus.VALIDATED
        assert rc[0].metrics == {"score": expected_score}

    # Run second trainer that should finish interrupted validations.
    trainer = DataParallelTrainer(
        train_fn_second,
        validation_config=ValidationConfig(
            fn=validation_fn_finish,
            task_config=ValidationTaskConfig(fn_kwargs={"score": 1}),
        ),
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(
            name="validation_fn_resumption", storage_path=str(tmp_path)
        ),
    )
    result = trainer.fit()
    assert result.metrics == {"score": expected_score}


@pytest.mark.parametrize(
    "validation_task_config, expected_score",
    [
        (True, 1),
        (ValidationTaskConfig(fn_kwargs={"score": 2}), 2),
    ],
)
def test_report_validation_fn_resumption_on_train_fn_error(
    tmp_path, validation_task_config, expected_score
):
    """Train run where train_fn fails after reporting a checkpoint with pending validation.
    The validation only returns after train_fn signals failure. before_controller_shutdown
    drains the validation, persisting the validated metrics. The second run sees them."""
    signal_actor = create_remote_signal_actor(ray).remote()

    def validation_fn(checkpoint, score):
        # Block until train_fn has signaled and sleep to ensure that the train_func has closed.
        ray.get(signal_actor.wait.remote())
        time.sleep(2)
        return {"score": score}

    def train_fn_first():
        with create_dict_checkpoint({}) as cp:
            ray.train.report(
                metrics={},
                checkpoint=cp,
                validation=validation_task_config,
            )
        try:
            raise RuntimeError("train_fn failed intentionally")
        finally:
            signal_actor.send.remote()

    def train_fn_second():
        rc = ray.train.get_all_reported_checkpoints(
            consistency_mode=CheckpointConsistencyMode.VALIDATED
        )
        assert len(rc) == 1
        assert rc[0].status == ReportedCheckpointStatus.VALIDATED
        assert rc[0].metrics == {"score": expected_score}

    run_config = RunConfig(
        name="validation_fn_resumption_on_train_fn_error",
        storage_path=str(tmp_path),
    )
    validation_config = ValidationConfig(
        fn=validation_fn,
        task_config=ValidationTaskConfig(fn_kwargs={"score": 1}),
    )

    with pytest.raises(WorkerGroupError):
        DataParallelTrainer(
            train_fn_first,
            validation_config=validation_config,
            run_config=run_config,
        ).fit()

    result = DataParallelTrainer(
        train_fn_second,
        validation_config=validation_config,
        run_config=run_config,
    ).fit()
    assert result.metrics == {"score": expected_score}


def test_report_validation_fn_resumption_checkpoint_status(tmp_path):
    """When a train_func does it remember all previous validation_fn status and metrics."""

    def validation_fn(checkpoint, score):
        return {"score": score}

    def train_fn_first():
        with create_dict_checkpoint({}) as cp:
            ray.train.report(
                metrics={"score": 1},
                checkpoint=cp,
                validation=False,
            )

        with create_dict_checkpoint({}) as cp:
            ray.train.report(
                metrics={},
                checkpoint=cp,
                validation=ValidationTaskConfig(fn_kwargs={"score": 2}),
            )

        raise RuntimeError("train_fn failed intentionally")

    def train_fn_second():
        rc = ray.train.get_all_reported_checkpoints(
            consistency_mode=CheckpointConsistencyMode.VALIDATED
        )
        assert len(rc) == 2
        assert rc[0].status == ReportedCheckpointStatus.COMMITTED
        assert rc[0].metrics == {"score": 1}
        assert rc[1].status == ReportedCheckpointStatus.VALIDATED
        assert rc[1].metrics == {"score": 2}

        with create_dict_checkpoint({}) as cp:
            ray.train.report(
                metrics={},
                checkpoint=cp,
                validation=ValidationTaskConfig(fn_kwargs={"score": 3}),
            )

        rc = ray.train.get_all_reported_checkpoints(
            consistency_mode=CheckpointConsistencyMode.VALIDATED
        )
        assert len(rc) == 3
        assert rc[2].status == ReportedCheckpointStatus.VALIDATED
        assert rc[2].metrics == {"score": 3}

    run_config = RunConfig(
        name="validation_fn_resumption_checkpoint_status",
        storage_path=str(tmp_path),
    )

    with pytest.raises(WorkerGroupError):
        DataParallelTrainer(
            train_fn_first,
            validation_config=ValidationConfig(fn=validation_fn),
            run_config=run_config,
        ).fit()

    result = DataParallelTrainer(
        train_fn_second,
        validation_config=ValidationConfig(fn=validation_fn),
        run_config=run_config,
    ).fit()
    assert result.metrics == {"score": 3}


def test_report_checkpoint_upload_fn(tmp_path):
    def checkpoint_upload_fn(checkpoint, checkpoint_dir_name):
        full_checkpoint_path = (
            ray.train.get_context()
            .get_storage()
            .build_checkpoint_path_from_name(checkpoint_dir_name)
        )
        shutil.copytree(checkpoint.path, full_checkpoint_path)
        return Checkpoint.from_directory(full_checkpoint_path)

    def train_fn():
        if ray.train.get_context().get_world_rank() == 0:
            with create_dict_checkpoint(
                {"checkpoint_key": "checkpoint_value"}
            ) as checkpoint:
                ray.train.report(
                    metrics={},
                    checkpoint=checkpoint,
                    checkpoint_dir_name="my_checkpoint_dir_name",
                    checkpoint_upload_fn=checkpoint_upload_fn,
                )
        else:
            ray.train.report(metrics={}, checkpoint=None)

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    result = trainer.fit()
    assert load_dict_checkpoint(result.checkpoint) == {
        "checkpoint_key": "checkpoint_value"
    }


def test_checkpoint_upload_fn_returns_checkpoint(tmp_path):
    def train_fn():
        with create_dict_checkpoint({}) as checkpoint:
            ray.train.report(
                metrics={},
                checkpoint=checkpoint,
                checkpoint_upload_fn=lambda x, y: None,
            )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=1),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    with pytest.raises(
        WorkerGroupError,
        match="checkpoint_upload_fn must return a `ray.train.Checkpoint`",
    ):
        trainer.fit()


def test_report_get_all_reported_checkpoints(tmp_path):
    """Check that get_all_reported_checkpoints returns checkpoints depending on # report calls."""

    def train_fn():
        if ray.train.get_context().get_world_rank() == 0:
            ray.train.report(metrics={}, checkpoint=None)
            with create_dict_checkpoint({}) as checkpoint:
                ray.train.report(metrics={}, checkpoint=checkpoint)

            reported_checkpoints = ray.train.get_all_reported_checkpoints()
            assert len(reported_checkpoints) == 1
            assert reported_checkpoints[0].status == ReportedCheckpointStatus.COMMITTED

            with create_dict_checkpoint({}) as checkpoint:
                ray.train.report(metrics={}, checkpoint=checkpoint)
        else:
            ray.train.report(metrics={}, checkpoint=None)
            ray.train.report(metrics={}, checkpoint=None)
            ray.train.report(metrics={}, checkpoint=None)

            reported_checkpoints = ray.train.get_all_reported_checkpoints()
            assert len(reported_checkpoints) == 2
            assert all(
                rc.status == ReportedCheckpointStatus.COMMITTED
                for rc in reported_checkpoints
            )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    trainer.fit()


def test_get_all_reported_checkpoints_all_consistency_modes(tmp_path):
    signal_actor = create_remote_signal_actor(ray).remote()

    def validation_fn(checkpoint, validation_score):
        ray.get(signal_actor.wait.remote())
        return {
            "validation_score": validation_score,
        }

    def train_fn(config):
        signal_actor = config["signal_actor"]

        if ray.train.get_context().get_world_rank() == 0:
            # Assert that we get committed checkpoints
            with create_dict_checkpoint({}) as cp1:
                ray.train.report(
                    metrics={"training_score": 1},
                    checkpoint=cp1,
                    validation=True,
                )
            reported_checkpoints = ray.train.get_all_reported_checkpoints(
                consistency_mode=CheckpointConsistencyMode.COMMITTED
            )
            assert len(reported_checkpoints) == 1
            assert (
                reported_checkpoints[0].status
                == ReportedCheckpointStatus.PENDING_VALIDATION
            )
            assert reported_checkpoints[0].metrics == {"training_score": 1}

            # Assert that we get validated checkpoints
            signal_actor.send.remote()
            reported_checkpoints = ray.train.get_all_reported_checkpoints(
                consistency_mode=CheckpointConsistencyMode.VALIDATED
            )
            assert len(reported_checkpoints) == 1
            assert reported_checkpoints[0].status == ReportedCheckpointStatus.VALIDATED
            assert reported_checkpoints[0].metrics == {
                "training_score": 1,
                "validation_score": 100,
            }
        else:
            ray.train.report(metrics={}, checkpoint=None)

    trainer = DataParallelTrainer(
        train_fn,
        validation_config=ValidationConfig(
            fn=validation_fn,
            task_config=ValidationTaskConfig(fn_kwargs={"validation_score": 100}),
        ),
        scaling_config=ScalingConfig(num_workers=2),
        train_loop_config={"signal_actor": signal_actor},
        run_config=RunConfig(storage_path=str(tmp_path)),
    )
    trainer.fit()


def test_get_all_reported_checkpoints_empty_reports():
    def train_fn():
        ray.train.report(metrics={}, checkpoint=None)
        assert len(ray.train.get_all_reported_checkpoints()) == 0

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
