import os
import shutil
from unittest.mock import create_autospec

import pytest

import ray
import ray.cloudpickle as ray_pickle
from ray.air.config import CheckpointConfig
from ray.train import Checkpoint, RunConfig, ScalingConfig
from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.exceptions import WorkerGroupError
from ray.train.v2.api.report_config import CheckpointUploadMode


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
    with pytest.raises(WorkerGroupError) as exc_info:
        trainer.fit()
    assert isinstance(exc_info.value.worker_failures[0]._base_exc, ValueError)


def test_report_validate_config_without_validate_fn():
    def train_fn():
        ray.train.report(metrics={}, checkpoint=None, validate_config={"test": "test"})

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
    )
    with pytest.raises(WorkerGroupError) as exc_info:
        trainer.fit()
    assert isinstance(exc_info.value.worker_failures[0]._base_exc, ValueError)


def test_report_validate_fn_keeps_correct_checkpoints(tmp_path):
    def validate_fn(checkpoint, config):
        if config and "new_score" in config:
            return {"score": config["new_score"]}
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
            validate_fn=validate_fn,
            validate_config=None,
        )
        with create_dict_checkpoint({}) as cp2:
            ray.train.report(
                metrics={"score": 3},
                checkpoint=cp2,
                checkpoint_upload_mode=CheckpointUploadMode.SYNC,
                validate_fn=validate_fn,
                validate_config=None,
            )
        with create_dict_checkpoint({}) as cp3:
            ray.train.report(
                metrics={"score": 2},
                checkpoint=cp3,
                checkpoint_upload_mode=CheckpointUploadMode.SYNC,
                validate_fn=validate_fn,
                validate_config={"new_score": 5},
            )

    trainer = DataParallelTrainer(
        train_fn,
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


def test_report_validate_fn_error():
    def validate_fn(checkpoint, config):
        if config["rank"] == 0 and config["iteration"] == 0:
            raise ValueError("validation failed")
        return {}

    def train_fn():
        rank = ray.train.get_context().get_world_rank()
        with create_dict_checkpoint({}) as cp1:
            ray.train.report(
                metrics={},
                checkpoint=cp1,
                validate_fn=validate_fn,
                validate_config={"rank": rank, "iteration": 0},
            )
        with create_dict_checkpoint({}) as cp2:
            ray.train.report(
                metrics={},
                checkpoint=cp2,
                validate_fn=validate_fn,
                validate_config={"rank": rank, "iteration": 1},
            )

    trainer = DataParallelTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2),
    )
    result = trainer.fit()
    assert result.error is None
    assert result.checkpoint == result.best_checkpoints[1][0]
    assert len(result.best_checkpoints) == 2


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
