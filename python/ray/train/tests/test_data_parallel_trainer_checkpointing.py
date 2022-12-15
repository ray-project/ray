import os
from pathlib import Path
from unittest.mock import patch
import pytest

import ray
from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train.constants import (
    COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV,
    DISABLE_LAZY_CHECKPOINTING_ENV,
)
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.air.config import CheckpointConfig, RunConfig, ScalingConfig


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


scale_config = ScalingConfig(num_workers=2)


def get_checkpoint_train_func(checkpoint_type):
    def checkpoint_train_func():
        for i in range(3):
            checkpoint = Checkpoint.from_dict({"model": i})
            path = None
            if checkpoint_type != "dict":
                checkpoint = Checkpoint.from_directory(checkpoint.to_directory())
                path = checkpoint._local_path
            session.report({"epoch": i, "path": path}, checkpoint=checkpoint)

    return checkpoint_train_func


checkpoint_type_and_should_copy = (
    ("dict", True),
    ("dir", True),
    ("lazy_dir", True),
    ("dir", False),
    ("lazy_dir", False),
)


@pytest.mark.parametrize(
    "checkpoint_type_and_should_copy", checkpoint_type_and_should_copy
)
def test_checkpoint(ray_start_4_cpus, checkpoint_type_and_should_copy):
    """
    Test that a checkpoint is created and accessible.

    - Assert that the data from the returned checkpoint has an expected state.
    - Assert that the directory was moved/copied depending on
      ``checkpoint_type_and_should_copy``.
    """
    checkpoint_type, should_copy = checkpoint_type_and_should_copy
    with patch.dict(
        os.environ,
        {
            DISABLE_LAZY_CHECKPOINTING_ENV: str(int(checkpoint_type != "lazy_dir")),
            COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV: str(int(should_copy)),
        },
    ):
        trainer = DataParallelTrainer(
            train_loop_per_worker=get_checkpoint_train_func(checkpoint_type),
            scaling_config=scale_config,
        )
        result = trainer.fit()
    assert result.checkpoint.to_dict()["model"] == 2

    path = result.metrics["path"]
    if path:
        if should_copy:
            assert list(Path(path).glob("*"))
        else:
            assert not list(Path(path).glob("*"))


@pytest.mark.parametrize(
    "checkpoint_type_and_should_copy", checkpoint_type_and_should_copy
)
def test_preprocessor_in_checkpoint(ray_start_4_cpus, checkpoint_type_and_should_copy):
    """
    Test that a checkpoint with a preprocessor is created and accessible.

    - Assert that the data from the returned checkpoint has an expected state.
    - Assert that the preprocessor keeps its state.
    - Assert that the directory was moved/copied depending on
      ``checkpoint_type_and_should_copy``.
    """
    checkpoint_type, should_copy = checkpoint_type_and_should_copy

    class DummyPreprocessor(Preprocessor):
        def __init__(self):
            super().__init__()
            self.is_same = True

    with patch.dict(
        os.environ,
        {
            DISABLE_LAZY_CHECKPOINTING_ENV: str(int(checkpoint_type != "lazy_dir")),
            COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV: str(int(should_copy)),
        },
    ):
        trainer = DataParallelTrainer(
            train_loop_per_worker=get_checkpoint_train_func(checkpoint_type),
            scaling_config=scale_config,
            preprocessor=DummyPreprocessor(),
        )
        result = trainer.fit()
    assert result.checkpoint.to_dict()["model"] == 2
    assert result.checkpoint.get_preprocessor().is_same

    path = result.metrics["path"]
    if path:
        if should_copy:
            assert list(Path(path).glob("*"))
        else:
            assert not list(Path(path).glob("*"))


def test_resume_from_checkpoint(ray_start_4_cpus, tmpdir):
    """
    Test that training can be resumed from a reported checkpoint.

    - Assert that the data from the returned checkpoint has an expected state.
    - Move the checkpoint to memory and then back to disk to test ser/deser and ensure
      that a different directory can be used.
    - Restart training from checkpoint and assert that hhe returned checkpoint
      has an expected state.
    """

    def train_func():
        checkpoint = session.get_checkpoint()
        if checkpoint:
            epoch = checkpoint.to_dict()["epoch"]
        else:
            epoch = 0
        for i in range(epoch, epoch + 2):
            session.report({"epoch": i}, checkpoint=Checkpoint.from_dict({"epoch": i}))

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["epoch"] == 1

    # Move checkpoint to a different directory.
    checkpoint_dict = result.checkpoint.to_dict()
    checkpoint = Checkpoint.from_dict(checkpoint_dict)
    checkpoint_path = checkpoint.to_directory(tmpdir)
    resume_from = Checkpoint.from_directory(checkpoint_path)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        resume_from_checkpoint=resume_from,
    )
    result = trainer.fit()
    assert result.checkpoint.to_dict()["epoch"] == 2


@pytest.mark.parametrize("mode", ["min", "max"])
def test_checkpoints_to_keep(ray_start_4_cpus, mode):
    """
    Test that ``CheckpointConfig`` is respected.

    - Report 4 times with different metrics.
    - Assert that the kept checkpoints match the expectation.
    """

    def train_func():
        session.report(
            dict(loss=float("nan")), checkpoint=Checkpoint.from_dict({"idx": 0})
        )  # nan, deleted
        session.report(
            dict(loss=3), checkpoint=Checkpoint.from_dict({"idx": 1})
        )  # best for min, worst for max (del)
        session.report(
            dict(loss=7), checkpoint=Checkpoint.from_dict({"idx": 2})
        )  # worst for min (del), best for max
        session.report(dict(loss=5), checkpoint=Checkpoint.from_dict({"idx": 3}))

    checkpoint_config = CheckpointConfig(
        num_to_keep=2, checkpoint_score_attribute="loss", checkpoint_score_order=mode
    )

    trainer = DataParallelTrainer(
        train_func,
        scaling_config=scale_config,
        run_config=RunConfig(checkpoint_config=checkpoint_config),
    )
    result = trainer.fit()
    assert len(result.best_checkpoints) == 2

    # Last checkpoint
    assert result.checkpoint.to_dict()["idx"] == 3

    if mode == "min":
        indices = [3, 1]
        losses = [5, 3]
    else:
        indices = [3, 2]
        losses = [5, 7]

    assert result.best_checkpoints[0][0].to_dict()["idx"] == indices[0]
    assert result.best_checkpoints[1][0].to_dict()["idx"] == indices[1]
    assert result.best_checkpoints[0][1]["loss"] == losses[0]
    assert result.best_checkpoints[1][1]["loss"] == losses[1]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
