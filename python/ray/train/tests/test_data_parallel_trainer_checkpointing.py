import pytest

import ray
from ray import train
from ray.train import CheckpointConfig, RunConfig, ScalingConfig
from ray.train.data_parallel_trainer import DataParallelTrainer

from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


scale_config = ScalingConfig(num_workers=2)
NUM_EPOCHS = 3


def checkpoint_train_func():
    for i in range(NUM_EPOCHS):
        with create_dict_checkpoint({"epoch": i}) as checkpoint:
            train.report({"epoch": i}, checkpoint=checkpoint)


def test_checkpoint(ray_start_4_cpus):
    """Test that a checkpoint is created and accessible."""
    trainer = DataParallelTrainer(
        train_loop_per_worker=checkpoint_train_func,
        scaling_config=scale_config,
    )
    result = trainer.fit()
    assert load_dict_checkpoint(result.checkpoint)["epoch"] == NUM_EPOCHS - 1


def test_resume_from_checkpoint(ray_start_4_cpus, tmpdir):
    """Test that training can be resumed from a reported checkpoint."""

    def train_func():
        checkpoint = train.get_checkpoint()
        if checkpoint:
            epoch = load_dict_checkpoint(checkpoint)["epoch"]
        else:
            epoch = 0
        for i in range(epoch, epoch + 2):
            with create_dict_checkpoint({"epoch": i}) as checkpoint:
                train.report({"epoch": i}, checkpoint=checkpoint)

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func, scaling_config=scale_config
    )
    result = trainer.fit()
    assert load_dict_checkpoint(result.checkpoint)["epoch"] == 1

    trainer = DataParallelTrainer(
        train_loop_per_worker=train_func,
        scaling_config=scale_config,
        resume_from_checkpoint=result.checkpoint,
    )
    result = trainer.fit()
    assert load_dict_checkpoint(result.checkpoint)["epoch"] == 2


@pytest.mark.parametrize("mode", ["min", "max"])
def test_checkpoints_to_keep(ray_start_4_cpus, mode):
    """
    Test that ``CheckpointConfig`` is respected.

    - Report 4 times with different metrics.
    - Assert that the kept checkpoints match the expectation.
    """

    def train_func():
        with create_dict_checkpoint({"idx": 0}) as checkpoint:
            train.report(dict(loss=float("nan")), checkpoint=checkpoint)  # nan, deleted
        with create_dict_checkpoint({"idx": 1}) as checkpoint:
            train.report(
                dict(loss=3), checkpoint=checkpoint
            )  # best for min, worst for max (del)
        with create_dict_checkpoint({"idx": 2}) as checkpoint:
            train.report(
                dict(loss=7), checkpoint=checkpoint
            )  # worst for min (del), best for max
        with create_dict_checkpoint({"idx": 3}) as checkpoint:
            train.report(dict(loss=5), checkpoint=checkpoint)

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
    assert load_dict_checkpoint(result.checkpoint)["idx"] == 3

    if mode == "min":
        indices = [3, 1]
        losses = [5, 3]
    else:
        indices = [3, 2]
        losses = [5, 7]

    assert load_dict_checkpoint(result.best_checkpoints[0][0])["idx"] == indices[0]
    assert load_dict_checkpoint(result.best_checkpoints[1][0])["idx"] == indices[1]
    assert result.best_checkpoints[0][1]["loss"] == losses[0]
    assert result.best_checkpoints[1][1]["loss"] == losses[1]


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
