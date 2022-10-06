import pytest

import ray
from ray.air import session, Checkpoint
from ray.air.config import ScalingConfig, CheckpointConfig, RunConfig
from ray.train.data_parallel_trainer import DataParallelTrainer


scale_config = ScalingConfig(num_workers=2)


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


def test_checkpoint_config(ray_start_4_cpus):
    def train_func():
        session.report(
            dict(loss=float("nan")), checkpoint=Checkpoint.from_dict({"idx": 0})
        )  # nan, deleted
        session.report(
            dict(loss=3), checkpoint=Checkpoint.from_dict({"idx": 1})
        )  # best
        session.report(
            dict(loss=7), checkpoint=Checkpoint.from_dict({"idx": 2})
        )  # worst, deleted
        session.report(dict(loss=5), checkpoint=Checkpoint.from_dict({"idx": 3}))

    checkpoint_config = CheckpointConfig(
        num_to_keep=2, checkpoint_score_attribute="loss", checkpoint_score_order="min"
    )

    trainer = DataParallelTrainer(
        train_func,
        scaling_config=scale_config,
        run_config=RunConfig(checkpoint_config=checkpoint_config),
    )
    results = trainer.fit()
    assert results.checkpoint.to_dict()["idx"] == 3
    assert len(results.best_checkpoints) == 2
    assert results.best_checkpoints[0][0].to_dict()["idx"] == 3
    assert results.best_checkpoints[1][0].to_dict()["idx"] == 1
    assert results.best_checkpoints[0][1]["loss"] == 5
    assert results.best_checkpoints[1][1]["loss"] == 3


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
