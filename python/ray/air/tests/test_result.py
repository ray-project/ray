import torch
import pytest

import ray
from ray.air import CheckpointConfig, RunConfig, ScalingConfig, session
from ray.air.result import Result
from ray.train.torch import TorchCheckpoint, TorchTrainer
from ray.train.trainer import TrainingFailedError


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_result_restore(ray_start_4_cpus):
    NUM_CHECKPOINTS = 3
    NUM_ITERATIONS = 5

    def worker_loop():
        model = torch.nn.Linear(2, 3)
        for i in range(NUM_ITERATIONS):
            session.report(
                metrics={"metric_a": i, "metric_b": -i},
                checkpoint=TorchCheckpoint.from_model(model),
            )
        raise RuntimeError()

    trainer = TorchTrainer(
        train_loop_per_worker=worker_loop,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        run_config=RunConfig(
            checkpoint_config=CheckpointConfig(
                num_to_keep=NUM_CHECKPOINTS,
                checkpoint_score_attribute="metric_a",
                checkpoint_score_order="max",
            )
        ),
    )

    with pytest.raises(TrainingFailedError) as exc_info:
        result = trainer.fit()
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    local_path = result.path

    # Delete the in-memory result object, then restore it
    del result
    result = Result.from_path(local_path)

    # Check if we restored all checkpoints
    assert result.checkpoint
    assert len(result.best_checkpoints) == NUM_CHECKPOINTS

    # Check if the checkpoints bounded with correct metrics
    best_ckpt_a = result.get_best_checkpoint(metric="metric_a", mode="max")
    assert best_ckpt_a.id == NUM_ITERATIONS - 1
    assert best_ckpt_a.iteration == NUM_ITERATIONS

    best_ckpt_b = result.get_best_checkpoint(metric="metric_b", mode="max")
    assert best_ckpt_b.id == NUM_ITERATIONS - NUM_CHECKPOINTS
    assert best_ckpt_b.iteration == NUM_ITERATIONS - NUM_CHECKPOINTS + 1

    # Check if we properly restored errors
    assert isinstance(result.error, RuntimeError)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
