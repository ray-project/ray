# __reproduce_start__
import ray.train
from ray.train import FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer


def train_func():
    rank = ray.train.get_context().get_world_rank()
    # Training logic.
    ray.train.report({"rank": rank})


trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    run_config=RunConfig(failure_config=FailureConfig(max_failures=0)),
)
result = trainer.fit()
# __reproduce_end__

assert result.error is None
