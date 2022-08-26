import pytest
from ray.air import session
from ray.air.checkpoint import Checkpoint
import torch

import ray
from ray.air.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.torch import TorchPredictor, TorchTrainer
from ray.tune import TuneError
from ray.air.config import ScalingConfig
from ray.train.torch import TorchConfig
import ray.train as train
from unittest.mock import patch
from ray.cluster_utils import Cluster


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_2_node_2_gpu():
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=4, num_gpus=2)

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.mark.parametrize("num_workers", [1, 2])
def test_torch_linear(ray_start_4_cpus, num_workers):
    def train_func(config):
        result = linear_train_func(config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    num_workers = num_workers
    epochs = 3
    scaling_config = ScalingConfig(num_workers=num_workers)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    trainer.fit()


def test_torch_e2e(ray_start_4_cpus):
    def train_func():
        model = torch.nn.Linear(1, 1)
        session.report({}, checkpoint=Checkpoint.from_dict(dict(model=model)))

    scaling_config = ScalingConfig(num_workers=2)
    trainer = TorchTrainer(
        train_loop_per_worker=train_func, scaling_config=scaling_config
    )
    result = trainer.fit()

    predict_dataset = ray.data.range(3)

    class TorchScorer:
        def __init__(self):
            self.pred = TorchPredictor.from_checkpoint(result.checkpoint)

        def __call__(self, x):
            return self.pred.predict(x, dtype=torch.float)

    predictions = predict_dataset.map_batches(
        TorchScorer, batch_format="pandas", compute="actors"
    )
    assert predictions.count() == 3


def test_torch_e2e_state_dict(ray_start_4_cpus):
    def train_func():
        model = torch.nn.Linear(1, 1).state_dict()
        session.report({}, checkpoint=Checkpoint.from_dict(dict(model=model)))

    scaling_config = ScalingConfig(num_workers=2)
    trainer = TorchTrainer(
        train_loop_per_worker=train_func, scaling_config=scaling_config
    )
    result = trainer.fit()

    # If loading from a state dict, a model definition must be passed in.
    with pytest.raises(ValueError):
        TorchPredictor.from_checkpoint(result.checkpoint)

    class TorchScorer:
        def __init__(self):
            self.pred = TorchPredictor.from_checkpoint(
                result.checkpoint, model=torch.nn.Linear(1, 1)
            )

        def __call__(self, x):
            return self.pred.predict(x, dtype=torch.float)

    predict_dataset = ray.data.range(3)
    predictions = predict_dataset.map_batches(
        TorchScorer, batch_format="pandas", compute="actors"
    )
    assert predictions.count() == 3


def test_checkpoint_freq(ray_start_4_cpus):
    # checkpoint_freq is not supported so raise an error
    trainer = TorchTrainer(
        train_loop_per_worker=lambda config: None,
        scaling_config=ray.air.ScalingConfig(num_workers=1),
        run_config=ray.air.RunConfig(
            checkpoint_config=ray.air.CheckpointConfig(
                checkpoint_frequency=2,
            ),
        ),
    )
    with pytest.raises(TuneError):
        trainer.fit()


@pytest.mark.parametrize(
    "num_gpus_per_worker, expected_local_rank", [(0.5, 0), (1, 0), (2, 0)]
)
def test_tune_torch_get_device_gpu(
    ray_2_node_2_gpu, num_gpus_per_worker, expected_local_rank
):
    """Tests if GPU ids are set correctly when running train concurrently in nested actors
    (for example when used with Tune).
    """
    from ray.air.config import ScalingConfig
    import time

    num_samples = int(2 // num_gpus_per_worker)
    num_workers = 2

    @patch("torch.cuda.is_available", lambda: True)
    def train_fn():
        # two workers are spread across two different nodes
        # the device ids are always set to 0,
        # for example, if `num_gpus_per_worker`` is 2,
        # then each worker will have `ray.get_gpu_ids() == [0, 1]`
        # and thus, the local rank is 0.
        assert train.torch.get_device().index == expected_local_rank

    @ray.remote
    class TrialActor:
        def __init__(self, warmup_steps):
            # adding warmup_steps to the config
            # to avoid the error of checkpoint name conflict
            time.sleep(2 * warmup_steps)
            self.trainer = TorchTrainer(
                train_fn,
                torch_config=TorchConfig(backend="gloo"),
                scaling_config=ScalingConfig(
                    num_workers=num_workers,
                    use_gpu=True,
                    resources_per_worker={"GPU": num_gpus_per_worker},
                    placement_strategy="SPREAD"
                    # Each gpu worker will be spread onto separate nodes.
                ),
            )

        def run(self):
            return self.trainer.fit()

    actors = [TrialActor.remote(_) for _ in range(num_samples)]
    ray.get([actor.run.remote() for actor in actors])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
