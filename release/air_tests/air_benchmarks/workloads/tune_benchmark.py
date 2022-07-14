import json
import os

import ray
from ray.train.torch import TorchTrainer

from benchmark_util import time_it

CONFIG = {"lr": 1e-3, "batch_size": 64, "epochs": 20}


def prepare_mnist():
    # Pre-download the data onto each node.
    from benchmark_util import upload_file_to_all_nodes, run_command_on_all_nodes

    ray.init("auto")
    print("Preparing Torch benchmark: Downloading MNIST")

    path = os.path.abspath("workloads/_torch_prepare.py")
    upload_file_to_all_nodes(path)
    run_command_on_all_nodes(["python", path])
    ray.shutdown()


def get_trainer():
    """Get the trainer to be used across train and tune to ensure consistency."""
    from torch_benchmark import train_func

    def train_loop(config):
        train_func(use_ray=True, config=config)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config=CONFIG,
        scaling_config={
            "num_workers": 4,
            "resources_per_worker": {"CPU": 2},
            "trainer_resources": {"CPU": 0},
            "use_gpu": False,
        },
    )
    return trainer


@time_it
def train_torch():
    trainer = get_trainer()
    trainer.fit()


@time_it
def tune_torch():
    """Making sure that tuning multiple trials in parallel is not
    taking significantly longer than training each one individually.

    Some overhead is expected.
    """

    from ray import tune
    from ray.tune.tuner import Tuner
    from ray.tune.tune_config import TuneConfig

    param_space = {
        "train_loop_config": {
            "lr": tune.loguniform(1e-4, 1e-1),
        },
    }

    trainer = get_trainer()
    tuner = Tuner(
        trainable=trainer,
        param_space=param_space,
        tune_config=TuneConfig(mode="min", metric="loss", num_samples=8),
    )
    tuner.fit()


def main():
    train_time = train_torch()
    tune_time = tune_torch()
    result = {"train_time": train_time, "tune_time": tune_time}
    print("Results:", result)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main()
