import json
import os
import time
import timeit
import click
import numpy as np

import ray
from ray.air import ScalingConfig
from ray.train.torch import TorchTrainer


CONFIG = {"lr": 1e-3, "batch_size": 64, "epochs": 20}


def prepare_mnist():
    # Pre-download the data onto each node.
    from benchmark_util import schedule_remote_fn_on_all_nodes

    print("Preparing Torch benchmark: Downloading MNIST")

    @ray.remote
    def _download_data():
        import torchvision

        torchvision.datasets.FashionMNIST("/tmp/data_fashion_mnist", download=True)
        return True

    ray.get(schedule_remote_fn_on_all_nodes(_download_data))


def get_trainer(num_workers: int = 4, use_gpu: bool = False):
    """Get the trainer to be used across train and tune to ensure consistency."""
    from torch_benchmark import train_func

    def train_loop(config):
        train_func(use_ray=True, config=config)

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop,
        train_loop_config=CONFIG,
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            resources_per_worker={"CPU": 2},
            trainer_resources={"CPU": 0},
            use_gpu=use_gpu,
        ),
    )
    return trainer


def train_torch(num_workers: int, use_gpu: bool = False):
    trainer = get_trainer(num_workers=num_workers, use_gpu=use_gpu)
    trainer.fit()


def tune_torch(num_workers: int = 4, num_trials: int = 8, use_gpu: bool = False):
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

    trainer = get_trainer(num_workers=num_workers, use_gpu=use_gpu)
    tuner = Tuner(
        trainable=trainer,
        param_space=param_space,
        tune_config=TuneConfig(mode="min", metric="loss", num_samples=num_trials),
    )
    tuner.fit()


@click.command(help="Run Benchmark comparing Train to Tune.")
@click.option("--num-runs", type=int, default=1)
@click.option("--num-trials", type=int, default=8)
@click.option("--num-workers", type=int, default=4)
@click.option("--use-gpu", is_flag=True)
def main(
    num_runs: int = 1, num_trials: int = 8, num_workers: int = 4, use_gpu: bool = False
):
    ray.init(
        runtime_env={
            "working_dir": os.path.dirname(__file__),
            "env_vars": {"NCCL_SOCKET_IFNAME": "ens"},
        }
    )
    prepare_mnist()

    train_times = []
    tune_times = []

    for i in range(num_runs):
        print(f"Run {i+1} / {num_runs}")

        time.sleep(2)

        train_time = timeit.timeit(
            lambda: train_torch(num_workers=num_workers, use_gpu=use_gpu), number=1
        )
        train_times.append(train_time)

        time.sleep(2)

        tune_time = timeit.timeit(
            lambda: tune_torch(
                num_workers=num_workers, num_trials=num_trials, use_gpu=use_gpu
            ),
            number=1,
        )
        tune_times.append(tune_time)

        result = {"train_time": train_time, "tune_time": tune_time}
        print(f"Results run {i+1}: {result}")

    mean_train_time = np.mean(train_times)
    mean_tune_time = np.mean(tune_times)

    full_results = {
        "train_times": train_times,
        "train_mean": mean_train_time,
        "train_sd": np.std(train_times),
        "tune_times": tune_times,
        "tune_mean": mean_tune_time,
        "tune_sd": np.std(tune_times),
    }

    print("Full results:", full_results)

    factor = 1.2
    threshold = mean_train_time * factor

    assert (
        mean_tune_time <= threshold
    ), f"{mean_tune_time:.2f} > {threshold:.2f} = {factor:.1f} * {mean_train_time:.2f}"

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/result.json")
    with open(test_output_json, "wt") as f:
        json.dump(full_results, f)


if __name__ == "__main__":
    main()
