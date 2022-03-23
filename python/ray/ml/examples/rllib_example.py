import argparse
from typing import Tuple

import pandas as pd

import ray
from ray.ml.checkpoint import Checkpoint
from ray.ml.predictors.integrations.xgboost import XGBoostPredictor
from ray.ml.train.integrations.rllib.rllib_trainer import RLLibTrainer
from ray.ml.train.integrations.xgboost import XGBoostTrainer
from ray.data.dataset import Dataset
from ray.ml.result import Result
from ray.ml.preprocessors import StandardScaler
from sklearn.datasets import load_breast_cancer
from sklearn.model_selection import train_test_split


def train_rllib_bc(num_workers: int, use_gpu: bool = False) -> Result:
    path = "/tmp/out"
    parallelism = 2
    dataset = ray.data.read_json(
            path, parallelism=parallelism, ray_remote_args={"num_cpus": 1}
        )

    # Scale some random columns
    columns_to_scale = ["mean radius", "mean texture"]

    trainer = RLLibTrainer(
        scaling_config={
            "num_workers": num_workers,
            "use_gpu": use_gpu,
        },
        datasets={"train": dataset},
        num_boost_round=100,
    )
    result = trainer.fit()
    print(result.metrics)

    return result


def train_old_style():
    from ray import tune
    analysis = tune.run(
        "BC",
        stop={
            "timesteps_total": 500000

        },
        config={
            "env": "CartPole-v0",
            "framework": "tf",
            "evaluation_num_workers": 1,
            "evaluation_interval": 1,
            "evaluation_config": {
                "input": "sampler"
            },
            "input": "dataset",
            "input_config": {
                "format": "json",
                "path": "/tmp/out"
            }
        }
    )
    print(analysis.trials[0].checkpoint)


if __name__ == "__main__":
    train_old_style()

    import sys
    sys.exit(0)


    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    args, _ = parser.parse_known_args()

    ray.init(address=args.address)
    result = train_rllib_bc(num_workers=args.num_workers, use_gpu=args.use_gpu)
