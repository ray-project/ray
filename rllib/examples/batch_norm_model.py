"""Example of using a custom model with batch norm."""

import argparse
import os

import ray
from ray import tune
from ray.rllib.examples.models.batch_norm_model import BatchNormModel, \
    KerasBatchNormModel, TorchBatchNormModel
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--torch", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=150)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    ModelCatalog.register_custom_model(
        "bn_model", TorchBatchNormModel if args.torch else KerasBatchNormModel
        if args.run != "PPO" else BatchNormModel)

    config = {
        "env": "Pendulum-v0" if args.run in ["DDPG", "SAC"] else "CartPole-v0",
        "model": {
            "custom_model": "bn_model",
        },
        "lr": 0.0003,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 0,
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, stop=stop, config=config, verbose=2)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
