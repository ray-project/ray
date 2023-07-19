"""Example of using a custom model with batch norm."""

import argparse
import os

import ray
from ray import air, tune
from ray.rllib.examples.models.batch_norm_model import (
    BatchNormModel,
    KerasBatchNormModel,
    TorchBatchNormModel,
)
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import get_trainable_cls

tf1, tf, tfv = try_import_tf()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=150.0, help="Reward at which we stop training."
)
parser.add_argument(
    "--stop-time",
    type=float,
    default=60 * 60,
    help="Time (in seconds) after which we stop training.",
)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    ModelCatalog.register_custom_model(
        "bn_model",
        TorchBatchNormModel
        if args.framework == "torch"
        else KerasBatchNormModel
        if args.run != "PPO"
        else BatchNormModel,
    )

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment("Pendulum-v1" if args.run in ["DDPG", "SAC"] else "CartPole-v1")
        .framework(args.framework)
        .rollouts(num_rollout_workers=3)
        .training(
            model={"custom_model": "bn_model"},
            lr=0.0003,
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        # Batch-norm models have not been migrated to the RL Module API yet.
        .training(_enable_learner_api=False)
        .rl_module(_enable_rl_module_api=False)
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
        "time_total_s": args.stop_time,
    }

    tuner = tune.Tuner(
        args.run,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            verbose=2,
            stop=stop,
        ),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
