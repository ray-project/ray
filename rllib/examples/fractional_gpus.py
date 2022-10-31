"""Example of a custom gym environment and model. Run this for a demo.

This example shows:
  - using a custom environment
  - using a custom model
  - using Tune for grid search

You can visualize experiment results in ~/ray_results using TensorBoard.
"""
import argparse

import ray
from ray import air, tune
from ray.rllib.examples.env.gpu_requiring_env import GPURequiringEnv
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import get_trainable_cls

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--num-gpus", type=float, default=0.5)
parser.add_argument("--num-workers", type=int, default=1)
parser.add_argument("--num-gpus-per-worker", type=float, default=0.0)
parser.add_argument("--num-envs-per-worker", type=int, default=1)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=50, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=180.0, help="Reward at which we stop training."
)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=4)

    # These configs have been tested on a p2.8xlarge machine (8 GPUs, 16 CPUs),
    # where ray was started using only one of these GPUs:
    # $ ray start --num-gpus=1 --head

    # Note: A strange error could occur when using tf:
    #       "NotImplementedError: Cannot convert a symbolic Tensor
    #       (default_policy/cond/strided_slice:0) to a numpy array."
    #       In rllib/utils/exploration/random.py.
    #       Fix: Install numpy version 1.19.5.

    # Tested arg combinations (4 tune trials will be setup; see
    # tune.grid_search over 4 learning rates below):
    # - num_gpus=0.5 (2 tune trials should run in parallel).
    # - num_gpus=0.3 (3 tune trials should run in parallel).
    # - num_gpus=0.25 (4 tune trials should run in parallel)
    # - num_gpus=0.2 + num_gpus_per_worker=0.1 (1 worker) -> 0.3
    #   -> 3 tune trials should run in parallel.
    # - num_gpus=0.2 + num_gpus_per_worker=0.1 (2 workers) -> 0.4
    #   -> 2 tune trials should run in parallel.
    # - num_gpus=0.4 + num_gpus_per_worker=0.1 (2 workers) -> 0.6
    #   -> 1 tune trial should run in parallel.

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        # Setup the test env as one that requires a GPU, iff
        # num_gpus_per_worker > 0.
        .environment(
            GPURequiringEnv if args.num_gpus_per_worker > 0.0 else "CartPole-v1"
        )
        .framework(args.framework)
        .resources(
            # How many GPUs does the local worker (driver) need? For most algos,
            # this is where the learning updates happen.
            # Set this to > 1 for multi-GPU learning.
            num_gpus=args.num_gpus,
            # How many GPUs does each RolloutWorker (`num_workers`) need?
            num_gpus_per_worker=args.num_gpus_per_worker,
        )
        # How many RolloutWorkers (each with n environment copies:
        # `num_envs_per_worker`)?
        .rollouts(
            num_rollout_workers=args.num_workers,
            # This setting should not really matter as it does not affect the
            # number of GPUs reserved for each worker.
            num_envs_per_worker=args.num_envs_per_worker,
        )
        # 4 tune trials altogether.
        .training(lr=tune.grid_search([0.005, 0.003, 0.001, 0.0001]))
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    # Note: The above GPU settings should also work in case you are not
    # running via ``Tuner.fit()``, but instead do:

    # >> from ray.rllib.algorithms.ppo import PPO
    # >> algo = PPO(config=config)
    # >> for _ in range(10):
    # >>     results = algo.train()
    # >>     print(results)

    results = tune.Tuner(
        args.run, param_space=config.to_dict(), run_config=air.RunConfig(stop=stop)
    ).fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
