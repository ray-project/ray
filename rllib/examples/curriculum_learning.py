"""
Example of a curriculum learning setup using the `TaskSettableEnv` API
and the env_task_fn config.

This example shows:
  - Writing your own curriculum-capable environment using gym.Env.
  - Defining a env_task_fn that determines, whether and which new task
    the env(s) should be set to (using the TaskSettableEnv API).
  - Using Tune and RLlib to curriculum-learn this env.

You can visualize experiment results in ~/ray_results using TensorBoard.
"""
import argparse
import os

import ray
from ray import tune
from ray.rllib.env.base_env import BaseEnv
from ray.rllib.env.env_context import EnvContext
from ray.rllib.examples.env.curriculum_capable_env import CurriculumCapableEnv
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check_learning_achieved

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument("--torch", action="store_true")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=50)
parser.add_argument("--stop-timesteps", type=int, default=100000)
parser.add_argument("--stop-reward", type=float, default=0.1)


def curriculum_fn(train_results: dict,
                  base_env: BaseEnv,
                  env_ctx: EnvContext):
    #TODO: docstring.
    # Our env supports tasks 1 to 10.
    # Default: task 1
    # If mean episode rewards are above 10.0 -> Move to task 2.
    # etc..
    new_task = (train_results["episode_reward_mean"] // 10) + 1

    #TODO
    return new_task


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(local_mode=True)#TODO

    # Can also register the env creator function explicitly with:
    # register_env(
    #     "curriculum_env", lambda config: CurriculumCapableEnv(config))

    config = {
        "env": CurriculumCapableEnv,  # or "curriculum_env" if registered above
        "env_config": {
            "corridor_length": 5,
        },
        "env_task_fn": curriculum_fn,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "num_workers": 2,  # parallelism
        "framework": "torch" if args.torch else "tf",
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    from ray.rllib.agents.ppo import PPOTrainer
    trainer = PPOTrainer(config=config)
    trainer.train()
    results = trainer.workers.foreach_env_with_context(lambda env, ctx: ctx.worker_index)
    print(results)
    results = tune.run(args.run, config=config, stop=stop)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
