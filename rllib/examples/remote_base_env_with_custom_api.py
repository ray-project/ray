"""
This script demonstrates how one can specify custom env APIs in
combination with RLlib's `remote_worker_envs` setting, which
parallelizes individual sub-envs within a vector env by making each
one a ray Actor.

You can access your Env's API via a custom callback as shown below.
"""
import argparse
import gym
import os

import ray
from ray.rllib.agents.callbacks import DefaultCallbacks
from ray.rllib.env.apis.task_settable_env import TaskSettableEnv
from ray.rllib.utils.test_utils import check_learning_achieved
from ray import tune

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument("--num-workers", type=int, default=1)

# This should be >1, otherwise, remote envs make no sense.
parser.add_argument("--num-envs-per-worker", type=int, default=4)

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


class NonVectorizedEnvToBeVectorizedIntoRemoteBaseEnv(TaskSettableEnv):
    """Class for a single sub-env to be vectorized into RemoteBaseEnv.

    If you specify this class directly under the "env" config key, RLlib
    will auto-wrap

    Note that you may implement your own custom APIs. Here, we demonstrate
    using RLlib's TaskSettableEnv API (which is a simple sub-class
    of gym.Env).
    """

    def __init__(self, config=None):
        self.action_space = gym.spaces.Box(0, 1, shape=(1,))
        self.observation_space = gym.spaces.Box(0, 1, shape=(2,))
        self.task = 1

    def reset(self):
        self.steps = 0
        return self.observation_space.sample()

    def step(self, action):
        self.steps += 1
        return self.observation_space.sample(), 0, self.steps > 10, {}

    def set_task(self, task) -> None:
        """We can set the task of each sub-env (ray actor)"""
        print("Task set to {}".format(task))
        self.task = task


class TaskSettingCallback(DefaultCallbacks):
    """Custom callback to verify, we can set the task on each remote sub-env."""

    def on_train_result(self, *, trainer, result: dict, **kwargs) -> None:
        """Curriculum learning as seen in Ray docs"""
        if result["episode_reward_mean"] > 0.0:
            phase = 0
        else:
            phase = 1

        # Sub-envs are now ray.actor.ActorHandles, so we have to add
        # `remote()` here.
        trainer.workers.foreach_env(lambda env: env.set_task.remote(phase))


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(num_cpus=6)

    config = {
        # Specify your custom (single, non-vectorized) env directly as a
        # class. This way, RLlib can auto-create Actors from this class
        # and handle everything correctly.
        "env": NonVectorizedEnvToBeVectorizedIntoRemoteBaseEnv,
        # Set up our own callbacks.
        "callbacks": TaskSettingCallback,
        # Force sub-envs to be ray.actor.ActorHandles, so we can step
        # through them in parallel.
        "remote_worker_envs": True,
        # How many RolloutWorkers (each with n environment copies:
        # `num_envs_per_worker`)?
        "num_workers": args.num_workers,
        # This setting should not really matter as it does not affect the
        # number of GPUs reserved for each worker.
        "num_envs_per_worker": args.num_envs_per_worker,
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
        "framework": args.framework,
    }

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    results = tune.run(args.run, config=config, stop=stop, verbose=1)

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
    ray.shutdown()
