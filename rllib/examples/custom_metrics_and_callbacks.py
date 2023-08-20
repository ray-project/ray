"""Example of using RLlib's debug callbacks.

Here we use callbacks to track the average CartPole pole angle magnitude as a
custom metric.

We then use `keep_per_episode_custom_metrics` to keep the per-episode values
of our custom metrics and do our own summarization of them.
"""

from typing import Dict, Tuple
import argparse
import gymnasium as gym
import numpy as np
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.callbacks import DefaultCallbacks
from ray.rllib.env import BaseEnv
from ray.rllib.evaluation import Episode, RolloutWorker
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.pg.pg import PGConfig

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument("--stop-iters", type=int, default=2000)


# Create a custom CartPole environment that maintains an estimate of velocity
class CustomCartPole(gym.Env):
    def __init__(self, config):
        self.env = gym.make("CartPole-v1")
        self.observation_space = self.env.observation_space
        self.action_space = self.env.action_space
        self._pole_angle_vel = 0.0
        self.last_angle = 0.0

    def reset(self, *, seed=None, options=None):
        self._pole_angle_vel = 0.0
        obs, info = self.env.reset()
        self.last_angle = obs[2]
        return obs, info

    def step(self, action):
        obs, rew, term, trunc, info = self.env.step(action)
        angle = obs[2]
        self._pole_angle_vel = (
            0.5 * (angle - self.last_angle) + 0.5 * self._pole_angle_vel
        )
        info["pole_angle_vel"] = self._pole_angle_vel
        return obs, rew, term, trunc, info


class MyCallbacks(DefaultCallbacks):
    def on_episode_start(
        self,
        *,
        worker: RolloutWorker,
        base_env: BaseEnv,
        policies: Dict[str, Policy],
        episode: Episode,
        env_index: int,
        **kwargs
    ):
        # Make sure this episode has just been started (only initial obs
        # logged so far).
        assert episode.length == 0, (
            "ERROR: `on_episode_start()` callback should be called right "
            "after env reset!"
        )
        # Create lists to store angles in
        episode.user_data["pole_angles"] = []
        episode.hist_data["pole_angles"] = []

    def on_episode_step(
        self,
        *,
        worker: RolloutWorker,
        base_env: BaseEnv,
        policies: Dict[str, Policy],
        episode: Episode,
        env_index: int,
        **kwargs
    ):
        # Make sure this episode is ongoing.
        assert episode.length > 0, (
            "ERROR: `on_episode_step()` callback should not be called right "
            "after env reset!"
        )
        pole_angle = abs(episode.last_observation_for()[2])
        raw_angle = abs(episode.last_raw_obs_for()[2])
        assert pole_angle == raw_angle
        episode.user_data["pole_angles"].append(pole_angle)

        # Sometimes our pole is moving fast. We can look at the latest velocity
        # estimate from our environment and log high velocities.
        if np.abs(episode.last_info_for()["pole_angle_vel"]) > 0.25:
            print("This is a fast pole!")

    def on_episode_end(
        self,
        *,
        worker: RolloutWorker,
        base_env: BaseEnv,
        policies: Dict[str, Policy],
        episode: Episode,
        env_index: int,
        **kwargs
    ):
        # Check if there are multiple episodes in a batch, i.e.
        # "batch_mode": "truncate_episodes".
        if worker.config.batch_mode == "truncate_episodes":
            # Make sure this episode is really done.
            assert episode.batch_builder.policy_collectors["default_policy"].batches[
                -1
            ]["dones"][-1], (
                "ERROR: `on_episode_end()` should only be called "
                "after episode is done!"
            )
        pole_angle = np.mean(episode.user_data["pole_angles"])
        episode.custom_metrics["pole_angle"] = pole_angle
        episode.hist_data["pole_angles"] = episode.user_data["pole_angles"]

    def on_sample_end(self, *, worker: RolloutWorker, samples: SampleBatch, **kwargs):
        # We can also do our own sanity checks here.
        assert samples.count == 200, "I was expecting 200 here!"

    def on_train_result(self, *, algorithm, result: dict, **kwargs):
        # you can mutate the result dict to add new fields to return
        result["callback_ok"] = True

        # Normally, RLlib would aggregate any custom metric into a mean, max and min
        # of the given metric.
        # For the sake of this example, we will instead compute the variance and mean
        # of the pole angle over the evaluation episodes.
        pole_angle = result["custom_metrics"]["pole_angle"]
        var = np.var(pole_angle)
        mean = np.mean(pole_angle)
        result["custom_metrics"]["pole_angle_var"] = var
        result["custom_metrics"]["pole_angle_mean"] = mean
        # We are not interested in these original values
        del result["custom_metrics"]["pole_angle"]
        del result["custom_metrics"]["num_batches"]

    def on_learn_on_batch(
        self, *, policy: Policy, train_batch: SampleBatch, result: dict, **kwargs
    ) -> None:
        result["sum_actions_in_train_batch"] = train_batch["actions"].sum()
        # Log the sum of actions in the train batch.
        print(
            "policy.learn_on_batch() result: {} -> sum actions: {}".format(
                policy, result["sum_actions_in_train_batch"]
            )
        )

    def on_postprocess_trajectory(
        self,
        *,
        worker: RolloutWorker,
        episode: Episode,
        agent_id: str,
        policy_id: str,
        policies: Dict[str, Policy],
        postprocessed_batch: SampleBatch,
        original_batches: Dict[str, Tuple[Policy, SampleBatch]],
        **kwargs
    ):
        if "num_batches" not in episode.custom_metrics:
            episode.custom_metrics["num_batches"] = 0
        episode.custom_metrics["num_batches"] += 1


if __name__ == "__main__":
    args = parser.parse_args()

    config = (
        PGConfig()
        .environment(CustomCartPole)
        .framework(args.framework)
        .callbacks(MyCallbacks)
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
        .rollouts(enable_connectors=False)
        .reporting(keep_per_episode_custom_metrics=True)
    )

    ray.init(local_mode=True)
    tuner = tune.Tuner(
        "PG",
        run_config=air.RunConfig(
            stop={
                "training_iteration": args.stop_iters,
            },
        ),
        param_space=config,
    )
    # there is only one trial involved.
    result = tuner.fit().get_best_result()

    # Verify episode-related custom metrics are there.
    custom_metrics = result.metrics["custom_metrics"]
    print(custom_metrics)
    assert "pole_angle_mean" in custom_metrics
    assert "pole_angle_var" in custom_metrics
