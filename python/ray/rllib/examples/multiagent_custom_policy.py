"""Example of running a custom hand-coded policy alongside trainable policies.

This example has two policies:
    (1) a simple PG policy
    (2) a hand-coded policy that acts at random in the env (doesn't learn)

In the console output, you can see the PG policy does much better than random:
Result for PG_multi_cartpole_0:
  ...
  policy_reward_mean:
    pg_policy: 185.23
    random: 21.255
  ...
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import gym

import ray
from ray import tune
from ray.rllib.policy import Policy
from ray.rllib.tests.test_multi_agent_env import MultiCartpole
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()
parser.add_argument("--num-iters", type=int, default=20)


class RandomPolicy(Policy):
    """Hand-coded policy that returns random actions."""

    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        info_batch=None,
                        episodes=None,
                        **kwargs):
        """Compute actions on a batch of observations."""
        return [self.action_space.sample() for _ in obs_batch], [], {}

    def learn_on_batch(self, samples):
        """No learning."""
        return {}


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    # Simple environment with 4 independent cartpole entities
    register_env("multi_cartpole", lambda _: MultiCartpole(4))
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    tune.run(
        "PG",
        stop={"training_iteration": args.num_iters},
        config={
            "env": "multi_cartpole",
            "multiagent": {
                "policies": {
                    "pg_policy": (None, obs_space, act_space, {}),
                    "random": (RandomPolicy, obs_space, act_space, {}),
                },
                "policy_mapping_fn": tune.function(
                    lambda agent_id: ["pg_policy", "random"][agent_id % 2]),
            },
        },
    )
