from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

"""Simple example of setting up a multi-agent policy mapping."""

import gym
import random

import ray
from ray.rllib.pg.pg import PGAgent
from ray.rllib.pg.pg_policy_graph import PGPolicyGraph
from ray.rllib.test.test_multi_agent_env import MultiCartpole
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env


if __name__ == "__main__":
    # Simple environment with four independent cartpole entities in it
    register_env("multi_cartpole", lambda _: MultiCartpole(4))
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    ray.init()

    # Setup PG with an ensemble of two different policy graphs
    c1 = {
        "gamma": 0.95,
    }
    c2 = {
        "gamma": 0.50,
    }
    agent = PGAgent(
        env="multi_cartpole",
        config={
            "multiagent": {
                # Define the policy ensemble
                "policy_graphs": {
                    "p1": (PGPolicyGraph, obs_space, act_space, c1),
                    "p2": (PGPolicyGraph, obs_space, act_space, c2),
                },
                # Randomly assign the cartpoles to policies
                "policy_mapping_fn": (
                    lambda agent_id: random.choice(["p1", "p2"])),
            },
        })

    for i in range(20):
        print("== Iteration", i, "==")
        print(pretty_print(agent.train()))
