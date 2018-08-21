from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Simple example of setting up a multi-agent policy mapping.

Control the number of agents and policies via --num-agents and --num-policies.

This works with hundreds of agents and policies, but note that initializing
many TF policy graphs will take some time.

Also, TF evals might slow down with large numbers of policies. To debug TF
execution, set the TF_TIMELINE_DIR environment variable.
"""

import argparse
import gym
import random

import ray
from ray import tune
from ray.rllib.agents.pg.pg_policy_graph import PGPolicyGraph
from ray.rllib.test.test_multi_agent_env import MultiCartpole
from ray.tune import run_experiments
from ray.tune.registry import register_env

parser = argparse.ArgumentParser()

parser.add_argument("--num-agents", type=int, default=4)
parser.add_argument("--num-policies", type=int, default=2)
parser.add_argument("--num-iters", type=int, default=20)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    # Simple environment with `num_agents` independent cartpole entities
    register_env("multi_cartpole", lambda _: MultiCartpole(args.num_agents))
    single_env = gym.make("CartPole-v0")
    obs_space = single_env.observation_space
    act_space = single_env.action_space

    def gen_policy():
        config = {
            "gamma": random.choice([0.5, 0.8, 0.9, 0.95, 0.99]),
            "n_step": random.choice([1, 2, 3, 4, 5]),
        }
        return (PGPolicyGraph, obs_space, act_space, config)

    # Setup PG with an ensemble of `num_policies` different policy graphs
    policy_graphs = {
        "policy_{}".format(i): gen_policy()
        for i in range(args.num_policies)
    }
    policy_ids = list(policy_graphs.keys())

    run_experiments({
        "test": {
            "run": "PG",
            "env": "multi_cartpole",
            "stop": {
                "training_iteration": args.num_iters
            },
            "config": {
                "multiagent": {
                    "policy_graphs": policy_graphs,
                    "policy_mapping_fn": tune.function(
                        lambda agent_id: random.choice(policy_ids)),
                },
            },
        }
    })
