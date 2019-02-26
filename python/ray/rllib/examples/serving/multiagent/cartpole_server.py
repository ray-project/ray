from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Example of running a policy server. Copy this file for your use case.

To try this out, in two separate shells run:
    $ python cartpole_server.py
    $ python cartpole_client.py
"""

import argparse
import os
from gym import spaces
import numpy as np
import random

import ray
import ray.tune as tune
from ray.rllib.agents.ppo import PPOAgent
from ray.rllib.agents.ppo.ppo_policy_graph import PPOPolicyGraph
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.utils.multiagent.policy_server import PolicyServer
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9990
CHECKPOINT_FILE = "last_checkpoint.out"

parser = argparse.ArgumentParser()
parser.add_argument("--num-agents", type=int, default=4)
parser.add_argument("--num-policies", type=int, default=2)

ACT_SPACE = spaces.Box(low=-10, high=10, shape=(4, ), dtype=np.float32)
OBS_SPACE = spaces.Discrete(2)


class CartpoleServing(ExternalMultiAgentEnv):
    def __init__(self):
        ExternalMultiAgentEnv.__init__(self, ACT_SPACE, OBS_SPACE)

    def run(self):
        print("Starting policy server at {}:{}".format(SERVER_ADDRESS,
                                                       SERVER_PORT))
        server = PolicyServer(self, SERVER_ADDRESS, SERVER_PORT)
        server.serve_forever()


if __name__ == "__main__":

    # parse args
    args = parser.parse_args()
    agents = [f"agent_{x}" for x in range(args.num_agents)]

    # initialize ray & register Env
    ray.init()
    register_env("srv", lambda _: CartpoleServing())

    # Each policy can have a different configuration (including custom model)

    def gen_policy(i):
        config = {
            "gamma": random.choice([0.95, 0.99])
        }
        return (PPOPolicyGraph, OBS_SPACE, ACT_SPACE, config)

    # Setup PPO with an ensemble of `num_policies` different policy graphs
    policy_graphs = {
        "policy_{}".format(i): gen_policy(i)
        for i in range(args.num_policies)
    }
    policy_ids = list(policy_graphs.keys())

    ppo = PPOAgent(
        env="srv",
        config={
            # Use a single process to avoid needing to set up a load balancer
            "num_workers": 0,
            # Configure the agent to run short iterations for debugging
            # "learning_starts": 100,
            # "timesteps_per_iteration": 200,
            "observation_filter": "NoFilter",
            "log_level": "INFO",
            "num_sgd_iter": 10,
            "multiagent": {
                "policy_graphs": policy_graphs,
                "policy_mapping_fn": tune.function(
                    lambda agent_id: random.choice(policy_ids)),
            },
        })

    # Attempt to restore from checkpoint if possible.
    """
    if os.path.exists(CHECKPOINT_FILE):
        checkpoint_path = open(CHECKPOINT_FILE).read()
        print("Restoring from checkpoint path", checkpoint_path)
        ppo.restore(checkpoint_path)
    """

    # Serving and training loop
    while True:
        print(pretty_print(ppo.train()))
        checkpoint_path = ppo.save()
        print("Last checkpoint", checkpoint_path)
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(checkpoint_path)
