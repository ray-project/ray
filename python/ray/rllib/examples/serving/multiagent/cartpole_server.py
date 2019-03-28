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
from time import sleep
import threading

import ray
import ray.tune as tune
from ray.rllib.agents.ppo import PPOAgent
from ray.rllib.agents.ppo.ppo_policy_graph import PPOPolicyGraph
from ray.rllib.agents.dqn import DQNAgent
from ray.rllib.agents.dqn.dqn_policy_graph import DQNPolicyGraph
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.utils.policy_server import PolicyServer
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

from constants import *

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9990
CHECKPOINT_FILE = "last_checkpoint_ppo.out"

parser = argparse.ArgumentParser()


class CartpoleServing(ExternalMultiAgentEnv):
    def __init__(self):
        # all our agents share the same spaces in this example
        # ExternalMultiAgentEnv.__init__(self,
        # ACT_SPACE, OBS_SPACE)
        ExternalMultiAgentEnv.__init__(self,
        None, None)

    def run(self):
        print("Starting policy server at {}:{}".format(SERVER_ADDRESS,
                                                    SERVER_PORT))
        print(f"Thread {threading.get_ident()}")
        server = PolicyServer(self, SERVER_ADDRESS, SERVER_PORT)
        server.serve_forever()


if __name__ == "__main__":

    # parse args
    args = parser.parse_args()

    # initialize ray & register Env
    ray.init(num_cpus=NUM_CPUS)
    register_env("srv", lambda _: CartpoleServing())

    policy_graphs = {
        "ppo_policy": (PPOPolicyGraph, OBS_SPACE, ACT_SPACE, {}),
    }
    policy_ids = list(policy_graphs.keys())

    def policy_mapper(agent_id):
        return random.choice(policy_ids)

    ppo_trainer = PPOAgent(
        env="srv",
        config={
            "num_workers": 0,
            "multiagent": {
                "policy_graphs": policy_graphs,
                "policy_mapping_fn": policy_mapper,
                "policies_to_train": ["ppo_policy"],
            },
            "log_level": "INFO",
            # "exploration_fraction": 0.01,
            # "learning_starts": 100,
            # "timesteps_per_iteration": 200,

            # disable filters, otherwise we would need to synchronize those
            # as well to the DQN agent
            "observation_filter": "NoFilter",
            "simple_optimizer": True,
        })

    # disable DQN exploration when used by the PPO trainer
    # ppo_trainer.optimizer.foreach_evaluator(
    #     lambda ev: ev.for_policy(
    #         lambda pi: pi.set_epsilon(0.0), policy_id="dqn_policy"))


    # Attempt to restore from checkpoint if possible.
    try:
        if os.path.exists(CHECKPOINT_FILE):
            checkpoint_path = open(CHECKPOINT_FILE).read()
            print("Restoring from checkpoint path", checkpoint_path)
            ppo_trainer.restore(checkpoint_path)
    except:
        print("restore failed")

    # Serving and training loop
    while True:
        print("\n-- PPO --")
        print(pretty_print(ppo_trainer.train()))
        sleep(10)
        checkpoint_path = ppo_trainer.save()
        print("Last checkpoint", checkpoint_path)
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(checkpoint_path)
        sleep(0.5)
