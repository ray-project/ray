from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Example of running a policy server. Copy this file for your use case.

To try this out, in two separate shells run:
    $ python cartpole_server.py
    $ python cartpole_client.py
"""

import os
from gym import spaces
import numpy as np

import ray
from ray.rllib.agents.dqn import DQNAgent
from ray.rllib.env.serving_env import ServingEnv
from ray.rllib.utils.policy_server import PolicyServer
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

SERVER_ADDRESS = "localhost"
SERVER_PORT = 8900
CHECKPOINT_FILE = "last_checkpoint.out"


class CartpoleServing(ServingEnv):
    def __init__(self):
        ServingEnv.__init__(
            self, spaces.Discrete(2),
            spaces.Box(low=-10, high=10, shape=(4, ), dtype=np.float32))

    def run(self):
        print("Starting policy server at {}:{}".format(SERVER_ADDRESS,
                                                       SERVER_PORT))
        server = PolicyServer(self, SERVER_ADDRESS, SERVER_PORT)
        server.serve_forever()


if __name__ == "__main__":
    ray.init()
    register_env("srv", lambda _: CartpoleServing())

    # We use DQN since it supports off-policy actions, but you can choose and
    # configure any agent.
    dqn = DQNAgent(
        env="srv",
        config={
            # Use a single process to avoid needing to set up a load balancer
            "num_workers": 0,
            # Configure the agent to run short iterations for debugging
            "exploration_fraction": 0.01,
            "learning_starts": 100,
            "timesteps_per_iteration": 200,
        })

    # Attempt to restore from checkpoint if possible.
    if os.path.exists(CHECKPOINT_FILE):
        checkpoint_path = open(CHECKPOINT_FILE).read()
        print("Restoring from checkpoint path", checkpoint_path)
        dqn.restore(checkpoint_path)

    # Serving and training loop
    while True:
        print(pretty_print(dqn.train()))
        checkpoint_path = dqn.save()
        print("Last checkpoint", checkpoint_path)
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(checkpoint_path)
