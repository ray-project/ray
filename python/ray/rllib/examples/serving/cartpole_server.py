from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from gym import spaces

import ray
from ray.rllib.dqn import DQNAgent
from ray.rllib.utils.serving_env import ServingEnv
from ray.rllib.utils.policy_server import PolicyServer
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

SERVER_ADDRESS = "localhost"
SERVER_PORT = 8900


class CartpoleServing(ServingEnv):
    def __init__(self):
        ServingEnv.__init__(
            self, spaces.Discrete(2), spaces.Box(low=-10, high=10, shape=(4,)))

    def run(self):
        print("Starting policy server at {}:{}".format(
            SERVER_ADDRESS, SERVER_PORT))
        server = PolicyServer(self, SERVER_ADDRESS, SERVER_PORT)
        server.serve_forever()


if __name__ == "__main__":
    ray.init()
    register_env("srv", lambda _: CartpoleServing())
    dqn = DQNAgent(env="srv", config={
        # Use a single process to avoid needing to set up a load balancer
        "num_workers": 0,
        # Configure the agent to run short iterations for debugging
        "exploration_fraction": 0.01,
        "learning_starts": 100,
        "timesteps_per_iteration": 200,
    })

    # Serving and training loop
    while True:
        print(pretty_print(dqn.train()))
