"""Example of running a policy server. Copy this file for your use case.

To try this out, in two separate shells run:
    $ python cartpole_server.py
    $ python cartpole_client.py
"""

import argparse
import os
from gym import spaces
import numpy as np

import ray
from ray.rllib.agents.dqn import DQNTrainer
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.utils.policy_server import PolicyServer
from ray.rllib.utils.connector_server import ConnectorServer
from ray.tune.logger import pretty_print
from ray.tune.registry import register_env

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9900
CHECKPOINT_FILE = "last_checkpoint.out"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--use-connector",
    action="store_true",
    help="Whether to use the application connector API (this is faster).")


class CartpoleServing(ExternalEnv):
    def __init__(self):
        ExternalEnv.__init__(
            self, spaces.Discrete(2),
            spaces.Box(low=-10, high=10, shape=(4, ), dtype=np.float32))

    def run(self):
        print("---")
        print("--- Starting policy server at {}:{}".format(
            SERVER_ADDRESS, SERVER_PORT))
        print("---")
        server = PolicyServer(self, SERVER_ADDRESS, SERVER_PORT)
        server.serve_forever()


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    if args.use_connector:
        env = "CartPole-v0"
        connector_config = {
            # Use the connector server to generate experiences.
            "input": (
                lambda ioctx: ConnectorServer(ioctx, SERVER_ADDRESS, SERVER_PORT)
            ),
            # Disable OPE, since the rollouts are coming from online clients.
            "input_evaluation": [],
        }
    else:
        register_env("srv", lambda _: CartpoleServing())
        env = "srv"
        connector_config = {}

    # We use DQN since it supports off-policy actions, but you can choose and
    # configure any agent.
#    trainer = DQNTrainer(
#        env=env,
#        config=dict(connector_config, **{
#            # Use a single worker process to run the server.
#            "num_workers": 0,
#            # Configure the agent to run short iterations for debugging
#            "exploration_config": {
#                "type": "EpsilonGreedy",
#                "initial_epsilon": 1.0,
#                "final_epsilon": 0.02,
#                "epsilon_timesteps": 100,
#            },
#            "learning_starts": 100,
#            "timesteps_per_iteration": 200,
#        }))

    trainer = PPOTrainer(
        env=env,
        config=dict(
            connector_config,
            **{
                # Use a single worker process to run the server.
                "num_workers": 0,
                # Configure the agent to run short iterations for debugging
                "sample_batch_size": 1000,
                "train_batch_size": 4000,
            }))

    # Attempt to restore from checkpoint if possible.
    if os.path.exists(CHECKPOINT_FILE):
        checkpoint_path = open(CHECKPOINT_FILE).read()
        print("Restoring from checkpoint path", checkpoint_path)
        trainer.restore(checkpoint_path)

    # Serving and training loop
    while True:
        print(pretty_print(trainer.train()))
        checkpoint_path = trainer.save()
        print("Last checkpoint", checkpoint_path)
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(checkpoint_path)
