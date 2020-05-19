#!/usr/bin/env python
"""Example of running a policy server. Copy this file for your use case.

To try this out, in two separate shells run:
    $ python cartpole_server.py
    $ python cartpole_client.py --inference-mode=local|remote
"""

import argparse
from gym.spaces import Box
import os

import ray
from ray.tune import register_env
from ray.rllib.agents.ppo import PPOTrainer
from ray.rllib.env.policy_server_input import PolicyServerInput
from ray.rllib.env.vector_env import VectorEnv
from ray.rllib.examples.env.random_env import RandomEnv
from ray.tune.logger import pretty_print

SERVER_ADDRESS = "localhost"
SERVER_PORT = 9900
CHECKPOINT_FILE = "last_checkpoint_{}.out"

parser = argparse.ArgumentParser()
parser.add_argument("--no-restore", action="store_true")

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    register_env("fake_unity",
                 lambda c: VectorEnv.wrap(
                     make_env=lambda c: RandomEnv(c),
                     existing_envs=None,
                     num_envs=12,
                     env_config={
                         "action_space": Box(-1.0, 1.0, (2,)),
                         "observation_space": Box(float("-inf"), float("inf"),
                                                  (8,)),
                     }))

    connector_config = {
        # Use the connector server to generate experiences.
        "input": (
            lambda ioctx: PolicyServerInput(ioctx, SERVER_ADDRESS, SERVER_PORT)
        ),
        # Use a single worker process to run the server.
        "num_workers": 0,
        # Disable OPE, since the rollouts are coming from online clients.
        "input_evaluation": [],
    }

    # Example of using PPO (does NOT support off-policy actions).
    trainer = PPOTrainer(
        env="fake_unity",
        config=dict(
            connector_config, **{
                "sample_batch_size": 64,
                "train_batch_size": 256,
            }))

    checkpoint_path = CHECKPOINT_FILE.format("PPO")  #args.run)

    # Attempt to restore from checkpoint if possible.
    if not args.no_restore and os.path.exists(checkpoint_path):
        checkpoint_path = open(checkpoint_path).read()
        print("Restoring from checkpoint path", checkpoint_path)
        trainer.restore(checkpoint_path)

    # Serving and training loop.
    while True:
        print(pretty_print(trainer.train()))
        checkpoint = trainer.save()
        print("Last checkpoint", checkpoint)
        with open(checkpoint_path, "w") as f:
            f.write(checkpoint)
