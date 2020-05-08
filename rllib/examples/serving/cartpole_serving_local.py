#!/usr/bin/env python
"""Example of running inference on a policy. Copy this file for your use case.
To try this out run:
    $ python cartpole_local_serving.py
"""

import argparse
import os

import ray
from ray.rllib.agents.dqn import DQNTrainer
from ray.rllib.agents.ppo import PPOTrainer

CHECKPOINT_FILE = "last_checkpoint_{}.out"

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="DQN")

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    env = "CartPole-v0"

    if args.run == "DQN":
        # Example of using DQN (supports off-policy actions).
        trainer = DQNTrainer(env=env)
    elif args.run == "PPO":
        # Example of using PPO (does NOT support off-policy actions).
        trainer = PPOTrainer(env=env)
    else:
        raise ValueError("--run must be DQN or PPO")

    checkpoint_path = CHECKPOINT_FILE.format(args.run)

    # Attempt to restore from checkpoint if possible.
    if os.path.exists(checkpoint_path):
        checkpoint_path = open(checkpoint_path).read()
        print("Restoring from checkpoint path", checkpoint_path)
        trainer.restore(checkpoint_path)

    # Serving and training loop
    env = trainer.env_creator({})
    state = trainer.get_policy().get_initial_state()
    obs = env.reset()
    while True:
        action, state, info_trainer = trainer.compute_action(
            obs, state=state, full_fetch=True)
        obs, reward, done, info = env.step(action)
        env.render()
        if done:
            obs = env.reset()
