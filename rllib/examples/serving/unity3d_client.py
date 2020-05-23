#!/usr/bin/env python

"""Example of running a Unity3D instance against an RLlib Trainer.
"""

import argparse

from ray.rllib.env.policy_client import PolicyClient
from ray.rllib.env.unity3d_env import Unity3DEnv

parser = argparse.ArgumentParser()
parser.add_argument(
    "--no-train", action="store_true", help="Whether to disable training.")
parser.add_argument(
    "--inference-mode", type=str, default="local", choices=["local", "remote"])
#parser.add_argument(
#    "--stop-reward",
#    type=int,
#    default=9999,
#    help="Stop once the specified reward is reached.")


if __name__ == "__main__":
    args = parser.parse_args()

    # Start the client for sending environment information (e.g. observations,
    # actions) to a policy server (listening on port 9900).
    client = PolicyClient(
        "http://localhost:9900", inference_mode=args.inference_mode)

    # Start and reset the actual Unity3DEnv (either already running Unity3D
    # editor or a binary (game) to be started automatically).
    env = Unity3DEnv()  # TODO: add file and other options here
    obs = env.try_reset()
    eids = client.start_episode(
        training_enabled=not args.no_train, num_episodes=env.num_envs)

    # Loop infinitely through the env.
    while True:
        # Get actions from the Policy server given our current obs.
        action = client.get_action(eids, obs)
        # Apply actions to our env.
        obs, rewards, dones, infos = env.send_actions(action)
        if dones:
            #?? Tell server, the episode has ended.
            client.end_episode(eid, obs)
            obs = env.try_reset()
            eid = client.start_episode(training_enabled=not args.no_train)
