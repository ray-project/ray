"""Example of querying a policy server. Copy this file for your use case.

To try this out, in two separate shells run:
    $ python cartpole_server.py [--use-connector]
    $ python cartpole_client.py [--use-connector]

To enable the application connector, pass the --use-connector flag to both the
client and server. The application connector provides much higher performance
training by caching the policy on the client and executing inference there.
Note that this incurs more computational cost on the client.
"""

import argparse
import gym

from ray.rllib.utils.policy_client import PolicyClient
from ray.rllib.utils.connector_client import ConnectorClient

parser = argparse.ArgumentParser()
parser.add_argument(
    "--no-train", action="store_true", help="Whether to disable training.")
parser.add_argument(
    "--use-connector",
    action="store_true",
    help="Whether to use the application connector API (this is faster).")
parser.add_argument(
    "--off-policy",
    action="store_true",
    help="Whether to take random instead of on-policy actions.")
parser.add_argument(
    "--stop-at-reward",
    type=int,
    default=9999,
    help="Stop once the specified reward is reached.")

if __name__ == "__main__":
    args = parser.parse_args()
    env = gym.make("CartPole-v0")
    if args.use_connector:
        client = ConnectorClient("http://localhost:9900", update_interval=5.0)
    else:
        client = PolicyClient("http://localhost:9900")

    eid = client.start_episode(training_enabled=not args.no_train)
    obs = env.reset()
    rewards = 0

    while True:
        if args.off_policy:
            action = env.action_space.sample()
            client.log_action(eid, obs, action)
        else:
            action = client.get_action(eid, obs)
        obs, reward, done, info = env.step(action)
        rewards += reward
        client.log_returns(eid, reward, info=info)
        if done:
            print("Total reward:", rewards)
            if rewards >= args.stop_at_reward:
                print("Target reward achieved, exiting")
                exit(0)
            rewards = 0
            client.end_episode(eid, obs)
            obs = env.reset()
            eid = client.start_episode(training_enabled=not args.no_train)
