from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import gym
import uuid

from ray.rllib.utils.policy_client import PolicyClient


parser = argparse.ArgumentParser()
parser.add_argument(
    "--off-policy", action="store_true",
    help="Whether to take random instead of on-policy actions.")


def new_eid():
    return uuid.uuid4().hex


if __name__ == "__main__":
    args = parser.parse_args()
    env = gym.make("CartPole-v0")
    client = PolicyClient("http://localhost:8900")

    eid = new_eid()
    client.start_episode(eid)
    obs = env.reset()
    rewards = 0

    while True:
        if args.off_policy:
            action = env.action_space.sample()
            client.log_action(obs, action, episode_id=eid)
        else:
            action = client.get_action(obs, episode_id=eid)
        obs, reward, done, info = env.step(action)
        rewards += reward
        client.log_returns(reward, info=info, episode_id=eid)
        if done:
            print("Total reward:", rewards)
            rewards = 0
            client.end_episode(obs, episode_id=eid)
            obs = env.reset()
            eid = new_eid()
            client.start_episode(eid)
