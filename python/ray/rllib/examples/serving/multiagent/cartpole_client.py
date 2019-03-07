from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
"""Example of querying a policy server. Copy this file for your use case.

To try this out, in two separate shells run:
    $ python cartpole_server.py
    $ python cartpole_client.py
"""

import argparse
import gym
from gym import spaces
import random
import numpy as np
from time import sleep

from ray.rllib.utils.multiagent.policy_client import PolicyClient
from ray.rllib.test.test_multi_agent_env import MultiCartpole

from constants import *


parser = argparse.ArgumentParser()
parser.add_argument(
    "--no-train", action="store_true", help="Whether to disable training.")
parser.add_argument(
    "--off-policy",
    action="store_true",
    help="Whether to take random instead of on-policy actions.")
parser.add_argument(
    "--stop-at-reward",
    type=int,
    default=9999,
    help="Stop once the specified reward is reached.")

def debug(arg):
    print(f"[DEBUG] {arg}")

if __name__ == "__main__":
    # parse args
    args = parser.parse_args()

    # create env
    env = MultiCartpole(NUM_AGENTS)

    # run PolicyClient
    client = PolicyClient("http://localhost:9990")
    debug("PolicyClient created.")

    eid = client.start_episode(training_enabled=not args.no_train)
    obs = env.reset()
    debug(f"observations gathered: {obs}")
    rewards = 0
    debug("env.reset() called.")

    trial_count = 1
    all_rewards = {}

    while True:
        # choose action
        if args.off_policy:
            action = { i: env.action_space.sample() for i in range(NUM_AGENTS)}
            client.log_action(eid, obs, action)
            # debug(f"[loop] action sampled and logged.")
        else:
            # for each agent, get an action separately
            action = {}
            for ag, o in obs.items():
                action.update(client.get_action(eid, {ag: o}))
            # action = client.get_action(eid, obs)
        debug(f"[loop] action chosen: {action}")

        # step the env
        obs, reward, done, info = env.step(action)
        debug(f"obs: {obs}")
        debug(f"reward: {reward}")
        debug(f"done: {done}")

        if all_rewards == {}:
            all_rewards = reward
        else:
            for ag, rew in reward.items():
                all_rewards[ag] += rew
        
        # this does not work?!
        rewards += sum([v for k, v in reward.items()])
        client.log_returns(eid, reward, info=info)
        if done["__all__"]:
            print("----")
            print("Total rewards:")
            print(all_rewards)
            print("----")
            sleep(2)
            if rewards >= args.stop_at_reward:
                print("Target reward achieved, exiting")
                exit(0)
            rewards = 0
            client.end_episode(eid, obs)
            trial_count = 0
            all_rewards = {}
            obs = env.reset()
            eid = client.start_episode(training_enabled=not args.no_train)

        trial_count += 1