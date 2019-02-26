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
import random

from ray.rllib.utils.multiagent.policy_client import PolicyClient
from ray.rllib.test.test_multi_agent_env import MultiCartpole


ACT_SPACE = spaces.Box(low=-10, high=10, shape=(4, ), dtype=np.float32)
OBS_SPACE = spaces.Discrete(2)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--no-train", action="store_true", help="Whether to disable training.")
parser.add_argument(
    "--off-policy",
    action="store_true",
    help="Whether to take random instead of on-policy actions.")
parser.add_argument(
    "--num-agents",
    type=int,
    default=4,
    help="The number of agents in the Env")
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

    # create env and specify agents
    env = MultiCartpole(4)
    agents = [f"agent_{x}" for x in range(args.num_agents)]

    # run PolicyClient
    client = PolicyClient("http://localhost:9990")
    debug("PolicyClient created.")

    eid = client.start_episode(training_enabled=not args.no_train)
    obs = env.reset()
    rewards = 0
    debug("env.reset() called.")

    while True:
        if args.off_policy:
            action = { 0: env.action_space.sample() }
            client.log_action(eid, obs, action)
            debug(f"[loop] action sampled and logged.")
        else:
            action = client.get_action(eid, obs)
        debug(f"[loop] action chosen: {action}")
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
