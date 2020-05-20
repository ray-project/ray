import numpy as np
#import gym


#import ray
#from ray.rllib.env.unity3d_wrapper import Unity3DWrapper

#ray.init(local_mode=True)

#unity_env = Unity3DWrapper(100)
#unity_env.reset()
#unity_env_specs = next(iter(unity_env._env_specs.values()))





#!/usr/bin/env python
"""Example of running a Unity3D instance against a RLlib Trainer

TODO: Unity3D should be started automatically by this script.
"""

import argparse
from mlagents_envs.environment import UnityEnvironment
#from gym_unity.envs import UnityToGymWrapper

from ray.rllib.env.policy_client import PolicyClient

parser = argparse.ArgumentParser()
parser.add_argument(
    "--no-train", action="store_true", help="Whether to disable training.")
parser.add_argument(
    "--inference-mode", type=str, required=True, choices=["local", "remote"])
parser.add_argument(
    "--stop-reward",
    type=int,
    default=9999,
    help="Stop once the specified reward is reached.")


def _get_step_results(unity_env, brain_name):
    env_state = unity_env._env_state[brain_name]
    # [0] = DecisionSteps
    s = env_state[0].obs[0]  # [0] = TODO: (sven): Only use 1st obs comp for now.
    r = env_state[0].reward  # rewards vector
    # [1] = TerminalSteps
    d = env_state[1].agent_id  # Agent dones indices.
    return s, r, d


if __name__ == "__main__":
    args = parser.parse_args()

    # TODO(sven): Move all this logic into Unity3DWrapper class for RLlib
    #  that's already a PolicyClient, and just has to be "run".
    unity_env = UnityEnvironment()
    unity_env.reset()
    unity_env_spec = unity_env._env_specs
    # Don't wrap, only works with single agent Unity3D examples
    # (e.g. "Basic").
    # env = UnityToGymWrapper(unity_env, use_visual=False, uint8_visual=True)

    client = PolicyClient(
        "http://localhost:9900", inference_mode=args.inference_mode)
    eid = client.start_episode(training_enabled=not args.no_train)

    # Reset to set a first observation.
    unity_env.reset()
    # Get brain name.
    brain_name = list(unity_env._env_specs.keys())[0]
    num_agents = len(unity_env._env_state[brain_name][0].agent_id)
    obs_batch = unity_env._env_state[brain_name][0].obs[0]  # <- only take 0th component (assume observations are single-component obs).
    obs_batch = [obs_batch[i] for i in range(len(obs_batch))]
    episode_rewards = [0.0 for _ in range(len(obs_batch))]

    while True:
        action = client.get_action(eid, obs_batch)
        # Convert per-env + per-agent actions into Unity-readable action
        # vector.
        unity_actions = np.array([action[i]["agent0"] for i in range(len(action))])
        unity_env.set_actions(brain_name, unity_actions)
        unity_env.step()
        obs_batch, rewards, dones = _get_step_results(unity_env, brain_name)
        if len(rewards) != 0:
            episode_rewards += rewards
            client.log_returns(eid, rewards)
        if any(dones):
            print("Agents {} are done.".format(dones))
            print("Total reward:", rewards)
            if any(episode_rewards >= args.stop_reward):
                print("Target reward achieved, exiting.")
                exit(0)
            # Reset episode rewards for done agents.
            for i in dones:
                episode_rewards[i] = 0.0
            client.end_episode(eid, obs)
            obs = unity_env.reset()
            eid = client.start_episode(training_enabled=not args.no_train)


