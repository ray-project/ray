#!/usr/bin/env python
"""
For testing purposes only.
Runs a policy client that starts two episodes, uses one for calculating actions
("action episode") and the other for logging those actions ("logging episode").
Terminates the "logging episode" before computing a few more actions
from the "action episode".
The action episode is also started with the training_enabled=False flag so no
batches should be produced by this episode for training inside the
SampleCollector's `postprocess_trajectory` method.
"""

import argparse
import gym
import ray

from ray.rllib.env.policy_client import PolicyClient

parser = argparse.ArgumentParser()
parser.add_argument(
    "--inference-mode", type=str, default="local", choices=["local", "remote"]
)
parser.add_argument(
    "--off-policy",
    action="store_true",
    help="Whether to compute random actions instead of on-policy "
    "(Policy-computed) ones.",
)
parser.add_argument(
    "--port", type=int, default=9900, help="The port to use (on localhost)."
)
parser.add_argument("--dummy-arg", type=str, default="")


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init()

    # Use a CartPole-v1 env so this plays nicely with our cartpole server script.
    env = gym.make("CartPole-v1")

    # Note that the RolloutWorker that is generated inside the client (in case
    # of local inference) will contain only a RandomEnv dummy env to step through.
    # The actual env we care about is the above generated CartPole one.
    client = PolicyClient(
        f"http://localhost:{args.port}", inference_mode=args.inference_mode
    )

    # Get a dummy obs
    dummy_obs = env.reset()
    dummy_reward = 1.3

    # Start an episode to only compute actions (do NOT record this episode's
    # trajectories in any returned SampleBatches sent to the server for learning).
    action_eid = client.start_episode(training_enabled=False)
    print(f"Starting action episode: {action_eid}.")
    # Get some actions using the action episode
    dummy_action = client.get_action(action_eid, dummy_obs)
    print(f"Computing action 1 in action episode: {dummy_action}.")
    dummy_action = client.get_action(action_eid, dummy_obs)
    print(f"Computing action 2 in action episode: {dummy_action}.")

    # Start a log episode to log action and log rewards for learning.
    log_eid = client.start_episode(training_enabled=True)
    print(f"Starting logging episode: {log_eid}.")
    # Produce an action, just for testing.
    garbage_action = client.get_action(log_eid, dummy_obs)
    # Log 1 action and 1 reward.
    client.log_action(log_eid, dummy_obs, dummy_action)
    client.log_returns(log_eid, dummy_reward)
    print(f".. logged action + reward: {dummy_action} + {dummy_reward}")

    # Log 2 actions (w/o reward in the middle) and then one reward.
    # The reward after the 1st of these actions should be considered 0.0.
    client.log_action(log_eid, dummy_obs, dummy_action)
    client.log_action(log_eid, dummy_obs, dummy_action)
    client.log_returns(log_eid, dummy_reward)
    print(f".. logged actions + reward: 2x {dummy_action} + {dummy_reward}")

    # End the log episode
    client.end_episode(log_eid, dummy_obs)
    print(".. ended logging episode")

    # Continue getting actions using the action episode
    # The bug happens when executing the following line
    dummy_action = client.get_action(action_eid, dummy_obs)
    print(f"Computing action 3 in action episode: {dummy_action}.")
    dummy_action = client.get_action(action_eid, dummy_obs)
    print(f"Computing action 4 in action episode: {dummy_action}.")
