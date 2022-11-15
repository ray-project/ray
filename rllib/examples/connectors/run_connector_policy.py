"""This example script shows how to load a connector enabled policy,
and use it in a serving/inference setting.
"""

import argparse
import gymnasium as gym
from pathlib import Path

from ray.rllib.policy.policy import Policy
from ray.rllib.utils.policy import local_policy_inference


parser = argparse.ArgumentParser()
# This should a checkpoint created with connectors enabled.
parser.add_argument(
    "--checkpoint_file",
    help="Path to an RLlib checkpoint file, relative to //ray/rllib/ folder.",
)
parser.add_argument(
    "--policy_id",
    default="default_policy",
    help="ID of policy to load.",
)
args = parser.parse_args()

assert args.checkpoint_file, "Must specify flag --checkpoint_file."


def run(checkpoint_path):
    # __sphinx_doc_begin__
    # Restore policy.
    policies = Policy.from_checkpoint(
        checkpoint=checkpoint_path,
        policy_ids=[args.policy_id],
    )
    policy = policies[args.policy_id]

    # Run CartPole.
    env = gym.make("CartPole-v1")
    obs, info = env.reset()
    terminated = truncated = False
    step = 0
    while not terminated and not truncated:
        step += 1

        # Use local_policy_inference() to run inference, so we do not have to
        # provide policy states or extra fetch dictionaries.
        # "env_1" and "agent_1" are dummy env and agent IDs to run connectors with.
        policy_outputs = local_policy_inference(policy, "env_1", "agent_1", obs)
        assert len(policy_outputs) == 1
        action, _, _ = policy_outputs[0]
        print(f"step {step}", obs, action)

        # Step environment forward one more step.
        obs, _, terminated, truncated, _ = env.step(action)
    # __sphinx_doc_end__


if __name__ == "__main__":
    checkpoint_path = str(
        Path(__file__).parent.parent.parent.absolute().joinpath(args.checkpoint_file)
    )
    run(checkpoint_path)
