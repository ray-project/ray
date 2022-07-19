"""This example script shows how to load a connector enabled policy,
and use it in a serving/inference setting.
"""

import argparse
import gym

from ray.rllib.utils.policy import (
    load_policies_from_checkpoint,
    local_policy_inference,
)


parser = argparse.ArgumentParser()
# This should a checkpoint created with connectors enabled.
parser.add_argument(
    "--checkpoint_file",
    help="Path to an RLlib checkpoint file.",
)
parser.add_argument(
    "--policy_id",
    default="default_policy",
    help="ID of policy to load.",
)
args = parser.parse_args()

assert args.checkpoint_file, "Must specify flag --checkpoint_file."


def run():
    # Restore policy.
    policies = load_policies_from_checkpoint(args.checkpoint_file, [args.policy_id])
    policy = policies[args.policy_id]

    # Run CartPole.
    env = gym.make("CartPole-v0")
    obs = env.reset()
    done = False
    step = 0
    while not done:
        step += 1

        # Use local_policy_inference() to run inference, so we do not have to
        # provide policy states or extra fetch dictionaries.
        policy_outputs = local_policy_inference(policy, "env_1", "agent_1", obs)
        assert len(policy_outputs) == 1
        action, _, _ = policy_outputs[0]
        print(f"step {step}", obs, action)

        # Step environment forward one more step.
        obs, _, done, _ = env.step(action[0])


if __name__ == "__main__":
    run()
