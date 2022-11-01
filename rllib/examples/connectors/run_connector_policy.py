"""This example script shows how to load a connector enabled policy,
and use it in a serving/inference setting.
"""

import argparse
import gym
from pathlib import Path

from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch


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

    # Put policy in inference mode, so we don't spend time on training
    # only transformations.
    policy.agent_connectors.in_eval()
    policy.action_connectors.in_eval()

    # Run CartPole.
    env = gym.make("CartPole-v1")
    obs = env.reset()
    done = False
    step = 0

    while not done:
        step += 1
        # Use connectors() to run inference, so we do not have to
        # provide policy states or extra fetch dictionaries.
        # "env_1" and "agent_1" are dummy env and agent IDs to run connectors with.
        actions, state_outs, infos = policy.compute_actions_from_input_dict(
            env_ids=["env_1"],
            agent_ids=["agent_1"],
            input_dict={SampleBatch.OBS: [obs]},
        )
        print(f"step {step}", obs, actions[0])

        # Step environment forward one more step.
        obs, _, done, _ = env.step(actions[0])
    # __sphinx_doc_end__


if __name__ == "__main__":
    checkpoint_path = str(
        Path(__file__).parent.parent.parent.absolute().joinpath(args.checkpoint_file)
    )
    run(checkpoint_path)
