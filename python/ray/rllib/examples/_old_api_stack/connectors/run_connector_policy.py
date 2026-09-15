# @OldAPIStack
"""This example script loads a connector enabled policy,
and uses it in a serving or inference setting.
"""

import argparse
import os
import tempfile

import gymnasium as gym

from ray.rllib.examples._old_api_stack.connectors.prepare_checkpoint import (
    # For demo purpose only. Would normally not need this.
    create_appo_cartpole_checkpoint,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.policy import local_policy_inference

parser = argparse.ArgumentParser()
parser.add_argument("--use-lstm", action="store_true", help="Add LSTM to the setup.")


def run(checkpoint_path, policy_id):
    # __sphinx_doc_begin__
    # Restore policy.
    policy = Policy.from_checkpoint(
        checkpoint=checkpoint_path,
        policy_ids=[policy_id],
    )

    # Run CartPole.
    env = gym.make("CartPole-v1")
    env_id = "env_1"
    obs, info = env.reset()
    # Run for 2 episodes.
    episodes = step = 0
    while episodes < 2:
        # Use local_policy_inference() to run inference, so we do not have to
        # provide policy states or extra fetch dictionaries.
        # "env_1" and "agent_1" are dummy env and agent IDs to run connectors with.
        policy_outputs = local_policy_inference(
            policy, env_id, "agent_1", obs, explore=False
        )
        assert len(policy_outputs) == 1
        action, _, _ = policy_outputs[0]
        print(f"episode {episodes} step {step}", obs, action)

        # Step environment forward one more step.
        obs, _, terminated, truncated, _ = env.step(action)
        step += 1

        # If the episode is done, reset the env and our connectors and start a new
        # episode.
        if terminated or truncated:
            episodes += 1
            step = 0
            obs, info = env.reset()
            policy.agent_connectors.reset(env_id)

    # __sphinx_doc_end__


if __name__ == "__main__":
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        policy_id = "default_policy"

        # Note, this is just for demo purpose.
        # Normally, you would use a policy checkpoint from a real training run.
        create_appo_cartpole_checkpoint(tmpdir, args.use_lstm)
        policy_checkpoint_path = os.path.join(
            tmpdir,
            "policies",
            policy_id,
        )

        run(policy_checkpoint_path, policy_id)
