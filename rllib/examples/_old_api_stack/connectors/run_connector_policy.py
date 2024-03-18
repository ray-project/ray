"""This example script shows how to load a connector enabled policy,
and use it in a serving/inference setting.
"""

import gymnasium as gym
import os
import tempfile

from ray.rllib.examples._old_api_stack.connectors.prepare_checkpoint import (
    # For demo purpose only. Would normally not need this.
    create_appo_cartpole_checkpoint,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.policy import local_policy_inference


def run(checkpoint_path, policy_id):
    # __sphinx_doc_begin__
    # Restore policy.
    policy = Policy.from_checkpoint(
        checkpoint=checkpoint_path,
        policy_ids=[policy_id],
    )

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
        policy_outputs = local_policy_inference(
            policy, "env_1", "agent_1", obs, explore=False
        )
        assert len(policy_outputs) == 1
        action, _, _ = policy_outputs[0]
        print(f"step {step}", obs, action)

        # Step environment forward one more step.
        obs, _, terminated, truncated, _ = env.step(action)
    # __sphinx_doc_end__


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmpdir:
        policy_id = "default_policy"

        # Note, this is just for demo purpose.
        # Normally, you would use a policy checkpoint from a real training run.
        create_appo_cartpole_checkpoint(tmpdir)
        policy_checkpoint_path = os.path.join(
            tmpdir,
            "policies",
            policy_id,
        )

        run(policy_checkpoint_path, policy_id)
