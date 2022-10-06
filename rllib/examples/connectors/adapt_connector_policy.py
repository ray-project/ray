"""This example script shows how to load a connector enabled policy,
and adapt/use it with a different version of the environment.
"""

import argparse
import gym
import numpy as np
from pathlib import Path
from typing import Dict

from ray.rllib.connectors.connector import ConnectorContext
from ray.rllib.connectors.action.lambdas import register_lambda_action_connector
from ray.rllib.connectors.agent.lambdas import register_lambda_agent_connector
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.policy import local_policy_inference
from ray.rllib.utils.typing import (
    PolicyOutputType,
    StateBatches,
    TensorStructType,
)


parser = argparse.ArgumentParser()
# A policy checkpoint that works with this example script can be found at:
# rllib/tests/data/checkpoints/APPO_CartPole-v0_checkpoint-6-07092022
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


# __sphinx_doc_begin__
class MyCartPole(gym.Env):
    """A mock CartPole environment.

    Gives 2 additional observation states and takes 2 discrete actions.
    """

    def __init__(self):
        self._env = gym.make("CartPole-v0")
        self.observation_space = gym.spaces.Box(low=-10, high=10, shape=(6,))
        self.action_space = gym.spaces.MultiDiscrete(nvec=[2, 2])

    def step(self, actions):
        # Take the first action.
        action = actions[0]
        obs, reward, done, info = self._env.step(action)
        # Fake additional data points to the obs.
        obs = np.hstack((obs, [8.0, 6.0]))
        return obs, reward, done, info

    def reset(self):
        return np.hstack((self._env.reset(), [8.0, 6.0]))


# Custom agent connector to drop the last 2 feature values.
def v2_to_v1_obs(data: Dict[str, TensorStructType]) -> Dict[str, TensorStructType]:
    data[SampleBatch.NEXT_OBS] = data[SampleBatch.NEXT_OBS][:-2]
    return data


# Agent connector that adapts observations from the new CartPole env
# into old format.
V2ToV1ObsAgentConnector = register_lambda_agent_connector(
    "V2ToV1ObsAgentConnector", v2_to_v1_obs
)


# Custom action connector to add a placeholder action as the addtional action input.
def v1_to_v2_action(
    actions: TensorStructType, states: StateBatches, fetches: Dict
) -> PolicyOutputType:
    return np.hstack((actions, [0])), states, fetches


# Action connector that adapts action outputs from the old policy
# into new actions for the mock environment.
V1ToV2ActionConnector = register_lambda_action_connector(
    "V1ToV2ActionConnector", v1_to_v2_action
)


def run(checkpoint_path):
    # Restore policy.
    policies = Policy.from_checkpoint(
        checkpoint=checkpoint_path,
        policy_ids=[args.policy_id],
    )
    policy = policies[args.policy_id]

    # Adapt policy trained for standard CartPole to the new env.
    ctx: ConnectorContext = ConnectorContext.from_policy(policy)

    # When this policy was trained, it relied on FlattenDataAgentConnector
    # to add a batch dimension to single observations.
    # This is not necessary anymore, so we first remove the previously used
    # FlattenDataAgentConnector.
    policy.agent_connectors.remove("FlattenDataAgentConnector")

    # We then add the two adapter connectors.
    policy.agent_connectors.prepend(V2ToV1ObsAgentConnector(ctx))
    policy.action_connectors.append(V1ToV2ActionConnector(ctx))

    # Run CartPole.
    env = MyCartPole()
    obs = env.reset()
    done = False
    step = 0
    while not done:
        step += 1

        # Use local_policy_inference() to easily run poicy with observations.
        policy_outputs = local_policy_inference(policy, "env_1", "agent_1", obs)
        assert len(policy_outputs) == 1
        actions, _, _ = policy_outputs[0]
        print(f"step {step}", obs, actions)

        obs, _, done, _ = env.step(actions)


# __sphinx_doc_end__


if __name__ == "__main__":
    checkpoint_path = str(
        Path(__file__).parent.parent.parent.absolute().joinpath(args.checkpoint_file)
    )
    run(checkpoint_path)
