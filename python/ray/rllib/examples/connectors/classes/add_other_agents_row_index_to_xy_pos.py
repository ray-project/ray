from typing import Any

import gymnasium as gym
import numpy as np

from ray.rllib.connectors.env_to_module.observation_preprocessor import (
    MultiAgentObservationPreprocessor,
)
from ray.rllib.utils.annotations import override


class AddOtherAgentsRowIndexToXYPos(MultiAgentObservationPreprocessor):
    """Adds other agent's row index to an x/y-observation for an agent.

    Run this connector with this env:
    :py:class:`~ray.rllib.examples.env.classes.multi_agent.double_row_corridor_env.DoubleRowCorridorEnv`  # noqa

    In this env, 2 agents walk around in a grid-world and must, each separately, reach
    their individual goal position to receive a final reward. However, if they collide
    while search for these goal positions, another larger reward is given to both
    agents. Thus, optimal policies aim at seeking the other agent first, and only then
    proceeding to their agent's goal position.

    Each agents' observation space is a 2-tuple encoding the x/y position
    (x=row, y=column).
    This connector converts these observations to:
    A dict for `agent_0` of structure:
    {
        "agent": Discrete index encoding the position of the agent,
        "other_agent_row": Discrete(2), indicating whether the other agent is in row 0
        or row 1,
    }
    And a 3-tuple for `agent_1`, encoding the x/y position of `agent_1` plus the row
    index (0 or 1) of `agent_0`.

    Note that the row information for the respective other agent, which this connector
    provides, is needed for learning an optimal policy for any of the agents, because
    the env rewards the first collision between the two agents. Hence, an agent needs to
    have information on which row the respective other agent is currently in, so it can
    change to this row and try to collide with this other agent.
    """

    @override(MultiAgentObservationPreprocessor)
    def recompute_output_observation_space(
        self,
        input_observation_space,
        input_action_space,
    ) -> gym.Space:
        """Maps the original (input) observation space to the new one.

        Original observation space is `Dict({agent_n: Box(4,), ...})`.
        Converts the space for `self.agent` into information specific to this agent,
        plus the current row of the respective other agent.
        Output observation space is then:
        `Dict({`agent_n`: Dict(Discrete, Discrete), ...}), where the 1st Discrete
        is the position index of the agent and the 2nd Discrete encodes the current row
        of the other agent (0 or 1). If the other agent is already done with the episode
        (has reached its goal state) a special value of 2 is used.
        """
        agent_0_space = input_observation_space.spaces["agent_0"]
        self._env_corridor_len = agent_0_space.high[1] + 1  # Box.high is inclusive.
        # Env has always 2 rows (and `self._env_corridor_len` columns).
        num_discrete = int(2 * self._env_corridor_len)
        spaces = {
            "agent_0": gym.spaces.Dict(
                {
                    # Exact position of this agent (as an int index).
                    "agent": gym.spaces.Discrete(num_discrete),
                    # Row (0 or 1) of other agent. Or 2, if other agent is already done.
                    "other_agent_row": gym.spaces.Discrete(3),
                }
            ),
            "agent_1": gym.spaces.Box(
                0,
                agent_0_space.high[1],  # 1=column
                shape=(3,),
                dtype=np.float32,
            ),
        }
        return gym.spaces.Dict(spaces)

    @override(MultiAgentObservationPreprocessor)
    def preprocess(self, observations, episode) -> Any:
        # Observations: dict of keys "agent_0" and "agent_1", mapping to the respective
        # x/y positions of these agents (x=row, y=col).
        # For example: [1.0, 4.0] means the agent is in row 1 and column 4.

        new_obs = {}
        # 2=agent is already done
        row_agent_0 = observations.get("agent_0", [2])[0]
        row_agent_1 = observations.get("agent_1", [2])[0]

        if "agent_0" in observations:
            # Compute `agent_0` and `agent_1` enhanced observation.
            index_obs_agent_0 = (
                observations["agent_0"][0] * self._env_corridor_len
                + observations["agent_0"][1]
            )
            new_obs["agent_0"] = {
                "agent": index_obs_agent_0,
                "other_agent_row": row_agent_1,
            }

        if "agent_1" in observations:
            new_obs["agent_1"] = np.array(
                [
                    observations["agent_1"][0],
                    observations["agent_1"][1],
                    row_agent_0,
                ],
                dtype=np.float32,
            )

        return new_obs
