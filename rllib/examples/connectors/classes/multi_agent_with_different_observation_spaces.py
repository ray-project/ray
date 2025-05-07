from typing import Any, Dict, List, Optional

import gymnasium as gym
import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import AgentID, EpisodeType


class DoubleXYPosToDiscreteIndex(ConnectorV2):
    """Converts double x/y pos (for 2 agents) into discrete position index for 1 agent.

    The env this connector must run with is the
    :py:class:`~ray.rllib.examples.env.classes.multi_agent.double_row_corridor_env.DoubleRowCorridorEnv`  # noqa

    The env has a global observation space, which is a 4-tuple of x/y-position of
    `agent_0` and x/y-position of `agent_1`. This connector converts one of these
    x/y-positions (for the `agent_id` specified in the constructor) into a new
    observation, which is a dict of structure:
    {
        "agent": Discrete index encoding the position of the agent,
        "other_agent_row": Discrete(2), indicating whether the other agent is in row 0
        or row 1,
    }

    The row information for the other agent is needed for learning an optimal policy
    b/c the env rewards the first collision between the two agents. Hence, an agent
    should have information of which row the other agent is currently in, so it can
    change to this row and try to collide with the other agent.
    """

    @override(ConnectorV2)
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
        of the other agent (0 or 1).
        """
        spaces = input_observation_space.spaces.copy()
        agent_space = spaces[self._agent_id]

        # Box.high is inclusive.
        self._env_corridor_len = agent_space.high[self._global_obs_slots[1]] + 1
        # Env has always 2 rows (and `self._env_corridor_len` columns).
        num_discrete = int(2 * self._env_corridor_len)
        spaces[self._agent_id] = gym.spaces.Dict(
            {
                # Exact position of this agent.
                "agent": gym.spaces.Discrete(num_discrete),
                # Row (0 or 1) of other agent.
                "other_agent_row": gym.spaces.Discrete(2),
            }
        )

        return gym.spaces.Dict(spaces)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        agent_id: AgentID,
        **kwargs,
    ):
        """Initializes a XYPosToDiscreteIndex instance.

        Args:
            agent_id: The agent ID, for which to convert the global observation,
                consisting of 2 x/y coordinates for the two agents in the env,
                into a single int index for only that agent's x/y position.
        """
        self._agent_id = agent_id
        self._global_obs_slots = [0, 1] if self._agent_id == "agent_0" else [2, 3]
        self._other_agent_global_obs_slots = (
            [2, 3] if self._agent_id == "agent_0" else [0, 1]
        )

        super().__init__(input_observation_space, input_action_space, **kwargs)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        for sa_episode in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=True
        ):
            if sa_episode.agent_id != self._agent_id:
                continue

            # Observations: positions of both agents (row, col).
            # For example: [0.0, 2.0, 1.0, 4.0] means agent_0 is in position
            # (row=0, col=2) and agent_1 is in position (row=1, col=4).
            last_global_obs = sa_episode.get_observations(-1)

            # [0/2] = row of this agent, [1/3] = col of this agent.
            index_obs = (
                last_global_obs[self._global_obs_slots[0]] * self._env_corridor_len
                + last_global_obs[self._global_obs_slots[1]]
            )
            other_agent_row = last_global_obs[self._other_agent_global_obs_slots[0]]
            new_obs = {
                "agent": index_obs,
                "other_agent_row": other_agent_row,
            }

            # Write new observation directly back into the episode.
            sa_episode.set_observations(at_indices=-1, new_data=new_obs)

            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space[self._agent_id]

        return batch


class DoubleXYPosToSingleXYPos(ConnectorV2):
    """Converts double x/y pos (for 2 agents) into single x/y pos for 1 agent.

    The env this connector must run with is the
    :py:class:`~ray.rllib.examples.env.classes.multi_agent.double_row_corridor_env.DoubleRowCorridorEnv`  # noqa

    The env has a global observation space, which is a 4-tuple of x/y-position of
    `agent_0` and x/y-position of `agent_1`. This connector converts one of these
    x/y-positions (for the `agent_id` specified in the constructor) into a new
    observation, which is a 3-tuple of: x/y-position of the agent and the row
    (0.0 or 1.0) of the other agent.

    The row information for the other agent is needed for learning an optimal policy
    b/c the env rewards the first collision between the two agents. Hence, an agent
    should have information of which row the other agent is currently in, so it can
    change to this row and try to collide with the other agent.
    """

    @override(ConnectorV2)
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
        of the other agent (0 or 1).
        """
        spaces = input_observation_space
        agent_space = spaces[self._agent_id]
        spaces[self._agent_id] = gym.spaces.Box(
            0,
            agent_space.high[self._global_obs_slots[1]],
            shape=(3,),
            dtype=np.float32,
        )
        return gym.spaces.Dict(spaces)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        agent_id: AgentID,
        **kwargs,
    ):
        """Initializes a XYPosToDiscreteIndex instance.

        Args:
            agent_id: The agent ID, for which to convert the global observation,
                consisting of 2 x/y coordinates for the two agents in the env,
                into a single int index for only that agent's x/y position.
        """
        self._agent_id = agent_id
        self._global_obs_slots = [0, 1] if self._agent_id == "agent_0" else [2, 3]
        self._other_agent_global_obs_slots = (
            [2, 3] if self._agent_id == "agent_0" else [0, 1]
        )

        super().__init__(input_observation_space, input_action_space, **kwargs)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        for sa_episode in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=True
        ):
            if sa_episode.agent_id != self._agent_id:
                continue

            # Observations: positions of both agents (row, col).
            # For example: [0.0, 2.0, 1.0, 4.0] means agent_0 is in position (0, 2)
            # and agent_1 is in position (1, 4), where the first number is the row
            # index, the second number is the column index.
            last_global_obs = sa_episode.get_observations(-1)

            # [0/2] = row of this agent, [1/3] = col of this agent.
            xy_obs_plus_other_agent_row = np.array(
                [
                    last_global_obs[self._global_obs_slots[0]],
                    last_global_obs[self._global_obs_slots[1]],
                    last_global_obs[self._other_agent_global_obs_slots[0]],
                ],
                dtype=np.float32,
            )
            # Write new observation directly back into the episode.
            sa_episode.set_observations(
                at_indices=-1,
                new_data=xy_obs_plus_other_agent_row,
            )

            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space[self._agent_id]

        return batch
