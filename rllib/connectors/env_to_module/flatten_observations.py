from typing import Any, Collection, List, Optional

import gymnasium as gym
from gymnasium.spaces import Box
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import AgentID, EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class FlattenObservations(ConnectorV2):
    """A connector piece that flattens all observation components into a 1D array.

    - Works directly on the incoming episodes list and changes the last observation
    in-place (write the flattened observation back into the episode).
    - This connector does NOT alter the incoming batch (`data`) when called.
    - This connector does NOT work in a `LearnerConnectorPipeline` because it requires
    the incoming episodes to still be ongoing (in progress) as it only alters the
    latest observation, not all observations in an episode.

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from ray.rllib.connectors.env_to_module import FlattenObservations
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode
        from ray.rllib.utils.test_utils import check

        # Some arbitrarily nested, complex observation space.
        obs_space = gym.spaces.Dict({
            "a": gym.spaces.Box(-10.0, 10.0, (), np.float32),
            "b": gym.spaces.Tuple([
                gym.spaces.Discrete(2),
                gym.spaces.Box(-1.0, 1.0, (2, 1), np.float32),
            ]),
            "c": gym.spaces.MultiDiscrete([2, 3]),
        })
        act_space = gym.spaces.Discrete(2)

        # Two example episodes, both with initial (reset) observations coming from the
        # above defined observation space.
        episode_1 = SingleAgentEpisode(
            observations=[
                {
                    "a": np.array(-10.0, np.float32),
                    "b": (1, np.array([[-1.0], [-1.0]], np.float32)),
                    "c": np.array([0, 2]),
                },
            ],
        )
        episode_2 = SingleAgentEpisode(
            observations=[
                {
                    "a": np.array(10.0, np.float32),
                    "b": (0, np.array([[1.0], [1.0]], np.float32)),
                    "c": np.array([1, 1]),
                },
            ],
        )

        # Construct our connector piece.
        connector = FlattenObservations(obs_space, act_space)

        # Call our connector piece with the example data.
        output_data = connector(
            rl_module=None,  # This connector works without an RLModule.
            data={},  # This connector does not alter any data.
            episodes=[episode_1, episode_2],
            explore=True,
            shared_data={},
        )

        # The connector does not alter the data and acts as pure pass-through.
        check(output_data, {})

        # The connector has flattened each item in the episodes to a 1D tensor.
        check(
            episode_1.get_observations(0),
            #         box()  disc(2).  box(2, 1).  multidisc(2, 3)........
            np.array([-10.0, 0.0, 1.0, -1.0, -1.0, 1.0, 0.0, 0.0, 0.0, 1.0]),
        )
        check(
            episode_2.get_observations(0),
            #         box()  disc(2).  box(2, 1).  multidisc(2, 3)........
            np.array([10.0, 1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0]),
        )
    """

    @override(ConnectorV2)
    def recompute_observation_space_from_input_spaces(self):
        self._input_obs_base_struct = get_base_struct_from_space(
            self.input_observation_space
        )
        if self._multi_agent:
            spaces = {}
            for agent_id, space in self._input_obs_base_struct.items():
                if self._agent_ids and agent_id not in self._agent_ids:
                    spaces[agent_id] = self._input_obs_base_struct[agent_id]
                else:
                    sample = flatten_inputs_to_1d_tensor(
                        tree.map_structure(
                            lambda s: s.sample(),
                            self._input_obs_base_struct[agent_id],
                        ),
                        self._input_obs_base_struct[agent_id],
                        batch_axis=False,
                    )
                    spaces[agent_id] = Box(
                        float("-inf"), float("inf"), (len(sample),), np.float32
                    )
            return gym.spaces.Dict(spaces)
        else:
            sample = flatten_inputs_to_1d_tensor(
                tree.map_structure(
                    lambda s: s.sample(),
                    self._input_obs_base_struct,
                ),
                self._input_obs_base_struct,
                batch_axis=False,
            )
            return Box(float("-inf"), float("inf"), (len(sample),), np.float32)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        multi_agent: bool = False,
        agent_ids: Optional[Collection[AgentID]] = None,
        **kwargs,
    ):
        """Initializes a FlattenObservations instance.

        Args:
            multi_agent: Whether this connector operates on multi-agent observations,
                in which case, the top-level of the Dict space (where agent IDs are
                mapped to individual agents' observation spaces) is left as-is.
            agent_ids: If multi_agent is True, this argument defines a collection of
                AgentIDs for which to flatten. AgentIDs not in this collection are
                ignored.
                If None, flatten observations for all AgentIDs. None is the default.
        """
        self._input_obs_base_struct = None
        self._multi_agent = multi_agent
        self._agent_ids = agent_ids

        super().__init__(input_observation_space, input_action_space, **kwargs)

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        for sa_episode in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=True
        ):
            # Episode is not finalized yet and thus still operates on lists of items.
            assert not sa_episode.is_finalized

            last_obs = sa_episode.get_observations(-1)

            if self._multi_agent:
                if (
                    self._agent_ids is not None
                    and sa_episode.agent_id not in self._agent_ids
                ):
                    flattened_obs = last_obs
                else:
                    flattened_obs = flatten_inputs_to_1d_tensor(
                        inputs=last_obs,
                        # In the multi-agent case, we need to use the specific agent's
                        # space struct, not the multi-agent observation space dict.
                        spaces_struct=self._input_obs_base_struct[sa_episode.agent_id],
                        # Our items are individual observations (no batch axis present).
                        batch_axis=False,
                    )
            else:
                flattened_obs = flatten_inputs_to_1d_tensor(
                    inputs=last_obs,
                    spaces_struct=self._input_obs_base_struct,
                    # Our items are individual observations (no batch axis present).
                    batch_axis=False,
                )

            # Write new observation directly back into the episode.
            sa_episode.set_observations(at_indices=-1, new_data=flattened_obs)
            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space

        return data
