from typing import Any, Collection, Dict, List, Optional

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree
from gymnasium.spaces import Box

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import AgentID, EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class FlattenObservations(ConnectorV2):
    """A connector piece that flattens all observation components into a 1D array.

    - Can be used either in env-to-module or learner pipelines.
    - When used in env-to-module pipelines:
        - Works directly on the incoming episodes list and changes the last observation
        in-place (write the flattened observation back into the episode).
        - This connector does NOT alter the incoming batch (`data`) when called.
    - When used in learner pipelines:
        Works directly on the incoming episodes list and changes all observations
        before stacking them into the batch.


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
        output_batch = connector(
            rl_module=None,  # This connector works without an RLModule.
            batch={},  # This connector does not alter the input batch.
            episodes=[episode_1, episode_2],
            explore=True,
            shared_data={},
        )

        # The connector does not alter the data and acts as pure pass-through.
        check(output_batch, {})

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

        # Construct our connector piece and remove in it the "a" (Box()) key-value
        # pair from the dictionary observations.
        connector = FlattenObservations(obs_space, act_space, keys_to_remove=["a"])

        # Call our connector piece with the example data.
        output_batch = connector(
            rl_module=None,  # This connector works without an RLModule.
            batch={},  # This connector does not alter the input batch.
            episodes=[episode_1, episode_2],
            explore=True,
            shared_data={},
        )

        # The connector has flattened each item in the episodes to a 1D tensor
        # and removed the "a" (Box()) key-value pair.
        check(
            episode_1.get_observations(0),
            #         disc(2).  box(2, 1).  multidisc(2, 3)........
            np.array([0.0, 1.0, -1.0, -1.0, 1.0, 0.0, 0.0, 0.0, 1.0]),
        )
        check(
            episode_2.get_observations(0),
            #         disc(2).  box(2, 1).  multidisc(2, 3)........
            np.array([1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0]),
        )

        # Use the connector in a learner pipeline. Note, we need here two
        # observations in the episode because the agent has to have stepped
        # at least once.
        episode_1 = SingleAgentEpisode(
            observations=[
                {
                    "a": np.array(-10.0, np.float32),
                    "b": (1, np.array([[-1.0], [-1.0]], np.float32)),
                    "c": np.array([0, 2]),
                },
                {
                    "a": np.array(-10.0, np.float32),
                    "b": (1, np.array([[-1.0], [-1.0]], np.float32)),
                    "c": np.array([0, 2]),
                },
            ],
            actions=[1],
            rewards=[0],
            # Set the length of the lookback buffer to 0 to read the data as
            # from an actual step.
            len_lookback_buffer=0,
        )
        episode_2 = SingleAgentEpisode(
            observations=[
                {
                    "a": np.array(10.0, np.float32),
                    "b": (0, np.array([[1.0], [1.0]], np.float32)),
                    "c": np.array([1, 1]),
                },
                {
                    "a": np.array(10.0, np.float32),
                    "b": (0, np.array([[1.0], [1.0]], np.float32)),
                    "c": np.array([1, 1]),
                },

            ],
            actions=[1],
            rewards=[0],
            # Set the length of the lookback buffer to 0 to read the data as
            # from an actual step.
            len_lookback_buffer=0,
        )

        # Construct our connector piece for a learner pipeline and remove the
        # "a" (Box()) key-value pair.
        connector = FlattenObservations(
            obs_space,
            act_space,
            as_learner_connector=True,
            keys_to_remove=["a"]
        )

        # Call our connector piece with the example data.
        output_batch = connector(
            rl_module=None,  # This connector works without an RLModule.
            batch={},  # This connector does not alter the input batch.
            episodes=[episode_1, episode_2],
            explore=True,
            shared_data={},
        )

        check(list(output_batch.keys()), ["obs"])
        check(list(output_batch["obs"].keys()), [(episode_1.id_,), (episode_2.id_,)])

        check(
            output_batch["obs"][(episode_1.id_,)][0][0],
            np.array([0.0, 1.0, -1.0, -1.0, 1.0, 0.0, 0.0, 0.0, 1.0]),
        )
        check(
            output_batch["obs"][(episode_2.id_,)][0][0],
            np.array([1.0, 0.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0]),
        )
    """

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space,
        input_action_space,
    ) -> gym.Space:
        self._input_obs_base_struct = get_base_struct_from_space(
            self.input_observation_space
        )

        if self._multi_agent:
            spaces = {}
            for agent_id, space in self._input_obs_base_struct.items():
                # Remove keys, if necessary.
                # TODO (simon): Maybe allow to remove different keys for different agents.
                if self._keys_to_remove:
                    self._input_obs_base_struct[agent_id] = {
                        k: v
                        for k, v in self._input_obs_base_struct[agent_id].items()
                        if k not in self._keys_to_remove
                    }
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
            # Remove keys, if necessary.
            if self._keys_to_remove:
                self._input_obs_base_struct = {
                    k: v
                    for k, v in self._input_obs_base_struct.items()
                    if k not in self._keys_to_remove
                }
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
        as_learner_connector: bool = False,
        keys_to_remove: Optional[List[str]] = None,
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
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to an env-to-module pipeline.
                Note, this is usually only used for offline rl where the data comes
                from an offline dataset instead of a simulator. With a simulator the
                data is simply rewritten.
            keys_to_remove: Optional keys to remove from the observations.

        """
        self._input_obs_base_struct = None
        self._multi_agent = multi_agent
        self._agent_ids = agent_ids
        self._as_learner_connector = as_learner_connector

        assert keys_to_remove is None or (
            keys_to_remove
            and isinstance(input_observation_space, gym.spaces.Dict)
            or (
                multi_agent
                and any(
                    isinstance(agent_space, gym.spaces.Dict)
                    for agent_space in self.input_observation_space
                )
            )
        ), "When using `keys_to_remove` the observation space must be of type `gym.spaces.Dict`."
        self._keys_to_remove = keys_to_remove or []

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
        if self._as_learner_connector:
            for sa_episode in self.single_agent_episode_iterator(
                episodes, agents_that_stepped_only=True
            ):

                def _map_fn(obs, _sa_episode=sa_episode):
                    # Remove keys, if necessary.
                    obs = [self._remove_keys_from_dict(o, sa_episode) for o in obs]

                    batch_size = len(sa_episode)
                    flattened_obs = flatten_inputs_to_1d_tensor(
                        inputs=obs,
                        # In the multi-agent case, we need to use the specific agent's
                        # space struct, not the multi-agent observation space dict.
                        spaces_struct=self._input_obs_base_struct,
                        # Our items are individual observations (no batch axis present).
                        batch_axis=False,
                    )
                    return flattened_obs.reshape(batch_size, -1).copy()

                self.add_n_batch_items(
                    batch=batch,
                    column=Columns.OBS,
                    items_to_add=_map_fn(
                        sa_episode.get_observations(indices=slice(0, len(sa_episode))),
                        sa_episode,
                    ),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )

        else:
            for sa_episode in self.single_agent_episode_iterator(
                episodes, agents_that_stepped_only=True
            ):
                last_obs = sa_episode.get_observations(-1)
                # Remove keys, if necessary.
                last_obs = self._remove_keys_from_dict(last_obs, sa_episode)

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
                            spaces_struct=self._input_obs_base_struct[
                                sa_episode.agent_id
                            ],
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

        return batch

    def _remove_keys_from_dict(self, obs, sa_episode):
        """Removes keys from dictionary spaces.

        Args:
            obs: Observation sample from space.
            sa_episode: Single-agent episode. Needs `agent_id` set in multi-agent
                setups.

        Returns:
            Observation sample `obs` with keys in `self._keys_to_remove` removed.
        """

        # Only remove keys for agents that have a dictionary space.
        is_dict_space = False
        if self._multi_agent:
            is_dict_space = isinstance(
                self.input_observation_space[sa_episode.agent_id], gym.spaces.Dict
            )
        else:
            is_dict_space = isinstance(self.input_observation_space, gym.spaces.Dict)

        # Remove keys, if necessary.
        if is_dict_space and self._keys_to_remove:
            obs = {k: v for k, v in obs.items() if k not in self._keys_to_remove}

        return obs
