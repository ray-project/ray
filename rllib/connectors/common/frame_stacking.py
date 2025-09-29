import numpy as np
from typing import Any, Dict, List, Optional

import gymnasium as gym
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class FrameStacking(ConnectorV2):
    """A connector piece that stacks the previous n observations into one."""

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        # Change our observation space according to the given stacking settings.
        if self._multi_agent:
            ret = {}
            for agent_id, obs_space in input_observation_space.spaces.items():
                ret[agent_id] = self._convert_individual_space(obs_space)
            return gym.spaces.Dict(ret)
        else:
            return self._convert_individual_space(input_observation_space)

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        num_frames: int = 1,
        multi_agent: bool = False,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        """Initializes a FrameStackingConnector instance.

        Args:
            num_frames: The number of observation frames to stack up (into a single
                observation) for the RLModule's forward pass.
            multi_agent: Whether this is a connector operating on a multi-agent
                observation space mapping AgentIDs to individual agents' observations.
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to an env-to-module pipeline.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self._multi_agent = multi_agent
        self.num_frames = num_frames
        self._as_learner_connector = as_learner_connector

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
        # Learner connector pipeline. Episodes have been numpy'ized.
        if self._as_learner_connector:
            for sa_episode in self.single_agent_episode_iterator(
                episodes, agents_that_stepped_only=False
            ):

                def _map_fn(s, _sa_episode=sa_episode):
                    # Squeeze out last dim.
                    s = np.squeeze(s, axis=-1)
                    # Calculate new shape and strides
                    new_shape = (len(_sa_episode), self.num_frames) + s.shape[1:]
                    new_strides = (s.strides[0],) + s.strides
                    # Create a strided view of the array.
                    # But return a copy to avoid non-contiguous memory in the object
                    # store (which is very expensive to deserialize).
                    return np.transpose(
                        np.lib.stride_tricks.as_strided(
                            s, shape=new_shape, strides=new_strides
                        ),
                        axes=[0, 2, 3, 1],
                    ).copy()

                # Get all observations from the episode in one np array (except for
                # the very last one, which is the final observation not needed for
                # learning).
                self.add_n_batch_items(
                    batch=batch,
                    column=Columns.OBS,
                    items_to_add=tree.map_structure(
                        _map_fn,
                        sa_episode.get_observations(
                            indices=slice(-self.num_frames + 1, len(sa_episode)),
                            neg_index_as_lookback=True,
                            fill=0.0,
                        ),
                    ),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )

        # Env-to-module pipeline. Episodes still operate on lists.
        else:
            for sa_episode in self.single_agent_episode_iterator(episodes):
                assert not sa_episode.is_numpy
                # Get the list of observations to stack.
                obs_stack = sa_episode.get_observations(
                    indices=slice(-self.num_frames, None),
                    fill=0.0,
                )
                # Observation components are (w, h, 1)
                # -> concatenate along axis=-1 to (w, h, [num_frames]).
                stacked_obs = tree.map_structure(
                    lambda *s: np.concatenate(s, axis=2),
                    *obs_stack,
                )
                self.add_batch_item(
                    batch=batch,
                    column=Columns.OBS,
                    item_to_add=stacked_obs,
                    single_agent_episode=sa_episode,
                )

        return batch

    def _convert_individual_space(self, obs_space):
        # Some assumptions: Space is box AND last dim (the stacking one) is 1.
        assert isinstance(obs_space, gym.spaces.Box), obs_space
        assert obs_space.shape[-1] == 1, obs_space

        return gym.spaces.Box(
            low=np.repeat(obs_space.low, repeats=self.num_frames, axis=-1),
            high=np.repeat(obs_space.high, repeats=self.num_frames, axis=-1),
            shape=list(obs_space.shape)[:-1] + [self.num_frames],
            dtype=obs_space.dtype,
        )
