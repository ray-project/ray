from typing import Any, Collection, Dict, List, Optional, Union

import gymnasium as gym
import numpy as np
import tree
from gymnasium.spaces import Discrete, MultiDiscrete

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.filter import MeanStdFilter as _MeanStdFilter, RunningStat
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import AgentID, EpisodeType, StateDict
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class MeanStdFilter(ConnectorV2):
    """A connector used to mean-std-filter observations.

    Incoming observations are filtered such that the output of this filter is on
    average 0.0 and has a standard deviation of 1.0. If the observation space is
    a (possibly nested) dict, this filtering is applied separately per element of
    the observation space (except for discrete- and multi-discrete elements, which
    are left as-is).

    This connector is stateful as it continues to update its internal stats on mean
    and std values as new data is pushed through it (unless `update_stats` is False).
    """

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        _input_observation_space_struct = get_base_struct_from_space(
            input_observation_space
        )

        # Adjust our observation space's Boxes (only if clipping is active).
        _observation_space_struct = tree.map_structure(
            lambda s: (
                s
                if not isinstance(s, gym.spaces.Box)
                else gym.spaces.Box(
                    low=-self.clip_by_value,
                    high=self.clip_by_value,
                    shape=s.shape,
                    dtype=s.dtype,
                )
            ),
            _input_observation_space_struct,
        )
        if isinstance(input_observation_space, (gym.spaces.Dict, gym.spaces.Tuple)):
            return type(input_observation_space)(_observation_space_struct)
        else:
            return _observation_space_struct

    def __init__(
        self,
        *,
        multi_agent: bool = False,
        de_mean_to_zero: bool = True,
        de_std_to_one: bool = True,
        clip_by_value: Optional[float] = 10.0,
        update_stats: bool = True,
        **kwargs,
    ):
        """Initializes a MeanStdFilter instance.

        Args:
            multi_agent: Whether this is a connector operating on a multi-agent
                observation space mapping AgentIDs to individual agents' observations.
            de_mean_to_zero: Whether to transform the mean values of the output data to
                0.0. This is done by subtracting the incoming data by the currently
                stored mean value.
            de_std_to_one: Whether to transform the standard deviation values of the
                output data to 1.0. This is done by dividing the incoming data by the
                currently stored std value.
            clip_by_value: If not None, clip the incoming data within the interval:
                [-clip_by_value, +clip_by_value].
            update_stats: Whether to update the internal mean and std stats with each
                incoming sample (with each `__call__()`) or not. You should set this to
                False if you would like to perform inference in a production
                environment, without continuing to "learn" stats from new data.
        """
        super().__init__(**kwargs)

        self._multi_agent = multi_agent

        # We simply use the old MeanStdFilter until non-connector env_runner is fully
        # deprecated to avoid duplicate code
        self.de_mean_to_zero = de_mean_to_zero
        self.de_std_to_one = de_std_to_one
        self.clip_by_value = clip_by_value
        self._update_stats = update_stats

        self._filters: Optional[Dict[AgentID, _MeanStdFilter]] = None

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        if self._filters is None:
            self._init_new_filters()

        # This connector acts as a classic preprocessor. We process and then replace
        # observations inside the episodes directly. Thus, all following connectors
        # will only see and operate on the already normalized data (w/o having access
        # anymore to the original observations).
        for sa_episode in self.single_agent_episode_iterator(episodes):
            sa_obs = sa_episode.get_observations(indices=-1)
            try:
                normalized_sa_obs = self._filters[sa_episode.agent_id](
                    sa_obs, update=self._update_stats
                )
            except KeyError:
                raise KeyError(
                    "KeyError trying to access a filter by agent ID "
                    f"`{sa_episode.agent_id}`! You probably did NOT pass the "
                    f"`multi_agent=True` flag into the `MeanStdFilter()` constructor. "
                )
            sa_episode.set_observations(at_indices=-1, new_data=normalized_sa_obs)
            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space

        # Leave `batch` as is. RLlib's default connector will automatically
        # populate the OBS column therein from the episodes' now transformed
        # observations.
        return batch

    @override(ConnectorV2)
    def get_state(
        self,
        components: Optional[Union[str, Collection[str]]] = None,
        *,
        not_components: Optional[Union[str, Collection[str]]] = None,
        **kwargs,
    ) -> StateDict:
        if self._filters is None:
            self._init_new_filters()
        return self._get_state_from_filters(self._filters)

    @override(ConnectorV2)
    def set_state(self, state: StateDict) -> None:
        if self._filters is None:
            self._init_new_filters()
        for agent_id, agent_state in state.items():
            filter = self._filters[agent_id]
            filter.shape = agent_state["shape"]
            filter.demean = agent_state["de_mean_to_zero"]
            filter.destd = agent_state["de_std_to_one"]
            filter.clip = agent_state["clip_by_value"]
            filter.running_stats = tree.unflatten_as(
                filter.shape,
                [RunningStat.from_state(s) for s in agent_state["running_stats"]],
            )
            # Do not update the buffer.

    @override(ConnectorV2)
    def reset_state(self) -> None:
        """Creates copy of current state and resets accumulated state"""
        if not self._update_stats:
            raise ValueError(
                f"State of {type(self).__name__} can only be changed when "
                f"`update_stats` was set to False."
            )
        self._init_new_filters()

    @override(ConnectorV2)
    def merge_states(self, states: List[Dict[str, Any]]) -> Dict[str, Any]:
        if self._filters is None:
            self._init_new_filters()

        # Make sure data is uniform across given states.
        ref = next(iter(states[0].values()))

        for state in states:
            for agent_id, agent_state in state.items():
                assert (
                    agent_state["shape"] == ref["shape"]
                    and agent_state["de_mean_to_zero"] == ref["de_mean_to_zero"]
                    and agent_state["de_std_to_one"] == ref["de_std_to_one"]
                    and agent_state["clip_by_value"] == ref["clip_by_value"]
                )

                _filter = _MeanStdFilter(
                    ref["shape"],
                    demean=ref["de_mean_to_zero"],
                    destd=ref["de_std_to_one"],
                    clip=ref["clip_by_value"],
                )
                # Override running stats of the filter with the ones stored in
                # `agent_state`.
                _filter.buffer = tree.unflatten_as(
                    agent_state["shape"],
                    [
                        RunningStat.from_state(stats)
                        for stats in agent_state["running_stats"]
                    ],
                )

                # Leave the buffers as-is, since they should always only reflect
                # what has happened on the particular env runner.
                self._filters[agent_id].apply_changes(_filter, with_buffer=False)

        return MeanStdFilter._get_state_from_filters(self._filters)

    def _init_new_filters(self):
        filter_shape = tree.map_structure(
            lambda s: (
                None if isinstance(s, (Discrete, MultiDiscrete)) else np.array(s.shape)
            ),
            get_base_struct_from_space(self.input_observation_space),
        )
        if not self._multi_agent:
            filter_shape = {None: filter_shape}

        del self._filters
        self._filters = {
            agent_id: _MeanStdFilter(
                agent_filter_shape,
                demean=self.de_mean_to_zero,
                destd=self.de_std_to_one,
                clip=self.clip_by_value,
            )
            for agent_id, agent_filter_shape in filter_shape.items()
        }

    @staticmethod
    def _get_state_from_filters(filters: Dict[AgentID, Dict[str, Any]]):
        ret = {}
        for agent_id, agent_filter in filters.items():
            ret[agent_id] = {
                "shape": agent_filter.shape,
                "de_mean_to_zero": agent_filter.demean,
                "de_std_to_one": agent_filter.destd,
                "clip_by_value": agent_filter.clip,
                "running_stats": [
                    s.to_state() for s in tree.flatten(agent_filter.running_stats)
                ],
            }
        return ret
