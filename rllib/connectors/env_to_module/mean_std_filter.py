from typing import Any, Dict, List, Optional
from gymnasium.spaces import Discrete, MultiDiscrete

import gymnasium as gym
import numpy as np
import tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.filter import Filter, MeanStdFilter as _MeanStdFilter
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI
from ray.rllib.utils.filter import RunningStat


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
    def __init__(
        self,
        *,
        de_mean_to_zero: bool = True,
        de_std_to_one: bool = True,
        clip_by_value: Optional[float] = 10.0,
        update_stats: bool = True,
        **kwargs,
    ):
        """Initializes a MeanStdFilter instance.

        Args:
            de_mean_to_zero: Whether to transform the mean values of the output data to
                0.0. This is done by subtracting the incoming data by the currently
                stored mean value.
            de_std_to_one: Whether to transform the standard deviation values of the
                output data to 1.0. This is done by dividing the incoming data by the
                currently stored std value.
            clip_by_value: If not None, clip the incoming data within the interval:
                [-clip_by_value, +clip_by_value].
            update_stats: Whether to update the internal mean and std stats with each
                incoming sample (with each `__call__()`) or not. For example, you should
                set this to False if you would like to perform inference in a
                production environment, without continuing to "learn" stats from new
                data.
        """
        super().__init__(**kwargs)

        # We simply use the old MeanStdFilter until non-connector env_runner is fully
        # deprecated to avoid duplicate code

        self._input_observation_space_struct = (
            get_base_struct_from_space(self.input_observation_space)
        )
        self.filter_shape = tree.map_structure(
            lambda s: (
                None
                if isinstance(s, (Discrete, MultiDiscrete))  # noqa
                else np.array(s.shape)
            ),
            self._input_observation_space_struct,
        )
        self.de_mean_to_zero = de_mean_to_zero
        self.de_std_to_one = de_std_to_one
        self.clip_by_value = clip_by_value
        self._update_stats = update_stats

        # Adjust our observation space's Boxes (only if clipping is active).
        _observation_space_struct = tree.map_structure(
            lambda s: (
                s if not isinstance(s, gym.spaces.Box)
                else gym.spaces.Box(
                    low=-self.clip_by_value,
                    high=self.clip_by_value,
                    shape=s.shape,
                    dtype=s.dtype,
                )
            ),
            self._input_observation_space_struct,
        )
        if isinstance(self.observation_space, (gym.spaces.Dict, gym.spaces.Tuple)):
            type(self.observation_space)(_observation_space_struct)
        else:
            self.observation_space = _observation_space_struct

        self._filter: Optional[_MeanStdFilter] = None
        self._init_new_filter()

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # This connector acts as a classic postprocessor. We process and then replace
        # observations inside the episodes directly. Thus, all following connectors
        # will only see and operate the already normalized data (w/o having access
        # anymore to the original observations).
        for episode in episodes:
            observations = episode.get_observations(indices=-1)
            normalized_observations = self._filter(
                observations, update=self._update_stats
            )
            # TODO (sven): This is kind of a hack.
            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error). However, this would NOT work if our
            #  space were to be more more restrictive than the env's original space
            #  b/c then the adding of the original env observation would fail.
            #episode.observation_space = episode.observations.space = self.observation_space
            # TODO (sven): Add setter APIs to multi-agent episode.
            if isinstance(episode, MultiAgentEpisode):
                for agent_id, val in normalized_observations.items():
                    episode.agent_episodes[agent_id].set_observations(
                        new_data=val,
                        at_indices=-1,
                    )
            else:
                episode.set_observations(
                    new_data=normalized_observations,
                    at_indices=-1,
                )

        # Leave the `input_` as is. RLlib's default connector will automatically
        # populate the OBS column therein from the episodes' (transformed) observations.
        return data

    def get_state(self) -> Any:
        return self._get_state_from_filter(self._filter)

    @override(ConnectorV2)
    def set_state(self, state: Dict[str, Any]) -> None:
        self._filter.shape = state["shape"]
        self._filter.demean = state["de_mean_to_zero"]
        self._filter.destd = state["de_std_to_one"]
        self._filter.clip = state["clip_by_value"]
        running_stats = [RunningStat.from_state(s) for s in state["running_stats"]]
        self._filter.running_stats = tree.unflatten_as(
            self._filter.shape, running_stats
        )
        buffer = [RunningStat.from_state(s) for s in state["buffer"]]
        self._filter.buffer = tree.unflatten_as(self._filter.shape, buffer)

    @override(ConnectorV2)
    def reset_state(self) -> None:
        """Creates copy of current state and resets accumulated state"""
        if not self._update_stats:
            raise ValueError(
                f"State of {type(self).__name__} can only be changed when "
                f"`update_stats` was set to False."
            )
        self._init_new_filter()

    @override(ConnectorV2)
    @staticmethod
    def merge_states(states: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Make sure data is uniform across given states.
        ref = states[0]
        assert all(
            s["shape"] == ref["shape"]
            and s["de_mean_to_zero"] == ref["de_mean_to_zero"]
            and s["de_std_to_one"] == ref["de_std_to_one"]
            and s["clip_by_value"] == ref["clip_by_value"]
            for s in states
        )

        filter = None
        for state in states:
            _other_filter = _MeanStdFilter(
                ref["shape"],
                demean=ref["de_mean_to_zero"],
                destd=ref["de_std_to_one"],
                clip=ref["clip"],
            )
            # Override running stats of the filter with the ones stored in `state`.
            _other_filter.running_stats = (
                tree.map_structure(lambda s: RunningStat.from_state(s), state["shape"])
            )
            # Initialize `filter`, if necessary.
            if filter is None:
                filter = _other_filter
                continue

            # Update `filter` with all `_filter`s.
            filter.apply_changes(_other_filter, with_buffer=False)

        return MeanStdFilter._get_state_from_filter(filter)

    def _init_new_filter(self):
        self._filter = _MeanStdFilter(
            self.filter_shape,
            demean=self.de_mean_to_zero,
            destd=self.de_std_to_one,
            clip=self.clip_by_value,
        )

    @staticmethod
    def _get_state_from_filter(filter):
        flattened_rs = tree.flatten(filter.running_stats)
        flattened_buffer = tree.flatten(filter.buffer)
        return {
            "shape": filter.shape,
            "de_mean_to_zero": filter.demean,
            "de_std_to_one": filter.destd,
            "clip_by_value": filter.clip,
            "running_stats": [s.to_state() for s in flattened_rs],
            "buffer": [s.to_state() for s in flattened_buffer],
        }
