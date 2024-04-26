import copy
from collections import defaultdict
from gymnasium.core import ActType, ObsType
import numpy as np
import scipy
from typing import Any, Dict, List, Optional, Tuple, Union

from ray.rllib.core.columns import Columns
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import override, DeveloperAPI
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import AgentID, ModuleID, SampleBatchType


@DeveloperAPI
class MultiAgentEpisodeReplayBuffer(EpisodeReplayBuffer):
    """Multi-agent episode replay buffer that stores episodes by theior IDs.

    This class implements a replay buffer as used in "playing Atari with Deep
    Reinforcement Learning" (Mnih et al., 2013) for multi-agent reinforcement
    learning,

    Each "row" (a slot in a deque) in the buffer is occupied by one episode. If an
    incomplete episode is added to the buffer and then another chunk of that episode is
    added at a later time, the buffer will automatically concatenate the new fragment to
    the original episode. This way, episodes can be completed via subsequent `add`
    calls.

    Sampling returns batches of size B (number of 'rows'), where each row is a tuple
    of the form

    `(o_t, a_t, sum(r_t+1:t+n), o_t+n)`

    where `o_t` is the observation in `t`, `a_t` the action chosen at observation `o_t`,
    `o_t+n` is the observation `n` timesteps later and `sum(r_t+1:t+n)` is the sum of
    all rewards collected over the time steps between `t+1` and `t+n`. The `n`-step can
    be chosen freely when sampling and defaults to `1`. If `n_step` is a tuple it is
    sampled uniformly across the interval defined by the tuple (for each row in the
    batch).

    Each batch contains - in addition to the data tuples presented above - two further
    columns, namely `n_steps` and `weigths`. The former holds the `n_step` used for each
    row in the batch and the latter the (default) weight of `1.0` for each row in the
    batch. The weight is used for weighted loss calculations in the training process.
    """

    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 64,
        **kwargs,
    ):
        """Initializes a multi-agent episode replay buffer.

        Args:
            capacity: The capacity of the replay buffer in number of timesteps.
            batch_size_B: The batch size to sample from the replay buffer.
            batch_length_T: The length of the sampled batch. This feature is not
                yet implemented.
            **kwargs: Additional arguments to pass to the base class.
        """
        # Initialize the base episode replay buffer.
        super().__init__(
            capacity=capacity,
            batch_size_B=batch_size_B,
            batch_length_T=batch_length_T,
            **kwargs,
        )

        # Stores indices of module (single-agent) timesteps. Each index is a tuple
        # of the form:
        #   `(ma_episode_idx, agent_id, timestep)`.
        # This information is stored for each timestep of an episode and is used in
        # the `"independent"`` sampling process. The multi-agent episode index amd the
        # agent ID are used to retrieve the single-agent episode. The timestep is then
        # needed to retrieve the corresponding timestep data from that single-agent
        # episode.
        self._module_to_indices: Dict[
            ModuleID, List[Tuple[int, AgentID, int]]
        ] = defaultdict(list)

        # Stores the number of single-agent timesteps in the buffer.
        self._num_agent_timesteps: int = 0
        # Stores the number of single-agent timesteps per module.
        self._num_module_timesteps: Dict[ModuleID, int] = defaultdict(int)

        # Stores the number of added single-agent timesteps over the
        # lifetime of the buffer.
        self._num_agent_timesteps_added: int = 0
        # Stores the number of added single-agent timesteps per module
        # over the lifetime of the buffer.
        self._num_module_timesteps_added: Dict[ModuleID, int] = defaultdict(int)

        self._num_module_episodes: Dict[ModuleID, int] = defaultdict(int)
        # Stores the number of module episodes evicted. Note, this is
        # important for indexing.
        self._num_module_episodes_evicted: Dict[ModuleID, int] = defaultdict(int)

        # Stores hte number of module timesteps sampled.
        self.sampled_timesteps_per_module: Dict[ModuleID, int] = defaultdict(int)

    @override(EpisodeReplayBuffer)
    def add(
        self,
        episodes: Union[List["MultiAgentEpisode"], "MultiAgentEpisode"],
    ) -> None:
        """Adds episodes to the replay buffer.

        Note, if the incoming episodes' time steps cause the buffer to overflow,
        older episodes are evicted. Because episodes usually come in chunks and
        not complete, this could lead to edge cases (e.g. with very small capacity
        or very long episode length) where the first part of an episode is evicted
        while the next part just comes in.
        In such cases, we evict the complete episode, including the new chunk,
        unless the episode is the last one in the buffer. In the latter case the
        buffer will be allowed to overflow in a temporary fashion, i.e. during
        the next addition of samples to the buffer an attempt is made to fall below
        capacity again.

        The user is advised to select a large enough buffer with regard to the maximum
        expected episode length.

        Args:
            episodes: The multi-agent episodes to add to the replay buffer. Can be a
                single episode or a list of episodes.
        """
        episodes: List["MultiAgentEpisode"] = force_list(episodes)

        new_episode_ids: List[str] = []
        for eps in episodes:
            new_episode_ids.append(eps.id_)
            self._num_timesteps += eps.env_steps()
            self._num_timesteps_added += eps.env_steps()

        # Evict old episodes.
        eps_evicted: List["MultiAgentEpisode"] = []
        eps_evicted_ids: List[Union[str, int]] = []
        eps_evicted_idxs: List[int] = []
        while (
            self._num_timesteps > self.capacity
            and self._num_remaining_episodes(new_episode_ids, eps_evicted_ids) != 1
        ):
            # Evict episode.
            evicted_episode = self.episodes.popleft()
            eps_evicted.append(evicted_episode)
            eps_evicted_ids.append(evicted_episode.id_)
            eps_evicted_idxs.append(self.episode_id_to_index.pop(evicted_episode.id_))
            # If this episode has a new chunk in the new episodes added,
            # we subtract it again.
            # TODO (sven, simon): Should we just treat such an episode chunk
            # as a new episode?
            if evicted_episode.id_ in new_episode_ids:
                new_eps_to_evict = episodes[new_episode_ids.index(evicted_episode.id_)]
                self._num_timesteps -= new_eps_to_evict.env_steps()
                self._num_timesteps_added -= new_eps_to_evict.env_steps()
                episodes.remove(new_eps_to_evict)
            # Remove the timesteps of the evicted episode from the counter.
            self._num_timesteps -= evicted_episode.env_steps()
            self._num_agent_timesteps -= evicted_episode.agent_steps()
            self._num_episodes_evicted += 1
            # Remove the module timesteps of the evicted episode from the counters.
            self._evict_module_episodes(evicted_episode)

        # Add agent and module steps.
        for eps in episodes:
            self._num_agent_timesteps += eps.agent_steps()
            self._num_agent_timesteps_added += eps.agent_steps()
            # Update the module counters by the module timesteps.
            self._update_module_counters(eps)

        # Remove corresponding indices, if episodes were evicted.
        if eps_evicted_idxs:
            new_indices = []
            # Each index 2-tuple is of the form (ma_episode_idx, timestep) and
            # refers to a certain environment timestep in a certain multi-agent
            # episode.
            for idx_tuple in self._indices:
                # If episode index is not from an evicted episode, keep it.
                if idx_tuple[0] not in eps_evicted_idxs:
                    new_indices.append(idx_tuple)
            # Assign the new list of indices.
            self._indices = new_indices
            # Also remove corresponding module indices.
            for module_id, module_indices in self._module_to_indices.items():
                new_module_indices = []
                # Each index 3-tuple is of the form
                # (ma_episode_idx, agent_id, timestep) and refers to a certain
                # agent timestep in a certain multi-agent episode.
                for idx_triplet in module_indices:
                    if idx_triplet[0] not in eps_evicted_idxs:
                        new_module_indices.append(idx_triplet)
                self._module_to_indices[module_id] = new_module_indices

        for eps in episodes:
            eps = copy.deepcopy(eps)
            # If the episode is part of an already existing episode, concatenate.
            if eps.id_ in self.episode_id_to_index:
                eps_idx = self.episode_id_to_index[eps.id_]
                existing_eps = self.episodes[eps_idx]
                existing_len = len(existing_eps)
                self._indices.extend(
                    [
                        (
                            eps_idx,
                            # Note, we add 1 b/c the first timestep is
                            # never sampled.
                            existing_len + i + 1,
                        )
                        for i in range(len(eps))
                    ]
                )
                # Add new module indices.
                self._add_new_module_indices(eps, eps_idx, True)
                # Concatenate the episode chunk.
                existing_eps.concat_episode(eps)
            # Otherwise, create a new entry.
            else:
                # New episode.
                self.episodes.append(eps)
                eps_idx = len(self.episodes) - 1 + self._num_episodes_evicted
                self.episode_id_to_index[eps.id_] = eps_idx
                self._indices.extend([(eps_idx, i + 1) for i in range(len(eps))])
                # Add new module indices.
                self._add_new_module_indices(eps, eps_idx, False)

    @override(EpisodeReplayBuffer)
    def sample(
        self,
        num_items: Optional[int] = None,
        *,
        batch_size_B: Optional[int] = None,
        batch_length_T: Optional[int] = None,
        n_step: Optional[Union[int, Tuple]] = 1,
        gamma: float = 0.99,
        include_infos: bool = False,
        include_extra_model_outputs: bool = False,
        replay_mode: str = "independent",
        modules_to_sample: Optional[List[ModuleID]] = None,
    ) -> SampleBatchType:
        """Samples a batch of multi-agent transitions.

        Multi-agent transitions can be sampled either `"independent"` or
        `"synchronized"` with the former sampling for each module independent agent
        steps and the latter sampling agent transitions from the same environment step.

        The n-step parameter can be either a single integer or a tuple of two integers.
        In the former case, the n-step is fixed to the given integer and in the latter
        case, the n-step is sampled uniformly from the given range. Large n-steps could
        potentially lead to a many retries because not all samples might have a full
        n-step transition.

        Sampling returns batches of size B (number of 'rows'), where each row is a tuple
        of the form

        `(o_t, a_t, sum(r_t+1:t+n), o_t+n)`

        where `o_t` is the observation in `t`, `a_t` the action chosen at observation
        `o_t`, `o_t+n` is the observation `n` timesteps later and `sum(r_t+1:t+n)` is
        the sum of all rewards collected over the time steps between `t+1` and `t+n`.
        The n`-step can be chosen freely when sampling and defaults to `1`. If `n_step`
        is a tuple it is sampled uniformly across the interval defined by the tuple (for
        each row in the batch).

        Each batch contains - in addition to the data tuples presented above - two
        further columns, namely `n_steps` and `weigths`. The former holds the `n_step`
        used for each row in the batch and the latter a (default) weight of `1.0` for
        each row in the batch. This weight is used for weighted loss calculations in
        the training process.

        Args:
            num_items: The number of items to sample. If provided, `batch_size_B`
                should be `None`.
            batch_size_B: The batch size to sample. If provided, `num_items`
                should be `None`.
            batch_length_T: The length of the sampled batch. If not provided, the
                default batch length is used. This feature is not yet implemented.
            n_step: The n-step to sample. If the n-step is a tuple, the n-step is
                sampled uniformly from the given range. If not provided, the default
                n-step of `1` is used.
            gamma: The discount factor for the n-step reward calculation.
            include_infos: Whether to include the infos in the sampled batch.
            include_extra_model_outputs: Whether to include the extra model outputs
                in the sampled batch.
            replay_mode: The replay mode to use for sampling. Either `"independent"`
                or `"synchronized"`.
            modules_to_sample: A list of module IDs to sample from. If not provided,
                transitions for aall modules are sampled.

        Returns:
            A dictionary of the form `ModuleID -> SampleBatchType` containing the
            sampled data for each module or each module in `modules_to_sample`,
            if provided.
        """
        if num_items is not None:
            assert batch_size_B is None, (
                "Cannot call `sample()` with both `num_items` and `batch_size_B` "
                "provided! Use either one."
            )
            batch_size_B = num_items

        # Use our default values if no sizes/lengths provided.
        batch_size_B = batch_size_B or self.batch_size_B
        # TODO (simon): Implement trajectory sampling for RNNs.
        batch_length_T = batch_length_T or self.batch_length_T

        # Sample for each module independently.
        if replay_mode == "independent":
            return self._sample_independent(
                batch_size_B=batch_size_B,
                batch_length_T=batch_length_T,
                n_step=n_step,
                gamma=gamma,
                include_infos=include_infos,
                include_extra_model_outputs=include_extra_model_outputs,
                modules_to_sample=modules_to_sample,
            )
        else:
            return self._sample_synchonized(
                batch_size_B=batch_size_B,
                batch_length_T=batch_length_T,
                n_step=n_step,
                gamma=gamma,
                include_infos=include_infos,
                include_extra_model_outputs=include_extra_model_outputs,
                modules_to_sample=modules_to_sample,
            )

    def get_added_agent_timesteps(self) -> int:
        """Returns number of agent timesteps that have been added in buffer's lifetime.

        Note, this could be more than the `get_added_timesteps` returns as an
        environment timestep could contain multiple agent timesteps (for eaxch agent
        one).
        """
        return self._num_agent_timesteps_added

    def get_module_ids(self) -> List[ModuleID]:
        """Returns a list of module IDs stored in the buffer."""
        return list(self._module_to_indices.keys())

    def get_num_agent_timesteps(self) -> int:
        """Returns number of agent timesteps stored in the buffer.

        Note, this could be more than the `num_timesteps` as an environment timestep
        could contain multiple agent timesteps (for eaxch agent one).
        """
        return self._num_agent_timesteps

    @override(EpisodeReplayBuffer)
    def get_num_episodes(self, module_id: ModuleID = None) -> int:
        """Returns number of episodes stored for a module in the buffer.

        Note, episodes could be either complete or truncated.

        Args:
            module_id: The ID of the module to query. If not provided, the number of
                episodes for all modules is returned.

        Returns:
            The number of episodes stored for the module or all modules.
        """
        return (
            self._num_module_episodes[module_id]
            if module_id
            else super().get_num_episodes()
        )

    def get_num_timesteps(self, module_id: ModuleID = None) -> int:
        """Returns number of individual timesteps for a module stored in the buffer.

        Args:
            module_id: The ID of the module to query. If not provided, the number of
                timesteps for all modules are returned.

        Returns:
            The number of timesteps stored for the module or all modules.
        """
        return (
            self._num_module_timesteps[module_id]
            if module_id
            else super().get_num_timesteps()
        )

    def get_sampled_timesteps(self, module_id: ModuleID = None) -> int:
        """Returns number of timesteps that have been sampled for a module.

        Args:
            module_id: The ID of the module to query. If not provided, the number of
                sampled timesteps for all modules are returned.

        Returns:
            The number of timesteps sampled for the module or all modules.
        """
        return (
            self.sampled_timesteps_per_module[module_id]
            if module_id
            else super().get_sampled_timesteps()
        )

    def get_added_timesteps(self, module_id: ModuleID = None) -> int:
        """Returns number of timesteps that have been added in buffer's lifetime for a module.

        Args:
            module_id: The ID of the module to query. If not provided, the number of


        Returns:
            The number of timesteps added for the module or all modules.
        """
        return (
            self._num_module_timesteps_added[module_id]
            if module_id
            else super().get_added_timesteps()
        )

    @override(EpisodeReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        return super().get_state() | {
            "_module_to_indices": list(self._module_to_indices.items()),
            "_num_agent_timesteps": self._num_agent_timesteps,
            "_num_agent_timesteps_added": self._num_agent_timesteps_added,
            "_num_module_timesteps": list(self._num_module_timesteps.items()),
            "_num_module_timesteps_added": list(
                self._num_module_timesteps_added.items()
            ),
            "_num_module_episodes": list(self._num_module_episodes.items()),
            "_num_module_episodes_evicted": list(
                self._num_module_episodes_evicted.items()
            ),
            "sampled_timesteps_per_module": list(
                self.sampled_timesteps_per_module.items()
            ),
        }

    @override(EpisodeReplayBuffer)
    def set_state(self, state) -> None:
        # Set the super's state.
        super().set_state(state)
        # Now set the remaining attributes.
        self._module_to_indices = dict(state["_module_to_indices"])
        self._num_agent_timesteps = state["_num_agent_timesteps"]
        self._num_agent_timesteps_added = state["_num_agent_timesteps_added"]
        self._num_module_timesteps = dict(state["_num_module_timesteps"])
        self._num_module_timesteps_added = dict(state["_num_module_timesteps_added"])
        self._num_module_episodes = dict(state["_num_module_episodes"])
        self._num_module_episodes_evicted = dict(state["_num_module_episodes_evicted"])
        self.sampled_timesteps_per_module = dict(state["sampled_timesteps_per_module"])

    def _sample_independent(
        self,
        batch_size_B: Optional[int],
        batch_length_T: Optional[int],
        n_step: Optional[Union[int, Tuple]],
        gamma: float,
        include_infos: bool,
        include_extra_model_outputs: bool,
        modules_to_sample: Optional[List[ModuleID]],
    ) -> SampleBatchType:
        """Samples a batch of independent multi-agent transitions."""
        # Sample the n-step if necessary.
        if isinstance(n_step, tuple):
            # Use random n-step sampling.
            random_n_step = True
        else:
            actual_n_step = n_step or 1
            random_n_step = False

        ret = {}
        # TODO (simon): Ensure that the module has data and if not, skip it.
        #  TODO (sven): Should we then error out or skip? I think the Learner
        #  should handle this case when a module has no train data.
        for module_id in modules_to_sample or self._module_to_indices.keys():
            # Rows to return.
            observations: List[List[ObsType]] = [[] for _ in range(batch_size_B)]
            next_observations: List[List[ObsType]] = [[] for _ in range(batch_size_B)]
            actions: List[List[ActType]] = [[] for _ in range(batch_size_B)]
            rewards: List[List[float]] = [[] for _ in range(batch_size_B)]
            is_terminated: List[bool] = [False for _ in range(batch_size_B)]
            is_truncated: List[bool] = [False for _ in range(batch_size_B)]
            weights: List[float] = [[1.0] for _ in range(batch_size_B)]
            n_steps: List[List[int]] = [[] for _ in range(batch_size_B)]
            # If `info` should be included, construct also a container for them.
            if include_infos:
                infos: List[List[Dict[str, Any]]] = [[] for _ in range(batch_size_B)]
            # If `extra_model_outputs` should be included, construct a container for
            # them.
            if include_extra_model_outputs:
                extra_model_outputs: List[List[Dict[str, Any]]] = [
                    [] for _ in range(batch_size_B)
                ]
            B = 0
            while B < batch_size_B:
                # Now sample from the single-agent timesteps.
                index_tuple = self._module_to_indices[module_id][
                    self.rng.integers(len(self._module_to_indices[module_id]))
                ]

                # This will be an agent timestep (not env timestep).
                # TODO (simon, sven): Maybe deprecate sa_episode_idx (_) in the index
                #   quads. Is there any need for it?
                ma_episode_idx, agent_id, sa_episode_ts = (
                    index_tuple[0] - self._num_episodes_evicted,
                    index_tuple[1],
                    index_tuple[2],
                )
                # If we cannnot make the n-step, we resample.
                if sa_episode_ts - n_step < 0:
                    continue
                # If we use random n-step sampling, draw the n-step for this item.
                if random_n_step:
                    actual_n_step = int(self.rng.integers(n_step[0], n_step[1]))
                # If we are at the end of an episode, continue.
                # Note, priority sampling got us `o_(t+n)` and we need for the loss
                # calculation in addition `o_t`.
                # TODO (simon): Maybe introduce a variable `num_retries` until the
                # while loop should break when not enough samples have been collected
                # to make n-step possible.
                if sa_episode_ts - actual_n_step < 0:
                    continue
                else:
                    n_steps[B] = actual_n_step
                # Get the multi-agent episode.
                ma_episode = self.episodes[ma_episode_idx]
                # Retrieve the single-agent episode for filtering.
                sa_episode = ma_episode.agent_episodes[agent_id]
                # Ensure that each row contains a tuple of the form:
                #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
                # TODO (simon): Implement version for sequence sampling when using RNNs.
                sa_eps_observation = sa_episode.get_observations(
                    slice(sa_episode_ts - actual_n_step, sa_episode_ts + 1)
                )
                # Note, the reward that is collected by transitioning from `o_t` to
                # `o_(t+1)` is stored in the next transition in `SingleAgentEpisode`.
                sa_eps_rewards = sa_episode.get_rewards(
                    slice(sa_episode_ts - actual_n_step, sa_episode_ts)
                )
                observations[B] = sa_eps_observation[0]
                next_observations[B] = sa_eps_observation[-1]
                # Note, this will be the reward after executing action
                # `a_(episode_ts-n_step+1)`. For `n_step>1` this will be the sum of
                # all rewards that were collected over the last n steps.
                rewards[B] = scipy.signal.lfilter(
                    [1], [1, -gamma], sa_eps_rewards[::-1], axis=0
                )[-1]
                # Note, `SingleAgentEpisode` stores the action that followed
                # `o_t` with `o_(t+1)`, therefore, we need the next one.
                # TODO (simon): This gets the wrong action as long as the getters are
                # not fixed.
                actions[B] = sa_episode.get_actions(sa_episode_ts - actual_n_step)
                if include_infos:
                    # If infos are included we include the ones from the last timestep
                    # as usually the info contains additional values about the last
                    # state.
                    infos[B] = sa_episode.get_infos(sa_episode_ts)
                if include_extra_model_outputs:
                    # If `extra_model_outputs` are included we include the ones from the
                    # first timestep as usually the `extra_model_outputs` contain
                    # additional values from the forward pass that produced the action
                    # at the first timestep.
                    # Note, we extract them into single row dictionaries similar to the
                    # infos, in a connector we can then extract these into single batch
                    # rows.
                    extra_model_outputs[B] = {
                        k: sa_episode.get_extra_model_outputs(
                            k, sa_episode_ts - actual_n_step
                        )
                        for k in sa_episode.extra_model_outputs.keys()
                    }
                # If the sampled time step is the episode's last time step check, if
                # the episode is terminated or truncated.
                if sa_episode_ts == sa_episode.t:
                    is_terminated[B] = sa_episode.is_terminated
                    is_truncated[B] = sa_episode.is_truncated

                # Increase counter.
                B += 1

            # Increase the per module timesteps counter.
            self.sampled_timesteps_per_module[module_id] += batch_size_B
            ret[module_id] = {
                # Note, observation and action spaces could be complex. `batch`
                # takes care of these.
                Columns.OBS: batch(observations),
                Columns.ACTIONS: batch(actions),
                Columns.REWARDS: np.array(rewards),
                Columns.NEXT_OBS: batch(next_observations),
                Columns.TERMINATEDS: np.array(is_terminated),
                Columns.TRUNCATEDS: np.array(is_truncated),
                "weights": np.array(weights),
                "n_steps": np.array(n_steps),
            }
            # Include infos if necessary.
            if include_infos:
                ret[module_id].update(
                    {
                        Columns.INFOS: infos,
                    }
                )
            # Include extra model outputs, if necessary.
            if include_extra_model_outputs:
                ret[module_id].update(
                    # These could be complex, too.
                    batch(extra_model_outputs)
                )
        # Increase the counter for environment timesteps.
        self.sampled_timesteps += batch_size_B
        # Return multi-agent dictionary.
        return ret

    def _sample_synchonized(
        self,
        batch_size_B: Optional[int],
        batch_length_T: Optional[int],
        n_step: Optional[Union[int, Tuple]],
        gamma: float,
        include_infos: bool,
        include_extra_model_outputs: bool,
        modules_to_sample: Optional[List[ModuleID]],
    ) -> SampleBatchType:
        """Samples a batch of synchronized multi-agent transitions."""
        # Sample the n-step if necessary.
        if isinstance(n_step, tuple):
            # Use random n-step sampling.
            random_n_step = True
        else:
            actual_n_step = n_step or 1
            random_n_step = False

        # Containers for the sampled data.
        observations: Dict[ModuleID, List[ObsType]] = defaultdict(list)
        next_observations: Dict[ModuleID, List[ObsType]] = defaultdict(list)
        actions: Dict[ModuleID, List[ActType]] = defaultdict(list)
        rewards: Dict[ModuleID, List[float]] = defaultdict(list)
        is_terminated: Dict[ModuleID, List[bool]] = defaultdict(list)
        is_truncated: Dict[ModuleID, List[bool]] = defaultdict(list)
        weights: Dict[ModuleID, List[float]] = defaultdict(list)
        n_steps: Dict[ModuleID, List[int]] = defaultdict(list)
        # If `info` should be included, construct also a container for them.
        if include_infos:
            infos: Dict[ModuleID, List[Dict[str, Any]]] = defaultdict(list)
        # If `extra_model_outputs` should be included, construct a container for them.
        if include_extra_model_outputs:
            extra_model_outputs: Dict[ModuleID, List[Dict[str, Any]]] = defaultdict(
                list
            )

        B = 0
        while B < batch_size_B:
            index_tuple = self._indices[self.rng.integers(len(self._indices))]

            # This will be an env timestep (not agent timestep)
            ma_episode_idx, ma_episode_ts = (
                index_tuple[0] - self._num_episodes_evicted,
                index_tuple[1],
            )
            # If we use random n-step sampling, draw the n-step for this item.
            if random_n_step:
                actual_n_step = int(self.rng.integers(n_step[0], n_step[1]))
            # If we are at the end of an episode, continue.
            # Note, priority sampling got us `o_(t+n)` and we need for the loss
            # calculation in addition `o_t`.
            # TODO (simon): Maybe introduce a variable `num_retries` until the
            # while loop should break when not enough samples have been collected
            # to make n-step possible.
            if ma_episode_ts - actual_n_step < 0:
                continue

            # Retrieve the multi-agent episode.
            ma_episode = self.episodes[ma_episode_idx]

            # Ensure that each row contains a tuple of the form:
            #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
            # TODO (simon): Implement version for sequence sampling when using RNNs.
            eps_observation = ma_episode.get_observations(
                slice(ma_episode_ts - actual_n_step, ma_episode_ts + 1),
                return_list=True,
            )
            # Note, `MultiAgentEpisode` stores the action that followed
            # `o_t` with `o_(t+1)`, therefore, we need the next one.
            # TODO (simon): This gets the wrong action as long as the getters are not
            # fixed.
            eps_actions = ma_episode.get_actions(ma_episode_ts - actual_n_step)
            # Make sure that at least a single agent should have full transition.
            # TODO (simon): Filter for the `modules_to_sample`.
            agents_to_sample = self._agents_with_full_transitions(
                eps_observation,
                eps_actions,
            )
            # If not, we resample.
            if not agents_to_sample:
                continue
            # TODO (simon, sven): Do we need to include the common agent rewards?
            # Note, the reward that is collected by transitioning from `o_t` to
            # `o_(t+1)` is stored in the next transition in `MultiAgentEpisode`.
            eps_rewards = ma_episode.get_rewards(
                slice(ma_episode_ts - actual_n_step, ma_episode_ts),
                return_list=True,
            )
            # TODO (simon, sven): Do we need to include the common infos? And are
            # there common extra model outputs?
            if include_infos:
                # If infos are included we include the ones from the last timestep
                # as usually the info contains additional values about the last state.
                eps_infos = ma_episode.get_infos(ma_episode_ts)
            if include_extra_model_outputs:
                # If `extra_model_outputs` are included we include the ones from the
                # first timestep as usually the `extra_model_outputs` contain additional
                # values from the forward pass that produced the action at the first
                # timestep.
                # Note, we extract them into single row dictionaries similar to the
                # infos, in a connector we can then extract these into single batch
                # rows.
                eps_extra_model_outputs = {
                    k: ma_episode.get_extra_model_outputs(
                        k, ma_episode_ts - actual_n_step
                    )
                    for k in ma_episode.extra_model_outputs.keys()
                }
            # If the sampled time step is the episode's last time step check, if
            # the episode is terminated or truncated.
            episode_terminated = False
            episode_truncated = False
            if ma_episode_ts == ma_episode.env_t:
                episode_terminated = ma_episode.is_terminated
                episode_truncated = ma_episode.is_truncated
            # TODO (simon): Filter for the `modules_to_sample`.
            # TODO (sven, simon): We could here also sample for all agents in the
            # `modules_to_sample` and then adapt the `n_step` for agents that
            # have not a full transition.
            for agent_id in agents_to_sample:
                # Map our agent to the corresponding module we want to
                # train.
                module_id = ma_episode._agent_to_module_mapping[agent_id]
                # Sample only for the modules in `modules_to_sample`.
                if module_id not in (
                    modules_to_sample or self._module_to_indices.keys()
                ):
                    continue
                # TODO (simon, sven): Here we could skip for modules not
                # to be sampled in `modules_to_sample`.
                observations[module_id].append(eps_observation[0][agent_id])
                next_observations[module_id].append(eps_observation[-1][agent_id])
                # Fill missing rewards with zeros.
                agent_rewards = [r[agent_id] or 0.0 for r in eps_rewards]
                rewards[module_id].append(
                    scipy.signal.lfilter([1], [1, -gamma], agent_rewards[::-1], axis=0)[
                        -1
                    ]
                )
                # Note, this should exist, as we filtered for agents with full
                # transitions.
                actions[module_id].append(eps_actions[agent_id])
                if include_infos:
                    infos[module_id].append(eps_infos[agent_id])
                if include_extra_model_outputs:
                    extra_model_outputs[module_id].append(
                        {
                            k: eps_extra_model_outputs[agent_id][k]
                            for k in eps_extra_model_outputs[agent_id].keys()
                        }
                    )
                # If sampled observation is terminal for the agent. Either MAE
                # episode is truncated/terminated or SAE episode is truncated/
                # terminated at this ts.
                # TODO (simon, sven): Add method agent_alive(ts) to MAE.
                # or add slicing to get_terminateds().
                agent_ts = ma_episode.env_t_to_agent_t[agent_id][ma_episode_ts]
                agent_eps = ma_episode.agent_episodes[agent_id]
                agent_terminated = agent_ts == agent_eps.t and agent_eps.is_terminated
                agent_truncated = (
                    agent_ts == agent_eps.t
                    and agent_eps.is_truncated
                    and not agent_eps.is_terminated
                )
                if episode_terminated or agent_terminated:
                    is_terminated[module_id].append(True)
                    is_truncated[module_id].append(False)
                elif episode_truncated or agent_truncated:
                    is_truncated[module_id].append(True)
                    is_terminated[module_id].append(False)
                else:
                    is_terminated[module_id].append(False)
                    is_truncated[module_id].append(False)
                # Increase the per module counter.
                self.sampled_timesteps_per_module[module_id] += 1

            # Increase counter.
            B += 1
        # Increase the counter for environment timesteps.
        self.sampled_timesteps += batch_size_B

        # Should be convertible to MultiAgentBatch.
        ret = {
            **{
                module_id: {
                    Columns.OBS: batch(observations[module_id]),
                    Columns.ACTIONS: batch(actions[module_id]),
                    Columns.REWARDS: np.array(rewards[module_id]),
                    Columns.NEXT_OBS: batch(next_observations[module_id]),
                    Columns.TERMINATEDS: np.array(is_terminated[module_id]),
                    Columns.TRUNCATEDS: np.array(is_truncated[module_id]),
                    "weights": np.array(weights[module_id]),
                    "n_steps": np.array(n_steps[module_id]),
                }
                for module_id in observations.keys()
            }
        }

        # Return multi-agent dictionary.
        return ret

    def _num_remaining_episodes(self, new_eps, evicted_eps):
        """Calculates the number of remaining episodes.

        When adding episodes and evicting them in the `add()` method
        this function calculates iteratively the number of remaining
        episodes.

        Args:
            new_eps: List of new episode IDs.
            evicted_eps: List of evicted episode IDs.

        Returns:
            Number of episodes remaining after evicting the episodes in
            `evicted_eps` and adding the episode in `new_eps`.
        """
        return len(
            set(self.episode_id_to_index.keys()).union(set(new_eps)) - set(evicted_eps)
        )

    def _evict_module_episodes(self, ma_episode: MultiAgentEpisode) -> None:
        """Evicts the module episodes from the buffer adn updates all counters.

        Args:
            multi_agent_eps: The multi-agent episode to evict from the buffer.
        """

        # Note we need to take the agent ids from the evicted episode because
        # different episodes can have different agents and module mappings.
        for agent_id in ma_episode.agent_ids:
            # Retrieve the corresponding module ID and module episode.
            module_id = ma_episode._agent_to_module_mapping[agent_id]
            module_eps = ma_episode.agent_episodes[agent_id]
            # Update all counters.
            self._num_module_timesteps[module_id] -= module_eps.env_steps()
            self._num_module_episodes[module_id] -= 1
            self._num_module_episodes_evicted[module_id] += 1

    def _update_module_counters(self, ma_episode: MultiAgentEpisode) -> None:
        """Updates the module counters after adding an episode.

        Args:
            multi_agent_episode: The multi-agent episode to update the module counters
                for.
        """
        for agent_id in ma_episode.agent_ids:
            agent_steps = ma_episode.agent_episodes[agent_id].env_steps()
            # Only add if the agent has stepped in the episode (chunk).
            if agent_steps > 0:
                # Receive the corresponding module ID.
                # TODO (sven, simon): Is there always a mapping? What if not?
                # Is then module_id == agent_id?
                module_id = ma_episode._agent_to_module_mapping[agent_id]
                self._num_module_timesteps[module_id] += agent_steps
                self._num_module_timesteps_added[module_id] += agent_steps
                # Also add to the module episode counter.
                self._num_module_episodes[module_id] += 1

    def _add_new_module_indices(
        self, ma_episode: MultiAgentEpisode, episode_idx: int, exists: bool = True
    ) -> None:
        """Adds the module indices for new episode chunks.

        Args:
            multi_agent_episode: The multi-agent episode to add the module indices for.
            episode_idx: The index of the episode in the `self.episodes`.
        """

        for agent_id in ma_episode.agent_ids:
            # Get the corresponding module id.
            module_id = ma_episode._agent_to_module_mapping[agent_id]
            # Get the module episode.
            module_eps = ma_episode.agent_episodes[agent_id]
            # Check if the module episode is already in the buffer.
            if exists:
                old_ma_episode = self.episodes[
                    self.episode_id_to_index[ma_episode.id_]
                    - self._num_episodes_evicted
                ]
                # Is the agent episode already in the buffer?
                sa_episode_in_buffer = agent_id in old_ma_episode.agent_episodes
            else:
                # This agent episode is new. The agent might have just entered
                # the environment.
                sa_episode_in_buffer = False
            if sa_episode_in_buffer:
                existing_eps_len = len(
                    self.episodes[episode_idx].agent_episodes[agent_id]
                )
            else:
                existing_eps_len = 0
            # Add new module indices.
            self._module_to_indices[module_id].extend(
                [
                    (
                        # Keep the MAE index for sampling
                        episode_idx,
                        agent_id,
                        existing_eps_len + i + 1,
                    )
                    for i in range(len(module_eps))
                ]
            )

    def _agents_with_full_transitions(
        self, observations: Dict[AgentID, ObsType], actions: Dict[AgentID, ActType]
    ):
        """Filters for agents that have full transitions.

        Args:
            observations: The observations of the episode.
            actions: The actions of the episode.

        Returns:
            List of agent IDs that have full transitions.
        """
        agents_to_sample = []
        for agent_id in observations[0].keys():
            # Only if the agent has an action at the first and an observation
            # at the first and last timestep of the n-step transition, we can sample it.
            if agent_id in actions and agent_id in observations[-1]:
                agents_to_sample.append(agent_id)
        return agents_to_sample
