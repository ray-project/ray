import copy
import hashlib
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np
import scipy
from gymnasium.core import ActType, ObsType

from ray.rllib.core.columns import Columns
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils import force_list
from ray.rllib.utils.annotations import (
    DeveloperAPI,
    override,
)
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import AgentID, ModuleID, SampleBatchType


@DeveloperAPI
class MultiAgentEpisodeReplayBuffer(EpisodeReplayBuffer):
    """Multi-agent episode replay buffer that stores episodes by their IDs.

    This class implements a replay buffer as used in "playing Atari with Deep
    Reinforcement Learning" (Mnih et al., 2013) for multi-agent reinforcement
    learning,

    Each "row" (a slot in a deque) in the buffer is occupied by one episode. If an
    incomplete episode is added to the buffer and then another chunk of that episode is
    added at a later time, the buffer will automatically concatenate the new fragment to
    the original episode. This way, episodes can be completed via subsequent `add`
    calls.

    Sampling returns a size `B` episode list (number of 'rows'), where each episode
    holds a tuple tuple of the form

    `(o_t, a_t, sum(r_t+1:t+n), o_t+n)`

    where `o_t` is the observation in `t`, `a_t` the action chosen at observation `o_t`,
    `o_t+n` is the observation `n` timesteps later and `sum(r_t+1:t+n)` is the sum of
    all rewards collected over the time steps between `t+1` and `t+n`. The `n`-step can
    be chosen freely when sampling and defaults to `1`. If `n_step` is a tuple it is
    sampled uniformly across the interval defined by the tuple (for each row in the
    batch).

    Each episode contains - in addition to the data tuples presented above - two further
    elements in its `extra_model_outputs`, namely `n_steps` and `weights`. The former
    holds the `n_step` used for the sampled timesteps in the episode and the latter the
    corresponding (importance sampling) weight for the transition.

    .. testcode::

        import gymnasium as gym

        from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
        from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
        from ray.rllib.utils.replay_buffers import MultiAgentEpisodeReplayBuffer


        # Create the environment.
        env = MultiAgentCartPole({"num_agents": 2})

        # Set up the loop variables
        agent_ids = env.agents
        agent_ids.append("__all__")
        terminateds = {aid: False for aid in agent_ids}
        truncateds = {aid: False for aid in agent_ids}
        num_timesteps = 10000
        episodes = []

        # Initialize the first episode entries.
        eps = MultiAgentEpisode()
        obs, infos = env.reset()
        eps.add_env_reset(observations=obs, infos=infos)

        # Sample 10,000 env timesteps.
        for i in range(num_timesteps):
            # If terminated we create a new episode.
            if eps.is_done:
                episodes.append(eps.to_numpy())
                eps = MultiAgentEpisode()
                terminateds = {aid: False for aid in agent_ids}
                truncateds = {aid: False for aid in agent_ids}
                obs, infos = env.reset()
                eps.add_env_reset(observations=obs, infos=infos)

            # Sample a random action for all agents that should step in the episode
            # next.
            actions = {
                aid: env.get_action_space(aid).sample()
                for aid in eps.get_agents_to_act()
            }
            obs, rewards, terminateds, truncateds, infos = env.step(actions)
            eps.add_env_step(
                obs,
                actions,
                rewards,
                infos,
                terminateds=terminateds,
                truncateds=truncateds
            )

        # Add the last (truncated) episode to the list of episodes.
        if not eps.is_done:
            episodes.append(eps)

        # Create the buffer.
        buffer = MultiAgentEpisodeReplayBuffer()
        # Add the list of episodes sampled.
        buffer.add(episodes)

        # Pull a sample from the buffer using an `n-step` of 3.
        sample = buffer.sample(num_items=256, gamma=0.95, n_step=3)
    """

    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 1,
        metrics_num_episodes_for_smoothing: int = 100,
        **kwargs,
    ):
        """Initializes a multi-agent episode replay buffer.

        Args:
            capacity: The total number of timesteps to be storable in this buffer.
                Will start ejecting old episodes once this limit is reached.
            batch_size_B: The number of episodes returned from `sample()`.
            batch_length_T: The length of each episode in the episode list returned from
                `sample()`.
        """
        # Initialize the base episode replay buffer.
        super().__init__(
            capacity=capacity,
            batch_size_B=batch_size_B,
            batch_length_T=batch_length_T,
            metrics_num_episodes_for_smoothing=metrics_num_episodes_for_smoothing,
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
        To defend against such case, the complete episode is evicted, including
        the new chunk, unless the episode is the only one in the buffer. In the
        latter case the buffer will be allowed to overflow in a temporary fashion,
        i.e. during the next addition of samples to the buffer an attempt is made
        to fall below capacity again.

        The user is advised to select a large enough buffer with regard to the maximum
        expected episode length.

        Args:
            episodes: The multi-agent episodes to add to the replay buffer. Can be a
                single episode or a list of episodes.
        """
        episodes: List["MultiAgentEpisode"] = force_list(episodes)

        new_episode_ids: Set[str] = {eps.id_ for eps in episodes}
        total_env_timesteps = sum([eps.env_steps() for eps in episodes])
        self._num_timesteps += total_env_timesteps
        self._num_timesteps_added += total_env_timesteps

        # Set up some counters for metrics.
        num_env_steps_added = 0
        agent_to_num_steps_added = defaultdict(int)
        module_to_num_steps_added = defaultdict(int)
        num_episodes_added = 0
        agent_to_num_episodes_added = defaultdict(int)
        module_to_num_episodes_added = defaultdict(int)
        num_episodes_evicted = 0
        agent_to_num_episodes_evicted = defaultdict(int)
        module_to_num_episodes_evicted = defaultdict(int)
        num_env_steps_evicted = 0
        agent_to_num_steps_evicted = defaultdict(int)
        module_to_num_steps_evicted = defaultdict(int)

        # Evict old episodes.
        eps_evicted_ids: Set[Union[str, int]] = set()
        eps_evicted_idxs: Set[int] = set()
        while (
            self._num_timesteps > self.capacity
            and self._num_remaining_episodes(new_episode_ids, eps_evicted_ids) != 1
        ):
            # Evict episode.
            evicted_episode = self.episodes.popleft()
            eps_evicted_ids.add(evicted_episode.id_)
            eps_evicted_idxs.add(self.episode_id_to_index.pop(evicted_episode.id_))
            # If this episode has a new chunk in the new episodes added,
            # we subtract it again.
            # TODO (sven, simon): Should we just treat such an episode chunk
            # as a new episode?
            if evicted_episode.id_ in new_episode_ids:
                idx = next(
                    i
                    for i, eps in enumerate(episodes)
                    if eps.id_ == evicted_episode.id_
                )
                new_eps_to_evict = episodes.pop(idx)
                self._num_timesteps -= new_eps_to_evict.env_steps()
                self._num_timesteps_added -= new_eps_to_evict.env_steps()
            # Remove the timesteps of the evicted episode from the counter.
            self._num_timesteps -= evicted_episode.env_steps()
            self._num_agent_timesteps -= evicted_episode.agent_steps()
            self._num_episodes_evicted += 1
            # Increase the counters.
            num_episodes_evicted += 1
            num_env_steps_evicted += evicted_episode.env_steps()
            for aid, a_eps in evicted_episode.agent_episodes.items():
                mid = evicted_episode._agent_to_module_mapping[aid]
                agent_to_num_episodes_evicted[aid] += 1
                module_to_num_episodes_evicted[mid] += 1
                agent_to_num_steps_evicted[aid] += a_eps.agent_steps()
                module_to_num_steps_evicted[mid] += a_eps.agent_steps()
            # Remove the module timesteps of the evicted episode from the counters.
            self._evict_module_episodes(evicted_episode)
            del evicted_episode

        # Add agent and module steps.
        for eps in episodes:
            self._num_agent_timesteps += eps.agent_steps()
            self._num_agent_timesteps_added += eps.agent_steps()
            # Update the module counters by the module timesteps.
            self._update_module_counters(eps)

        # Remove corresponding indices, if episodes were evicted.
        if eps_evicted_idxs:
            # If the episode is not evicted, we keep the index.
            # Note, each index 2-tuple is of the form (ma_episode_idx, timestep)
            # and refers to a certain environment timestep in a certain
            # multi-agent episode.
            self._indices = [
                idx_tuple
                for idx_tuple in self._indices
                if idx_tuple[0] not in eps_evicted_idxs
            ]
            # Also remove corresponding module indices.
            for module_id, module_indices in self._module_to_indices.items():
                # Each index 3-tuple is of the form
                # (ma_episode_idx, agent_id, timestep) and refers to a certain
                # agent timestep in a certain multi-agent episode.
                self._module_to_indices[module_id] = [
                    idx_triplet
                    for idx_triplet in module_indices
                    if idx_triplet[0] not in eps_evicted_idxs
                ]

        for eps in episodes:
            eps = copy.deepcopy(eps)
            # If the episode is part of an already existing episode, concatenate.
            if eps.id_ in self.episode_id_to_index:
                eps_idx = self.episode_id_to_index[eps.id_]
                existing_eps = self.episodes[eps_idx - self._num_episodes_evicted]
                existing_len = len(existing_eps)
                self._indices.extend(
                    [
                        (
                            eps_idx,
                            existing_len + i,
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
                # Update the counters
                num_episodes_added += 1
                for aid, a_eps in eps.agent_episodes.items():
                    mid = eps._agent_to_module_mapping[aid]
                    agent_to_num_episodes_added[aid] += 1
                    module_to_num_episodes_added[mid] += 1
                eps_idx = len(self.episodes) - 1 + self._num_episodes_evicted
                self.episode_id_to_index[eps.id_] = eps_idx
                self._indices.extend([(eps_idx, i) for i in range(len(eps))])
                # Add new module indices.
                self._add_new_module_indices(eps, eps_idx, False)
            # Update the step counters.
            num_env_steps_added += eps.env_steps()
            for aid, e_eps in eps.agent_episodes.items():
                mid = eps._agent_to_module_mapping[aid]
                agent_to_num_steps_added[aid] += e_eps.agent_steps()
                module_to_num_steps_added[mid] += e_eps.agent_steps()

        # Update the adding metrics.
        self._update_add_metrics(
            num_episodes_added=num_episodes_added,
            num_env_steps_added=num_env_steps_added,
            num_episodes_evicted=num_episodes_evicted,
            num_env_steps_evicted=num_env_steps_evicted,
            agent_to_num_episodes_added=agent_to_num_episodes_added,
            agent_to_num_steps_added=agent_to_num_steps_added,
            agent_to_num_episodes_evicted=agent_to_num_episodes_evicted,
            agent_to_num_steps_evicted=agent_to_num_steps_evicted,
            module_to_num_episodes_added=module_to_num_steps_added,
            module_to_num_steps_added=module_to_num_episodes_added,
            module_to_num_episodes_evicted=module_to_num_episodes_evicted,
            module_to_num_steps_evicted=module_to_num_steps_evicted,
        )

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
        **kwargs,
    ) -> Union[List["MultiAgentEpisode"], List["SingleAgentEpisode"]]:
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
    def get_num_episodes(self, module_id: Optional[ModuleID] = None) -> int:
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

    @override(EpisodeReplayBuffer)
    def get_num_episodes_evicted(self, module_id: Optional[ModuleID] = None) -> int:
        """Returns number of episodes evicted for a module in the buffer."""
        return (
            self._num_module_episodes_evicted[module_id]
            if module_id
            else super().get_num_episodes_evicted()
        )

    @override(EpisodeReplayBuffer)
    def get_num_timesteps(self, module_id: Optional[ModuleID] = None) -> int:
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

    @override(EpisodeReplayBuffer)
    def get_sampled_timesteps(self, module_id: Optional[ModuleID] = None) -> int:
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

    @override(EpisodeReplayBuffer)
    def get_added_timesteps(self, module_id: Optional[ModuleID] = None) -> int:
        """Returns the number of timesteps added in buffer's lifetime for given module.

        Args:
            module_id: The ID of the module to query. If not provided, the total number
                of timesteps ever added.

        Returns:
            The number of timesteps added for `module_id` (or all modules if `module_id`
            is None).
        """
        return (
            self._num_module_timesteps_added[module_id]
            if module_id
            else super().get_added_timesteps()
        )

    @override(EpisodeReplayBuffer)
    def get_state(self) -> Dict[str, Any]:
        """Gets a pickable state of the buffer.

        This is used for checkpointing the buffer's state. It is specifically helpful,
        for example, when a trial is paused and resumed later on. The buffer's state
        can be saved to disk and reloaded when the trial is resumed.

        Returns:
            A dict containing all necessary information to restore the buffer's state.
        """
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
        """Sets the state of a buffer from a previously stored state.

        See `get_state()` for more information on what is stored in the state. This
        method is used to restore the buffer's state from a previously stored state.
        It is specifically helpful, for example, when a trial is paused and resumed
        later on. The buffer's state can be saved to disk and reloaded when the trial
        is resumed.

        Args:
            state: The state to restore the buffer from.
        """
        # Set the episodes.
        self._set_episodes(state)
        # Set the super's state.
        super().set_state(state)
        # Now set the remaining attributes.
        self._module_to_indices = defaultdict(list, dict(state["_module_to_indices"]))
        self._num_agent_timesteps = state["_num_agent_timesteps"]
        self._num_agent_timesteps_added = state["_num_agent_timesteps_added"]
        self._num_module_timesteps = defaultdict(
            int, dict(state["_num_module_timesteps"])
        )
        self._num_module_timesteps_added = defaultdict(
            int, dict(state["_num_module_timesteps_added"])
        )
        self._num_module_episodes = defaultdict(
            int, dict(state["_num_module_episodes"])
        )
        self._num_module_episodes_evicted = defaultdict(
            int, dict(state["_num_module_episodes_evicted"])
        )
        self.sampled_timesteps_per_module = defaultdict(
            list, dict(state["sampled_timesteps_per_module"])
        )

    def _set_episodes(self, state: Dict[str, Any]) -> None:
        """Sets the episodes from the state."""
        if not self.episodes:
            self.episodes = deque(
                [
                    MultiAgentEpisode.from_state(eps_data)
                    for eps_data in state["episodes"]
                ]
            )

    def _sample_independent(
        self,
        batch_size_B: Optional[int],
        batch_length_T: Optional[int],
        n_step: Optional[Union[int, Tuple[int, int]]],
        gamma: float,
        include_infos: bool,
        include_extra_model_outputs: bool,
        modules_to_sample: Optional[Set[ModuleID]],
    ) -> List["SingleAgentEpisode"]:
        """Samples a batch of independent multi-agent transitions."""

        actual_n_step = n_step or 1
        # Sample the n-step if necessary.
        random_n_step = isinstance(n_step, (tuple, list))

        sampled_episodes = []
        # Record the number of samples per module/agent/total.
        num_env_steps_sampled = 0
        agent_to_num_steps_sampled = defaultdict(int)
        module_to_num_steps_sampled = defaultdict(int)
        # Record all the env step buffer indices that are contained in the sample.
        sampled_env_step_idxs = set()
        agent_to_sampled_env_step_idxs = defaultdict(set)
        module_to_sampled_env_step_idxs = defaultdict(set)
        # Record all the episode buffer indices that are contained in the sample.
        sampled_episode_idxs = set()
        agent_to_sampled_episode_idxs = defaultdict(set)
        module_to_sampled_episode_idxs = defaultdict(set)
        # Record all n-steps that have been used.
        sampled_n_steps = []
        agent_to_sampled_n_steps = defaultdict(list)
        module_to_sampled_n_steps = defaultdict(list)
        # Record the number of times a sample needs to be resampled.
        num_resamples = 0
        agent_to_num_resamples = defaultdict(int)
        module_to_num_resamples = defaultdict(int)

        # TODO (simon): Ensure that the module has data and if not, skip it.
        #  TODO (sven): Should we then error out or skip? I think the Learner
        #  should handle this case when a module has no train data.
        modules_to_sample = modules_to_sample or set(self._module_to_indices.keys())
        for module_id in modules_to_sample:
            module_indices = self._module_to_indices[module_id]
            B = 0
            while B < batch_size_B:
                # Now sample from the single-agent timesteps.
                index_tuple = module_indices[self.rng.integers(len(module_indices))]

                # This will be an agent timestep (not env timestep).
                # TODO (simon, sven): Maybe deprecate sa_episode_idx (_) in the index
                #   quads. Is there any need for it?
                ma_episode_idx, agent_id, sa_episode_ts = (
                    index_tuple[0] - self._num_episodes_evicted,
                    index_tuple[1],
                    index_tuple[2],
                )

                # Get the multi-agent episode.
                ma_episode = self.episodes[ma_episode_idx]
                # Retrieve the single-agent episode for filtering.
                sa_episode = ma_episode.agent_episodes[agent_id]

                # If we use random n-step sampling, draw the n-step for this item.
                if random_n_step:
                    actual_n_step = int(self.rng.integers(n_step[0], n_step[1]))
                # If we cannnot make the n-step, we resample.
                if sa_episode_ts + actual_n_step > len(sa_episode):
                    num_resamples += 1
                    agent_to_num_resamples[agent_id] += 1
                    module_to_num_resamples[module_id] += 1
                    continue
                # Note, this will be the reward after executing action
                # `a_(episode_ts)`. For `n_step>1` this will be the discounted sum
                # of all rewards that were collected over the last n steps.
                sa_raw_rewards = sa_episode.get_rewards(
                    slice(sa_episode_ts, sa_episode_ts + actual_n_step)
                )
                sa_rewards = scipy.signal.lfilter(
                    [1], [1, -gamma], sa_raw_rewards[::-1], axis=0
                )[-1]

                sampled_sa_episode = SingleAgentEpisode(
                    id_=sa_episode.id_,
                    # Provide the IDs for the learner connector.
                    agent_id=sa_episode.agent_id,
                    module_id=sa_episode.module_id,
                    multi_agent_episode_id=ma_episode.id_,
                    # Ensure that each episode contains a tuple of the form:
                    #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
                    # Two observations (t and t+n).
                    observations=[
                        sa_episode.get_observations(sa_episode_ts),
                        sa_episode.get_observations(sa_episode_ts + actual_n_step),
                    ],
                    observation_space=sa_episode.observation_space,
                    infos=(
                        [
                            sa_episode.get_infos(sa_episode_ts),
                            sa_episode.get_infos(sa_episode_ts + actual_n_step),
                        ]
                        if include_infos
                        else None
                    ),
                    actions=[sa_episode.get_actions(sa_episode_ts)],
                    action_space=sa_episode.action_space,
                    rewards=[sa_rewards],
                    # If the sampled single-agent episode is the single-agent episode's
                    # last time step, check, if the single-agent episode is terminated
                    # or truncated.
                    terminated=(
                        sa_episode_ts + actual_n_step >= len(sa_episode)
                        and sa_episode.is_terminated
                    ),
                    truncated=(
                        sa_episode_ts + actual_n_step >= len(sa_episode)
                        and sa_episode.is_truncated
                    ),
                    extra_model_outputs={
                        "weights": [1.0],
                        "n_step": [actual_n_step],
                        **(
                            {
                                k: [
                                    sa_episode.get_extra_model_outputs(k, sa_episode_ts)
                                ]
                                for k in sa_episode.extra_model_outputs.keys()
                            }
                            if include_extra_model_outputs
                            else {}
                        ),
                    },
                    # TODO (sven): Support lookback buffers.
                    len_lookback_buffer=0,
                    t_started=sa_episode_ts,
                )
                # Append single-agent episode to the list of sampled episodes.
                sampled_episodes.append(sampled_sa_episode)
                # Add the episode indices.
                sampled_episode_idxs.add(ma_episode_idx)
                agent_to_sampled_episode_idxs[sa_episode.agent_id].add(sa_episode.id_)
                module_to_sampled_episode_idxs[module_id].add(sa_episode.id_)
                # Add the unique step hashes.
                # Get the corresponding index in the `env_to_agent_t` mapping.
                # TODO (simon, sven): This has complexity O(n) and could become
                # expensive when the episode is large. Note, however, that conversion
                # from list to `numpy.ndarray` is also complexity O(n) and we do this
                # at many places - also in the `MultiAgentEpisode`s.
                ma_episode_ts = ma_episode.env_t_to_agent_t[agent_id].data.index(
                    sa_episode_ts
                )
                sampled_env_step_idxs.add(
                    hashlib.sha256(
                        f"{ma_episode.id_}-{ma_episode_ts}".encode()
                    ).hexdigest()
                )
                hashed_agent_step = hashlib.sha256(
                    f"{sa_episode.id_}-{sa_episode_ts}".encode()
                ).hexdigest()
                agent_to_sampled_env_step_idxs[agent_id].add(hashed_agent_step)
                module_to_sampled_env_step_idxs[module_id].add(hashed_agent_step)
                # Add the actual n-step used in generating this sample.
                sampled_n_steps.append(actual_n_step)
                agent_to_sampled_n_steps[agent_id].append(actual_n_step)
                module_to_sampled_n_steps[module_id].append(actual_n_step)

                # Increase counter.
                B += 1

            # Increase the per module timesteps counter.
            self.sampled_timesteps_per_module[module_id] += B
            # Increase the counter metrics.
            num_env_steps_sampled += B
            agent_to_num_steps_sampled[agent_id] += B
            module_to_num_steps_sampled[module_id] += B

        # Increase the counter for environment timesteps.
        self.sampled_timesteps += batch_size_B

        # Update the sample metrics.
        num_episodes_per_sample = len(sampled_episode_idxs)
        num_env_steps_per_sample = len(sampled_env_step_idxs)
        sampled_n_step = sum(sampled_n_steps) / batch_size_B
        agent_to_num_episodes_per_sample = {
            aid: len(l) for aid, l in agent_to_sampled_episode_idxs.items()
        }
        module_to_num_episodes_per_sample = {
            mid: len(l) for mid, l in module_to_sampled_episode_idxs.items()
        }
        agent_to_num_steps_per_sample = {
            aid: len(l) for aid, l in agent_to_sampled_env_step_idxs.items()
        }
        module_to_num_steps_per_sample = {
            mid: len(l) for mid, l in module_to_sampled_env_step_idxs.items()
        }
        agent_to_sampled_n_step = {
            aid: sum(l) / len(l) for aid, l in agent_to_sampled_n_steps.items()
        }
        module_to_sampled_n_step = {
            mid: sum(l) / len(l) for mid, l in module_to_sampled_n_steps.items()
        }
        self._update_sample_metrics(
            num_env_steps_sampled=num_env_steps_sampled,
            num_episodes_per_sample=num_episodes_per_sample,
            num_env_steps_per_sample=num_env_steps_per_sample,
            sampled_n_step=sampled_n_step,
            num_resamples=num_resamples,
            agent_to_num_steps_sampled=agent_to_num_steps_sampled,
            agent_to_num_episodes_per_sample=agent_to_num_episodes_per_sample,
            agent_to_num_steps_per_sample=agent_to_num_steps_per_sample,
            agent_to_sampled_n_step=agent_to_sampled_n_step,
            agent_to_num_resamples=agent_to_num_resamples,
            module_to_num_steps_sampled=module_to_num_steps_sampled,
            module_to_num_episodes_per_sample=module_to_num_episodes_per_sample,
            module_to_num_steps_per_sample=module_to_num_steps_per_sample,
            module_to_sampled_n_step=module_to_sampled_n_step,
            module_to_num_resamples=module_to_num_resamples,
        )

        # Return multi-agent dictionary.
        return sampled_episodes

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
                    "n_step": np.array(n_steps[module_id]),
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
        for agent_id in ma_episode.agent_episodes:
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
                module_id = ma_episode.module_for(agent_id)
                self._num_module_timesteps[module_id] += agent_steps
                self._num_module_timesteps_added[module_id] += agent_steps
                # if ma_episode.agent_episodes[agent_id].is_done:
                #     # TODO (simon): Check, if we do not count the same episode
                #     # multiple times.
                #     # Also add to the module episode counter.
                #     self._num_module_episodes[module_id] += 1

    def _add_new_module_indices(
        self,
        ma_episode: MultiAgentEpisode,
        episode_idx: int,
        ma_episode_exists: bool = True,
    ) -> None:
        """Adds the module indices for new episode chunks.

        Args:
            ma_episode: The multi-agent episode to add the module indices for.
            episode_idx: The index of the episode in the `self.episodes`.
            ma_episode_exists: Whether `ma_episode` is already in this buffer (with a
                predecessor chunk to which we'll concatenate `ma_episode` later).
        """
        existing_ma_episode = None
        if ma_episode_exists:
            existing_ma_episode = self.episodes[
                self.episode_id_to_index[ma_episode.id_] - self._num_episodes_evicted
            ]

        # Note, we iterate through the agent episodes b/c we want to store records
        # and some agents could not have entered the environment.
        for agent_id in ma_episode.agent_episodes:
            # Get the corresponding module id.
            module_id = ma_episode.module_for(agent_id)
            # Get the module episode.
            module_eps = ma_episode.agent_episodes[agent_id]

            # Is the agent episode already in the buffer's existing `ma_episode`?
            if ma_episode_exists and agent_id in existing_ma_episode.agent_episodes:
                existing_sa_eps_len = len(existing_ma_episode.agent_episodes[agent_id])
            # Otherwise, it is a new single-agent episode and we increase the counter.
            else:
                existing_sa_eps_len = 0
                self._num_module_episodes[module_id] += 1

            # Add new module indices.
            self._module_to_indices[module_id].extend(
                [
                    (
                        # Keep the MAE index for sampling
                        episode_idx,
                        agent_id,
                        existing_sa_eps_len + i,
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
