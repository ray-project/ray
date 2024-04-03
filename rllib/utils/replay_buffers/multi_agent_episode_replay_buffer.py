import copy
from collections import defaultdict
from typing import Dict, List, Tuple, Union

from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils import force_list


class MultiAgentEpisodeReplayBuffer(EpisodeReplayBuffer):
    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 64,
        **kwargs,
    ):
        # Initialize the base episode replay buffer.
        super().__init__(
            capacity=capacity,
            batch_size_B=batch_size_B,
            batch_length_T=batch_length_T,
            **kwargs,
        )

        # Stores indices of module (single-agent) timesteps.
        self._module_indices: Dict[str, List[Tuple[int]]] = defaultdict(list)
        # Stores for each module the episode indices.
        self.module_episode_id_to_index: Dict[str, Dict[str, int]] = defaultdict(dict)

        # Stores the number of single-agent timesteps in the buffer.
        self._num_agent_timesteps: int = 0
        # Stores the number of single-agent timesteps per module.
        self._num_module_timesteps: Dict[str, int] = defaultdict(int)

        # Stores the number of added single-agent timesteps over the
        # lifetime of the buffer.
        self._num_agent_timesteps_added: int = 0
        # Stores the number of added single-agent timesteps per module
        # over the lifetime of the buffer.
        self._num_module_timesteps_added: Dict[str, int] = defaultdict(int)

        self._num_module_episodes: Dict[str, int] = defaultdict(int)
        # Stores the number of module episodes evicted. Note, this is
        # important for indexing.
        self._num_module_episodes_evicted: Dict[str, int] = defaultdict(int)

    def add(
        self,
        episodes: Union[List["MultiAgentEpisode"], "MultiAgentEpisode"],
    ) -> None:

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
            eps_evicted.append(self.episodes.popleft())
            eps_evicted_ids.append(eps_evicted[-1].id_)
            eps_evicted_idxs.append(self.episode_id_to_index.pop(eps_evicted_ids[-1]))
            # If this episode has a new chunk in the new episodes added,
            # we subtract it again.
            # TODO (sven, simon): Should we just treat such an episode chunk
            # as a new episode?
            if eps_evicted_ids[-1] in new_episode_ids:
                new_eps_to_evict = episodes[new_episode_ids.index(eps_evicted_ids[-1])]
                self._num_timesteps -= new_eps_to_evict.env_steps()
                self._num_timesteps_added -= new_eps_to_evict.env_steps()
                episodes.remove(new_eps_to_evict)
            # Remove the timesteps of the evicted episode from the counter.
            self._num_timesteps -= eps_evicted[-1].env_steps()
            self._num_agent_timesteps -= eps_evicted[-1].agent_steps()
            self._num_episodes_evicted += 1
            # Remove the module timesteps of the evicted episode from the counters.
            self._evict_module_episodes(eps_evicted[-1])

        # Add agent and module steps.
        for eps in episodes:
            self._num_agent_timesteps += eps.agent_steps()
            self._num_agent_timesteps_added += eps.agent_steps()
            # Update the module counters by the module timesteps.
            self._update_module_counters(eps)

        # Remove corresponding indices, if episodes were evicted.
        if eps_evicted_idxs:
            new_indices = []
            for idx_tuple in self._indices:
                # If episode index is not from an evicted episode, keep it.
                if idx_tuple[0] not in eps_evicted_idxs:
                    new_indices.append(idx_tuple)
            # Assign the new list of indices.
            self._indices = new_indices
            # Also remove corresponding module indices.
            for module_id, module_indices in self._module_indices.items():
                new_module_indices = []
                for idx_triple in module_indices:
                    if idx_triple[0] not in eps_evicted_idxs:
                        new_module_indices.append(idx_triple)
                self._module_indices[module_id] = new_module_indices

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
                self._add_new_module_indices(eps, eps_idx)
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
                self._add_new_module_indices(eps, eps_idx)

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

    def _evict_module_episodes(self, multi_agent_eps: MultiAgentEpisode) -> None:
        """Evicts the module episodes from the buffer adn updates all counters.

        Args:
            multi_agent_eps: The multi-agent episode to evict from the buffer.
        """

        # Note we need to take the agent ids from the evicted episode because
        # different episodes can have different agents and module mappings.
        for agent_id in multi_agent_eps.agent_ids:
            # Retrieve the corresponding module ID and module episode.
            module_id = multi_agent_eps._agent_to_module_mapping[agent_id]
            module_eps = multi_agent_eps.agent_episodes[agent_id]
            # Remove the episode index from the module episode index map.
            self.module_episode_id_to_index[module_id].pop(module_eps.id_)
            # Update all counters.
            self._num_module_timesteps[module_id] -= module_eps.env_steps()
            self._num_module_timesteps_added[module_id] -= module_eps.env_steps()
            self._num_module_episodes[module_id] -= 1
            self._num_module_episodes_evicted[module_id] += 1

    def _update_module_counters(self, multi_agent_episode: MultiAgentEpisode) -> None:
        """Updates the module counters after adding an episode.

        Args:
            multi_agent_episode: The multi-agent episode to update the module counters
                for.
        """
        for agent_id in multi_agent_episode.agent_ids:
            agent_steps = multi_agent_episode.agent_episodes[agent_id].env_steps()
            # Only add if the agent has stepped in the episode (chunk).
            if agent_steps > 0:
                # Receive the corresponding module ID.
                # TODO (sven, simon): Is there always a mapping? What if not?
                # Is then module_id == agent_id?
                module_id = multi_agent_episode._agent_to_module_mapping[agent_id]
                self._num_module_timesteps[module_id] += agent_steps
                self._num_module_timesteps_added[module_id] += agent_steps
                # Also add to the module episode counter.
                self._num_module_episodes[module_id] += 1

    def _add_new_module_indices(
        self, multi_agent_episode: MultiAgentEpisode, episode_idx: int
    ) -> None:
        """Adds the module indices for new episode chunks.

        Args:
            multi_agent_episode: The multi-agent episode to add the module indices for.
            episode_idx: The index of the episode in the `self.episodes`.
        """

        for agent_id in multi_agent_episode.agent_ids:
            # Get the corresponding module id.
            module_id = multi_agent_episode._agent_to_module_mapping[agent_id]
            # Get the module episode.
            module_eps = multi_agent_episode.agent_episodes[agent_id]
            if module_eps.id_ in self.module_episode_id_to_index[module_id]:
                module_eps_idx = self.module_episode_id_to_index[module_id][
                    module_eps.id_
                ]
                existing_eps_len = len(
                    self.episodes[episode_idx].agent_episodes[agent_id]
                )
            else:
                module_eps_idx = (
                    self._num_module_episodes[module_id]
                    - 1
                    + self._num_module_episodes_evicted[module_id]
                )
                existing_eps_len = 0
                # Add the module episode ID to the index map.
                self.module_episode_id_to_index[module_id][
                    module_eps.id_
                ] = module_eps_idx
            # Add new module indices.
            self._module_indices[module_id].extend(
                [
                    (
                        # Keep the MAE index for sampling
                        episode_idx,
                        module_eps_idx,
                        existing_eps_len + i + 1,
                    )
                    for i in range(len(module_eps))
                ]
            )
