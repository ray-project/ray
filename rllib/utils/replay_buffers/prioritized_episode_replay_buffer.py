import copy
import numpy as np

from collections import deque
from typing import List, Optional, Union
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.execution.segment_tree import MinSegmentTree, SumSegmentTree
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.annotations import override


class PrioritizedEpisodeReplayBuffer(EpisodeReplayBuffer):
    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 64,
        alpha: float = 1.0,
    ):
        super().__init__(
            capacity=capacity, batch_size_B=batch_size_B, batch_length_T=batch_length_T
        )

        assert alpha > 0
        self._alpha = alpha

        # Initialize segment trees for the priority weights. Note, b/c the trees a
        # binary we need for them a capacity that is an exponential of 2.
        tree_capacity = 2 ** np.ceil(np.log2(self.capacity))

        self._max_priority = 1.0
        self._sum_segment = SumSegmentTree(tree_capacity)
        self._min_segment = MinSegmentTree(tree_capacity)
        # At initialization all nodes are free.
        self._free_nodes = deque(list(range(tree_capacity)), maxlen=tree_capacity)

    @override(EpisodeReplayBuffer)
    def add(
        self,
        episodes: Union[List["SingleAgentEpisode"], "SingleAgentEpisode"],
        weight: Optional[float] = None,
    ) -> None:
        if weight is None:
            weight = self._max_priority

        # self._sum_segment[self._indices[]] = weight ** self.alpha
        # self._min_segment[self._] = weight ** self.alpha

        if isinstance(episodes, SingleAgentEpisode):
            episodes = [episodes]

        new_episode_ids = []
        for eps in episodes:
            new_episode_ids.append(eps.id_)
            self._num_timesteps += len(eps)
            self._num_timesteps_added += len(eps)

        new_list = []
        eps_evicted = []
        eps_ids = []
        while self.get_num_timesteps() > self.capacity and self.get_num_episodes() != 1:
            # Evict episode
            eps_evicted.append(self.episodes.popleft())
            eps_ids.append(self.episode_id_to_index(eps_evicted.id_))
            # If this episode has a new chunk in the new episodes added,
            # we subtract it again.
            # TODO (sven, simon): Should we just treat such an episode chunk
            # as a new episode?
            if eps_ids[-1] in new_episode_ids:
                len_to_subtract = len(episodes[new_episode_ids.index(eps_ids[-1])])
                self._num_timesteps -= len_to_subtract
                self._num_timesteps_added -= len_to_subtract
            # Remove the timesteps of the evicted episode from the counter.
            self._num_timesteps -= len(eps_evicted[-1])

        # Remove corresponding indices.
        for idx_triple in self._indices:
            if idx_triple[0] in eps_ids:
                self._free_nodes.append(idx_triple[2])
            else:
                new_list.append(idx_triple)
        # Assign the new list of indices.
        self._indices = new_list

        # Now append the indices for the new epsiodes.
        for eps in episodes:
            # If the episode chunk is part of an evicted episode continue.
            if eps.id_ in eps_ids:
                continue
            # Otherwise, add the episode data to the buffer.
            else:
                eps = copy.deepcopy(eps)
                # If the episode is part of an already existing episode, concatenate.
                if eps.id_ in self.episode_id_to_index:
                    eps_idx = self.episode_id_to_index[eps.id_]
                    existing_eps = self.episodes[eps_idx - self._num_episodes_evicted]
                    old_len = len(existing_eps)
                    self._indices.extend(
                        [
                            (
                                eps_idx,
                                old_len + i,
                                # Get the index in the segment trees.
                                self._get_free_node_and_assign(weight),
                            )
                            for i in range(len(eps))
                        ]
                    )
                    existing_eps.concat_episode(eps)
                # Otherwise, create a new entry.
                else:
                    self.episodes.append(eps)
                    eps_idx = len(self.episodes) - 1 + self._num_episodes_evicted
                    self.episode_id_to_index[eps.id_] = eps_idx
                    self._indices.extend(
                        [
                            (eps_idx, i, self._get_free_node_and_assign(weight))
                            for i in range(len(eps))
                        ]
                    )

    def _get_free_node_and_assign(self, weight: float = 1.0) -> int:
        # Get an index from the free nodes in the segment trees.
        idx = self._free_nodes.popleft()
        # Add the weight to the segments.
        self._sum_segment[idx] = weight**self.alpha
        self._min_segment[idx] = weight**self.alpha
        # Return the index.
        return idx
