import copy
import numpy as np

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

        for eps in episodes:
            # Make sure we don't change what's coming in from the user.
            # TODO (sven): It'd probably be better to make sure in the EnvRunner to not
            #  hold on to episodes (for metrics purposes only) that we are returning
            #  back to the user from `EnvRunner.sample()`. Then we wouldn't have to
            #  do any copying. Instead, either compile the metrics right away on the
            #  EnvRunner OR compile metrics entirely on the Algorithm side (this is
            #  actually preferred).
            eps = copy.deepcopy(eps)

            # Ongoing episode, concat to existing record.
            if eps.id_ in self.episode_id_to_index:
                eps_idx = self.episode_id_to_index[eps.id_]
                existing_eps = self.episodes[eps_idx - self._num_episodes_evicted]
                old_len = len(existing_eps)
                self._indices.extend(
                    [(eps_idx, old_len + i, None) for i in range(len(eps))]
                )
                existing_eps.concat_episode(eps)
            # New episode. Add to end of our episodes deque.
            else:
                self.episodes.append(eps)
                eps_idx = len(self.episodes) - 1 + self._num_episodes_evicted
                self.episode_id_to_index[eps.id_] = eps_idx
                self._indices.extend(
                    [
                        (
                            # Include the episode index,
                            eps_idx,
                            # the timestep
                            i,
                            # and the segment tree index.
                            # Set to `None`.
                            None,
                        )
                        for i in range(len(eps))
                    ]
                )

            self._num_timesteps += len(eps)
            self._num_timesteps_added += len(eps)

            # Eject old records from front of deque (only if we have more than 1 episode
            # in the buffer).
            evicted_indices = []
            while self._num_timesteps > self.capacity and self.get_num_episodes() > 1:
                # Eject oldest episode.
                evicted_eps = self.episodes.popleft()
                evicted_eps_len = len(evicted_eps)
                evicted_indices += [evicted_eps]
                # Correct our size.
                self._num_timesteps -= evicted_eps_len

                # Erase episode from all our indices:
                # 1) Main episode index.
                del self.episode_id_to_index[evicted_eps.id_]

                # 2) All timestep indices that this episode owned.
                self._indices = self._indices[evicted_eps_len:]

                # Increase episode evicted counter.
                self._num_episodes_evicted += 1

    def _get_segment_tree_index(self) -> Optional[int]:
        pass
