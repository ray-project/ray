import copy
import numpy as np

from collections import deque
from typing import List, Optional, Union
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.execution.segment_tree import MinSegmentTree, SumSegmentTree
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType


# TODO (simon): Set `self._indices` to a dictionary to have 
# O(1) average complexity when searching for an index given 
# in sampling from the `SumSegmentTree`.
class PrioritizedEpisodeReplayBuffer(EpisodeReplayBuffer):
    def __init__(
        self,
        capacity: int = 10000,
        *,
        batch_size_B: int = 16,
        batch_length_T: int = 1,
        alpha: float = 1.0,
    ):
        super().__init__(
            capacity=capacity, batch_size_B=batch_size_B, batch_length_T=batch_length_T
        )

        assert alpha > 0
        self._alpha = alpha

        # Initialize segment trees for the priority weights. Note, b/c the trees a
        # binary we need for them a capacity that is an exponential of 2.
        tree_capacity = int(2 ** np.ceil(np.log2(self.capacity)))

        self._max_priority = 1.0
        self._sum_segment = SumSegmentTree(2 * tree_capacity)
        self._min_segment = MinSegmentTree(2 * tree_capacity)
        # At initialization all nodes are free.
        self._free_nodes = deque(
            list(range(2 * tree_capacity)), maxlen=2 * tree_capacity
        )
        self._tree_idx_to_sample_idx = {}

    @override(EpisodeReplayBuffer)
    def add(
        self,
        episodes: Union[List["SingleAgentEpisode"], "SingleAgentEpisode"],
        weight: Optional[float] = None,
    ) -> None:
        if weight is None:
            weight = self._max_priority

        if isinstance(episodes, SingleAgentEpisode):
            episodes = [episodes]

        new_episode_ids = []
        for eps in episodes:
            new_episode_ids.append(eps.id_)
            self._num_timesteps += len(eps)
            self._num_timesteps_added += len(eps)

        eps_evicted = []
        eps_evicted_ids = []
        eps_evicted_idxs = []
        while (
            self._num_timesteps > self.capacity
            and self._num_remaining_episodes(new_episode_ids, eps_evicted_ids) != 1
        ):
            # Evict episode
            eps_evicted.append(self.episodes.popleft())
            eps_evicted_ids.append(eps_evicted[-1].id_)
            eps_evicted_idxs.append(self.episode_id_to_index[eps_evicted_ids[-1]])
            del self.episode_id_to_index[eps_evicted[-1].id_]
            # If this episode has a new chunk in the new episodes added,
            # we subtract it again.
            # TODO (sven, simon): Should we just treat such an episode chunk
            # as a new episode?
            if eps_evicted_idxs[-1] in new_episode_ids:
                len_to_subtract = len(
                    episodes[new_episode_ids.index(eps_evicted_idxs[-1])]
                )
                self._num_timesteps -= len_to_subtract
                self._num_timesteps_added -= len_to_subtract
            # Remove the timesteps of the evicted episode from the counter.
            self._num_timesteps -= len(eps_evicted[-1])
            self._num_episodes_evicted += 1

        # Remove corresponding indices.
        new_list = []
        for idx_triple in self._indices:
            if idx_triple[0] in eps_evicted_idxs:
                self._free_nodes.append(idx_triple[2])
            else:
                new_list.append(idx_triple)
        # Assign the new list of indices.
        self._indices = new_list

        # Now append the indices for the new episodes.
        for eps in episodes:
            # If the episode chunk is part of an evicted episode continue.
            if eps.id_ in eps_evicted_ids:
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

    def sample(
        self,
        num_items: Optional[int] = None,
        *,
        batch_size_B: Optional[int] = None,
        batch_length_T: Optional[int] = None,
        n_step: int = 1,
        beta: float = 0.0,
        include_infos: bool = False,
    ) -> SampleBatchType:
        assert beta >= 0.0

        if num_items is not None:
            assert batch_size_B is None, (
                "Cannot call `sample()` with both `num_items` and `batch_size_B` "
                "provided! Use either one."
            )
            batch_size_B = num_items

        # Use our default values if no sizes/lengths provided.
        batch_size_B = batch_size_B or self.batch_size_B
        batch_length_T = batch_length_T or self.batch_length_T

        # Rows to return.
        observations = [[] for _ in range(batch_size_B)]
        next_observations = [[] for _ in range(batch_size_B)]
        actions = [[] for _ in range(batch_size_B)]
        rewards = [[] for _ in range(batch_size_B)]
        is_terminated = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_truncated = [[False] * batch_length_T for _ in range(batch_size_B)]
        weights = [[] for _ in range(batch_size_B)]
        # If `info` should be included, construct also a container for them.
        # TODO (simon): Add also `extra_model_outs`.
        if include_infos:
            infos = [[] for _ in range(batch_size_B)]

        self._last_sampled_indices = []

        # Sample proportionally from replay buffer's segments using the weights.

        total_segment_sum = self._sum_segment.sum()
        p_min = self._min_segment.min() / total_segment_sum
        max_weight = (p_min * self.get_num_timesteps()) ** (-beta)
        B = 0
        while B < batch_size_B:
            # First, draw a random sample from Uniform(0, sum over all weights).
            # Note, like this transitions with higher weight get sampled more
            # often (as more random draws fall into larger intervals).
            random_sum = self.rng.random() * self._sum_segment.sum(
                0, self.get_num_timesteps()
            )
            # Get the highest index in the sum-tree for which the sum is
            # smaller or equal the random sum sample.
            # Note, we sample `o_(t + n_step)` as this is the state that
            # brought the information contained in the TD-error (see Schaul
            # et al. (2018), Algorithm 1).
            idx = self._sum_segment.find_prefixsum_idx(random_sum)
            # Get the theoretical probability mass for drawing this sample.
            p_sample = self._sum_segment[idx] / total_segment_sum
            # Compute the importance sampling weight.
            weight = (p_sample * self.get_num_timesteps()) ** (-beta)
            # Now, get the transition stored at this index.
            index_tuple = self._indices[idx]

            # Compute the actual episode index (offset by the number of
            # already evicted episodes)
            episode_idx, episode_ts = (
                index_tuple[0] - self._num_episodes_evicted,
                index_tuple[1],
            )
            episode = self.episodes[episode_idx]
            # If we are at the end of an episode, continue.
            # Note, priority sampling got us `o_(t+n_step)` and we need
            # for the loss calculation in addition `o_t`.
            if episode_ts - n_step < 0:
                continue

            # Starting a new chunk.
            # Ensure that each row contains a tuple of the form:
            #   (o_t, a_t, sum(r_(t:t+n_step)), o_(t+n_step))
            # TODO (simon): Implement version for sequence sampling when using RNNs.
            eps_observations = episode.get_observations(
                slice(episode_ts - n_step, episode_ts + 1)
            )
            # Note, the reward that is collected by transitioning from `o_t` to
            # `o_(t+1)` is stored in the next transition in `SingleAgentEpisode`.
            eps_rewards = episode.get_rewards(slice(episode_ts - n_step, episode_ts))
            observations[B].append(eps_observations[0])
            next_observations[B].append(eps_observations[-1])
            # Note, this will be the reward after executing action a_(episode_ts-n_step+1).
            # For `n_step>1` this will be the sum of all rewards that were collected
            # over the last n steps.
            rewards[B].append(sum(eps_rewards))
            # Note, `SingleAgentEpisode` stores the action that followed
            # `o_t` with `o_(t+1)`, therefore, we need the next one.
            actions[B].append(episode.get_actions(episode_ts - n_step))
            if include_infos:
                # If infos are included we include the ones from the last timestep
                # as usually the info contains additional values about the last state.
                infos[B].append(episode.get_infos(episode_ts))

            # If the sampled time step is the episode's last time step check, if
            # the episode is terminated or truncated.
            if episode_ts == episode.t:
                is_terminated[B] = episode.is_terminated
                is_truncated[B] = episode.is_truncated

            # TODO (simon): Check, if we have to correct here for sequences
            # later.
            actual_size = 1
            weights[B].append(weight / max_weight * actual_size)

            # Increment counter.
            B += 1

            # Keep track of sampled indices for updating priorities later.
            self._last_sampled_indices.append(idx)

        self.sampled_timesteps += batch_size_B

        # TODO Return SampleBatch instead of this simpler dict.
        ret = {
            SampleBatch.OBS: np.array(observations),
            SampleBatch.ACTIONS: np.array(actions),
            SampleBatch.REWARDS: np.array(rewards),
            SampleBatch.NEXT_OBS: np.array(next_observations),
            SampleBatch.TERMINATEDS: np.array(is_terminated),
            SampleBatch.TRUNCATEDS: np.array(is_truncated),
            "weights": np.array(weights),
        }
        if include_infos:
            ret.update(
                {
                    SampleBatch.INFOS: np.array(infos),
                }
            )

        return ret

    def _get_free_node_and_assign(self, sample_index, weight: float = 1.0) -> int:
        # Get an index from the free nodes in the segment trees.
        idx = self._free_nodes.popleft()
        # Add the weight to the segments.
        self._sum_segment[idx] = weight**self._alpha
        self._min_segment[idx] = weight**self._alpha
        # Add an entry to the index mapping.
        self._tree_idx_to_sample_idx[idx] = sample_index
        # Return the index.
        return idx

    def _num_remaining_episodes(self, new_eps, evicted_eps):
        return len(
            set(self.episode_id_to_index.keys()).union(set(new_eps)) - set(evicted_eps)
        )
