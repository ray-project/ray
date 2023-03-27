from collections import deque
import copy
from typing import List, Union

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer, StorageUnit
from ray.rllib.utils.replay_buffers.utils import ReplayBufferEpisode
from ray.rllib.utils.typing import SampleBatchType


class EpisodeReplayBuffer(ReplayBuffer):
    """Buffer that stores (completed or truncated) episodes.
    """
    def __init__(self, capacity: int = 10000):
        super().__init__(capacity=capacity, storage_unit=StorageUnit.TIMESTEPS)

        # The actual episode buffer.
        self.episodes = deque()
        # Maps (unique) episode IDs to the index under which to find this episode
        # within our `self.episodes` deque. Note that after ejection started, the
        # indices will NOT be changed. We will therefore need to offset these indices
        # by the number of episodes that have already been ejected.
        self.episode_id_to_index = {}
        # The number of episodes that have already been ejected from the buffer
        # due to reaching capacity. This is the offset, which we have to subtract
        # from the episode index to get the actual location within `self.episodes`.
        self._num_episodes_ejected = 0

        # List storing all index tuples: [eps_idx, ts_in_eps_idx], where ...
        # `eps_idx - self._num_episodes_ejected' is the index into self.episodes.
        # `ts_in_eps_idx` is the timestep index within that episode
        #  (0 = 1st timestep, etc..).
        # We sample uniformly from the set of these indices in a `sample()`
        # call.
        self._indices = []

        # The size of the buffer in timesteps.
        self.size = 0
        # How many timesteps have been sampled from the buffer in total?
        self.sampled_timesteps = 0

    @override(ReplayBuffer)
    def add(self, batch: SampleBatchType, **kwargs) -> None:
        episode_slices = batch.split_by_episode()
        episodes = [
            ReplayBufferEpisode.from_sample_batch(eps_slice)
            for eps_slice in episode_slices
        ]

        for eps in episodes:
            self.size += len(eps)
            # Ongoing episode, concat to existing record.
            if eps.id_ in self.episode_id_to_index:
                buf_idx = self.episode_id_to_index[eps.id_]
                existing_eps = self.episodes[buf_idx - self._num_episodes_ejected]
                old_len = len(existing_eps)
                self._indices.extend([(buf_idx, old_len + i) for i in range(len(eps))])
                existing_eps.concat_episode(eps)
            # New episode. Add to end of our buffer.
            else:
                self.episodes.append(eps)
                buf_idx = len(self.episodes) - 1 + self._num_episodes_ejected
                self.episode_id_to_index[eps.id_] = buf_idx
                self._indices.extend([(buf_idx, i) for i in range(len(eps))])

            # Eject old records from front of deque.
            while self.size > self.capacity:
                # Eject oldest episode.
                ejected_eps = self.episodes.popleft()
                ejected_eps_len = len(ejected_eps)
                # Correct our size.
                self.size -= len(ejected_eps)
                # Erase episode from all our indices.
                # Main episode index.
                ejected_idx = self.episode_id_to_index[ejected_eps.id_]
                del self.episode_id_to_index[ejected_eps.id_]
                # All timestep indices that this episode owned.
                new_indices = []
                idx_cursor = 0
                for i, idx_tuple in enumerate(self._indices):
                    if idx_cursor is not None and idx_tuple[0] == ejected_idx:
                        new_indices.extend(self._indices[idx_cursor:i])
                        idx_cursor = None
                    elif idx_cursor is None:
                        if idx_tuple[0] != ejected_idx:
                            idx_cursor = i
                        # Early-out: We reached the end of the to-be-ejected episode.
                        # We can stop searching further here.
                        elif idx_tuple[1] == ejected_eps_len - 1:
                            assert self._indices[i+1][0] != idx_tuple[0]
                            idx_cursor = i + 1
                            break
                if idx_cursor is not None:
                    new_indices.extend(self._indices[idx_cursor:])
                self._indices = new_indices
                # Increase episode ejected counter.
                self._num_episodes_ejected += 1

    def sample(self, batch_size_B: int = 16, batch_length_T: int = 64):
        # Uniformly sample n samples from self._indices.
        index_tuples_idx = []
        observations = [[] for _ in range(batch_size_B)]
        actions = [[] for _ in range(batch_size_B)]
        rewards = [[] for _ in range(batch_size_B)]
        is_first = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_last = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_terminated = [[False] * batch_length_T for _ in range(batch_size_B)]
        is_truncated = [[False] * batch_length_T for _ in range(batch_size_B)]

        B = 0
        T = 0
        idx_cursor = 0
        while B < batch_size_B:
            # Ran out of uniform samples -> Sample new set.
            if len(index_tuples_idx) <= idx_cursor:
                index_tuples_idx.extend(list(np.random.randint(
                    0, len(self._indices), size=batch_size_B * 10
                )))

            index_tuple = self._indices[index_tuples_idx[idx_cursor]]
            episode_idx, episode_ts = (
                index_tuple[0] - self._num_episodes_ejected, index_tuple[1]
            )
            episode = self.episodes[episode_idx]

            # Starting a new chunk, set continue to False.
            is_first[B][T] = True

            # Begin of new batch item (row).
            if len(rewards[B]) == 0:
                # And we are at the start of an episode: Set reward to 0.0.
                if episode_ts == 0:
                    rewards[B].append(0.0)
                # We are in the middle of an episode: Set reward and h_state to
                # the previous timestep's values.
                else:
                    rewards[B].append(episode.rewards[episode_ts - 1])
            # We are in the middle of a batch item (row). Concat next episode to this
            # row from the episode's beginning. In other words, we never concat
            # a middle of an episode to another truncated one.
            else:
                episode_ts = 0
                rewards[B].append(0.0)

            observations[B].extend(episode.observations[episode_ts:])
            # Repeat last action to have the same number of actions than observations.
            actions[B].extend(episode.actions[episode_ts:])
            actions[B].append(episode.actions[-1])
            # Number of rewards are also the same as observations b/c we have the
            # initial 0.0 one.
            rewards[B].extend(episode.rewards[episode_ts:])
            assert len(observations[B]) == len(actions[B]) == len(rewards[B])

            T = min(len(observations[B]), batch_length_T)

            # Set is_last=True.
            is_last[B][T-1] = True
            # If episode is terminated and we have reached the end of it, set
            # is_terminated=True.
            if episode.is_terminated and T == len(observations[B]):
                is_terminated[B][T-1] = True
            # If episode is truncated and we have reached the end of it, set
            # is_truncated=True.
            elif episode.is_truncated and T == len(observations[B]):
                is_truncated[B][T-1] = True

            # We are done with this batch row.
            if T == batch_length_T:
                # We may have overfilled this row: Clip trajectory at the end.
                observations[B] = observations[B][:batch_length_T]
                actions[B] = actions[B][:batch_length_T]
                rewards[B] = rewards[B][:batch_length_T]
                # Start filling the next row.
                B += 1
                T = 0

            # Use next sampled episode/ts pair to fill the row.
            idx_cursor += 1

        ret = {
            "obs": np.array(observations),
            "actions": np.array(actions),
            "rewards": np.array(rewards),
            "is_first": np.array(is_first),
            "is_last": np.array(is_last),
            "is_terminated": np.array(is_terminated),
            "is_truncated": np.array(is_truncated),
        }

        # Update our sampled counter.
        self.sampled_timesteps += batch_size_B * batch_length_T

        return ret

    def get_num_episodes(self):
        """Returns the number of episodes (completed or truncated) stored in the buffer.
        """
        return len(self.episodes)

    def get_num_timesteps(self):
        """Returns the number of individual timesteps stored in the buffer."""
        return len(self._indices)

    def get_sampled_timesteps(self):
        """Returns the number of timesteps that have been sampled in buffer's lifetime.
        """
        return self.sampled_timesteps

    def get_state(self):
        return np.array(list({
            "episodes": [eps.get_state() for eps in self.episodes],
            "episode_id_to_index": list(self.episode_id_to_index.items()),
            "_num_episodes_ejected": self._num_episodes_ejected,
            "_indices": self._indices,
            "size": self.size,
            "sampled_timesteps": self.sampled_timesteps,
        }.items()))

    def set_state(self, state):
        assert state[0][0] == "episodes"
        self.episodes = deque([
            Episode.from_state(eps_data) for eps_data in state[0][1]
        ])
        assert state[1][0] == "episode_id_to_index"
        self.episode_id_to_index = dict(state[1][1])
        assert state[2][0] == "_num_episodes_ejected"
        self._num_episodes_ejected = state[2][1]
        assert state[3][0] == "_indices"
        self._indices = state[3][1]
        assert state[4][0] == "size"
        self.size = state[4][1]
        assert state[5][0] == "sampled_timesteps"
        self.sampled_timesteps = state[5][1]
