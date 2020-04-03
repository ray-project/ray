import numpy as np
import random
import sys

from ray.rllib.optimizers.segment_tree import SumSegmentTree, MinSegmentTree
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.window_stat import WindowStat


@DeveloperAPI
class ReplayBuffer:
    @DeveloperAPI
    def __init__(self, size):
        """Create Prioritized Replay buffer.

        Parameters
        ----------
        size: int
          Max number of transitions to store in the buffer. When the buffer
          overflows the old memories are dropped.
        """
        self._storage = []
        self._maxsize = size
        self._next_idx = 0
        self._hit_count = np.zeros(size)
        self._eviction_started = False
        self._num_added = 0
        self._num_sampled = 0
        self._evicted_hit_stats = WindowStat("evicted_hit", 1000)
        self._est_size_bytes = 0

    def __len__(self):
        return len(self._storage)

    @DeveloperAPI
    def add(self, obs_t, action, reward, obs_tp1, done, weight):
        data = (obs_t, action, reward, obs_tp1, done)
        self._num_added += 1

        if self._next_idx >= len(self._storage):
            self._storage.append(data)
            self._est_size_bytes += sum(sys.getsizeof(d) for d in data)
        else:
            self._storage[self._next_idx] = data
        if self._next_idx + 1 >= self._maxsize:
            self._eviction_started = True
        self._next_idx = (self._next_idx + 1) % self._maxsize
        if self._eviction_started:
            self._evicted_hit_stats.push(self._hit_count[self._next_idx])
            self._hit_count[self._next_idx] = 0

    def _encode_sample(self, idxes):
        obses_t, actions, rewards, obses_tp1, dones = [], [], [], [], []
        for i in idxes:
            data = self._storage[i]
            obs_t, action, reward, obs_tp1, done = data
            obses_t.append(np.array(unpack_if_needed(obs_t), copy=False))
            actions.append(np.array(action, copy=False))
            rewards.append(reward)
            obses_tp1.append(np.array(unpack_if_needed(obs_tp1), copy=False))
            dones.append(done)
            self._hit_count[i] += 1
        return (np.array(obses_t), np.array(actions), np.array(rewards),
                np.array(obses_tp1), np.array(dones))

    @DeveloperAPI
    def sample_idxes(self, batch_size):
        return np.random.randint(0, len(self._storage), batch_size)

    @DeveloperAPI
    def sample_with_idxes(self, idxes):
        self._num_sampled += len(idxes)
        return self._encode_sample(idxes)

    @DeveloperAPI
    def sample(self, batch_size):
        """Sample a batch of experiences.

        Parameters
        ----------
        batch_size: int
            How many transitions to sample.

        Returns
        -------
        obs_batch: np.array
          batch of observations
        act_batch: np.array
          batch of actions executed given obs_batch
        rew_batch: np.array
          rewards received as results of executing act_batch
        next_obs_batch: np.array
          next set of observations seen after executing act_batch
        done_mask: np.array
          done_mask[i] = 1 if executing act_batch[i] resulted in
          the end of an episode and 0 otherwise.
        """
        idxes = [
            random.randint(0,
                           len(self._storage) - 1) for _ in range(batch_size)
        ]
        self._num_sampled += batch_size
        return self._encode_sample(idxes)

    @DeveloperAPI
    def stats(self, debug=False):
        data = {
            "added_count": self._num_added,
            "sampled_count": self._num_sampled,
            "est_size_bytes": self._est_size_bytes,
            "num_entries": len(self._storage),
        }
        if debug:
            data.update(self._evicted_hit_stats.stats())
        return data


@DeveloperAPI
class PrioritizedReplayBuffer(ReplayBuffer):
    @DeveloperAPI
    def __init__(self, size, alpha):
        """Create Prioritized Replay buffer.

        Parameters
        ----------
        size: int
          Max number of transitions to store in the buffer. When the buffer
          overflows the old memories are dropped.
        alpha: float
          how much prioritization is used
          (0 - no prioritization, 1 - full prioritization)

        See Also
        --------
        ReplayBuffer.__init__
        """
        super(PrioritizedReplayBuffer, self).__init__(size)
        assert alpha > 0
        self._alpha = alpha

        it_capacity = 1
        while it_capacity < size:
            it_capacity *= 2

        self._it_sum = SumSegmentTree(it_capacity)
        self._it_min = MinSegmentTree(it_capacity)
        self._max_priority = 1.0
        self._prio_change_stats = WindowStat("reprio", 1000)

    @DeveloperAPI
    def add(self, obs_t, action, reward, obs_tp1, done, weight):
        """See ReplayBuffer.store_effect"""

        idx = self._next_idx
        super(PrioritizedReplayBuffer, self).add(obs_t, action, reward,
                                                 obs_tp1, done, weight)
        if weight is None:
            weight = self._max_priority
        self._it_sum[idx] = weight**self._alpha
        self._it_min[idx] = weight**self._alpha

    def _sample_proportional(self, batch_size):
        res = []
        for _ in range(batch_size):
            # TODO(szymon): should we ensure no repeats?
            mass = random.random() * self._it_sum.sum(0, len(self._storage))
            idx = self._it_sum.find_prefixsum_idx(mass)
            res.append(idx)
        return res

    @DeveloperAPI
    def sample_idxes(self, batch_size):
        return self._sample_proportional(batch_size)

    @DeveloperAPI
    def sample_with_idxes(self, idxes, beta):
        assert beta > 0
        self._num_sampled += len(idxes)

        weights = []
        p_min = self._it_min.min() / self._it_sum.sum()
        max_weight = (p_min * len(self._storage))**(-beta)

        for idx in idxes:
            p_sample = self._it_sum[idx] / self._it_sum.sum()
            weight = (p_sample * len(self._storage))**(-beta)
            weights.append(weight / max_weight)
        weights = np.array(weights)
        encoded_sample = self._encode_sample(idxes)
        return tuple(list(encoded_sample) + [weights, idxes])

    @DeveloperAPI
    def sample(self, batch_size, beta):
        """Sample a batch of experiences.

        compared to ReplayBuffer.sample
        it also returns importance weights and idxes
        of sampled experiences.


        Parameters
        ----------
        batch_size: int
          How many transitions to sample.
        beta: float
          To what degree to use importance weights
          (0 - no corrections, 1 - full correction)

        Returns
        -------
        obs_batch: np.array
          batch of observations
        act_batch: np.array
          batch of actions executed given obs_batch
        rew_batch: np.array
          rewards received as results of executing act_batch
        next_obs_batch: np.array
          next set of observations seen after executing act_batch
        done_mask: np.array
          done_mask[i] = 1 if executing act_batch[i] resulted in
          the end of an episode and 0 otherwise.
        weights: np.array
          Array of shape (batch_size,) and dtype np.float32
          denoting importance weight of each sampled transition
        idxes: np.array
          Array of shape (batch_size,) and dtype np.int32
          idexes in buffer of sampled experiences
        """
        assert beta >= 0.0
        self._num_sampled += batch_size

        idxes = self._sample_proportional(batch_size)

        weights = []
        p_min = self._it_min.min() / self._it_sum.sum()
        max_weight = (p_min * len(self._storage))**(-beta)

        for idx in idxes:
            p_sample = self._it_sum[idx] / self._it_sum.sum()
            weight = (p_sample * len(self._storage))**(-beta)
            weights.append(weight / max_weight)
        weights = np.array(weights)
        encoded_sample = self._encode_sample(idxes)
        return tuple(list(encoded_sample) + [weights, idxes])

    @DeveloperAPI
    def update_priorities(self, idxes, priorities):
        """Update priorities of sampled transitions.

        sets priority of transition at index idxes[i] in buffer
        to priorities[i].

        Parameters
        ----------
        idxes: [int]
          List of idxes of sampled transitions
        priorities: [float]
          List of updated priorities corresponding to
          transitions at the sampled idxes denoted by
          variable `idxes`.
        """
        assert len(idxes) == len(priorities)
        for idx, priority in zip(idxes, priorities):
            assert priority > 0
            assert 0 <= idx < len(self._storage)
            delta = priority**self._alpha - self._it_sum[idx]
            self._prio_change_stats.push(delta)
            self._it_sum[idx] = priority**self._alpha
            self._it_min[idx] = priority**self._alpha

            self._max_priority = max(self._max_priority, priority)

    @DeveloperAPI
    def stats(self, debug=False):
        parent = ReplayBuffer.stats(self, debug)
        if debug:
            parent.update(self._prio_change_stats.stats())
        return parent
