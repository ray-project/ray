import logging
from collections import defaultdict
from typing import List
import random

import numpy as np

from ray.rllib.evaluation.postprocessing import discount_cumsum
from ray.rllib.policy.sample_batch import SampleBatch, concat_samples, MultiAgentBatch
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)


def front_pad_with_zero(arr: np.ndarray, max_seq_len: int):
    """Pad arr on the front/left with 0 up to max_seq_len."""
    length = arr.shape[0]
    pad_length = max_seq_len - length
    if pad_length > 0:
        return np.concatenate(
            [np.zeros((pad_length, *arr.shape[1:]), dtype=arr.dtype), arr], axis=0
        )
    else:
        return arr


class SegmentationBuffer:
    """A minimal replay buffer used by Decision Transformer (DT)
    to process episodes into max_seq_len length segments and do shuffling.
    """

    def __init__(
        self,
        capacity: int = 20,
        max_seq_len: int = 20,
        max_ep_len: int = 1000,
    ):
        """
        Args:
            capacity: Maximum number of episodes the buffer can store.
            max_seq_len: Length of segments that are sampled.
            max_ep_len: Maximum length of episodes added.
        """
        self.capacity = capacity
        self.max_seq_len = max_seq_len
        self.max_ep_len = max_ep_len

        self._buffer: List[SampleBatch] = []

    def add(self, batch: SampleBatch):
        """Add a SampleBatch of episodes. Replace if full.

        Args:
            batch: SampleBatch of full episodes.
        """
        episodes = batch.split_by_episode(key=SampleBatch.DONES)
        for episode in episodes:
            self._add_single_episode(episode)

    def _add_single_episode(self, episode: SampleBatch):
        ep_len = episode.env_steps()

        if ep_len > self.max_ep_len:
            raise ValueError(
                f"The maximum rollout length is {self.max_ep_len} but we tried to add a"
                f"rollout of {episode.env_steps()} steps to the SegmentationBuffer."
            )

        # compute returns to go
        rewards = episode[SampleBatch.REWARDS].reshape(-1)
        rtg = discount_cumsum(rewards, 1.0)
        # rtg needs to be one longer than the rest for return targets during training.
        rtg = np.concatenate([rtg, np.zeros((1,), dtype=np.float32)], axis=0)
        episode[SampleBatch.RETURNS_TO_GO] = rtg[:, None]

        # Add timesteps and masks
        episode[SampleBatch.T] = np.arange(ep_len, dtype=np.int32)
        episode[SampleBatch.ATTENTION_MASKS] = np.ones(ep_len, dtype=np.float32)

        # Add to the buffer.
        if len(self._buffer) < self.capacity:
            self._buffer.append(episode)
        else:
            # TODO: add config for sampling and eviction policies.
            replace_ind = random.randint(0, self.capacity - 1)
            self._buffer[replace_ind] = episode

    def sample(self, batch_size: int) -> SampleBatch:
        """Sample segments from the buffer.

        Args:
            batch_size: number of segments to sample.

        Returns:
            SampleBatch of segments with keys and shape {
                OBS: [batch_size, max_seq_len, obs_dim],
                ACTIONS: [batch_size, max_seq_len, act_dim],
                RETURNS_TO_GO: [batch_size, max_seq_len + 1, 1],
                T: [batch_size, max_seq_len],
                ATTENTION_MASKS: [batch_size, max_seq_len],
            }
        """
        samples = [self._sample_single() for _ in range(batch_size)]
        return concat_samples(samples)

    def _sample_single(self) -> SampleBatch:
        # TODO: sample proportional to episode length
        # Sample a random episode from the buffer and then sample a random
        # segment from that episode.
        buffer_ind = random.randint(0, len(self._buffer) - 1)

        episode = self._buffer[buffer_ind]
        ep_len = episode[SampleBatch.OBS].shape[0]

        # ei (end index) is exclusive
        ei = random.randint(1, ep_len)
        # si (start index) is inclusive
        si = max(ei - self.max_seq_len, 0)

        # Slice segments from obs, actions, timesteps, and rtgs
        obs = episode[SampleBatch.OBS][si:ei]
        actions = episode[SampleBatch.ACTIONS][si:ei]
        timesteps = episode[SampleBatch.T][si:ei]
        masks = episode[SampleBatch.ATTENTION_MASKS][si:ei]
        # Note that returns-to-go needs an extra elem as the rtg target for the last
        # action token passed into the transformer.
        returns_to_go = episode[SampleBatch.RETURNS_TO_GO][si : ei + 1]

        # Front-pad if we're at the beginning of the episode and we need more tokens
        # to pass into the transformer. Or if the episode length is shorter
        # than max_seq_len.
        obs = front_pad_with_zero(obs, self.max_seq_len)
        actions = front_pad_with_zero(actions, self.max_seq_len)
        returns_to_go = front_pad_with_zero(returns_to_go, self.max_seq_len + 1)
        timesteps = front_pad_with_zero(timesteps, self.max_seq_len)
        masks = front_pad_with_zero(masks, self.max_seq_len)

        assert obs.shape[0] == self.max_seq_len
        assert actions.shape[0] == self.max_seq_len
        assert timesteps.shape[0] == self.max_seq_len
        assert masks.shape[0] == self.max_seq_len
        assert returns_to_go.shape[0] == self.max_seq_len + 1

        return SampleBatch(
            {
                SampleBatch.OBS: obs[None],
                SampleBatch.ACTIONS: actions[None],
                SampleBatch.RETURNS_TO_GO: returns_to_go[None],
                SampleBatch.T: timesteps[None],
                SampleBatch.ATTENTION_MASKS: masks[None],
            }
        )


class MultiAgentSegmentationBuffer:
    """A minimal replay buffer used by Decision Transformer (DT)
    to process episodes into max_seq_len length segments and do shuffling.
    Stores MultiAgentSample.
    """

    def __init__(
        self,
        capacity: int = 20,
        max_seq_len: int = 20,
        max_ep_len: int = 1000,
    ):
        """
        Args:
            capacity: Maximum number of episodes the buffer can store.
            max_seq_len: Length of segments that are sampled.
            max_ep_len: Maximum length of episodes added.
        """

        def new_buffer():
            return SegmentationBuffer(capacity, max_seq_len, max_ep_len)

        self.buffers = defaultdict(new_buffer)

    def add(self, batch: SampleBatchType):
        """Add a MultiAgentBatch of episodes. Replace if full.

        Args:
            batch: MultiAgentBatch of full episodes.
        """
        # Make a copy so the replay buffer doesn't pin plasma memory.
        batch = batch.copy()
        # Handle everything as if multi-agent.
        batch = batch.as_multi_agent()

        for policy_id, sample_batch in batch.policy_batches.items():
            self.buffers[policy_id].add(sample_batch)

    def sample(self, batch_size: int) -> MultiAgentBatch:
        """Sample segments from the buffer.

        Args:
            batch_size: number of segments to sample.

        Returns:
            MultiAgentBatch of segments with keys and shape {
                OBS: [batch_size, max_seq_len, obs_dim],
                ACTIONS: [batch_size, max_seq_len, act_dim],
                RETURNS_TO_GO: [batch_size, max_seq_len + 1, 1],
                T: [batch_size, max_seq_len],
                ATTENTION_MASKS: [batch_size, max_seq_len],
            }
        """
        samples = {}
        for policy_id, buffer in self.buffers.items():
            samples[policy_id] = buffer.sample(batch_size)
        return MultiAgentBatch(samples, batch_size)
