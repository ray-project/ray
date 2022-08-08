import logging
from collections import defaultdict

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, concat_samples, MultiAgentBatch
from ray.rllib.utils.typing import SampleBatchType

logger = logging.getLogger(__name__)


class SegmentationBuffer:
    def __init__(
        self,
        capacity: int = 20,
        max_seq_len: int = 20,
        max_ep_len: int = 1000,
    ):
        self.capacity = capacity
        self.max_seq_len = max_seq_len
        self.max_ep_len = max_ep_len

        self._buffer = []

    def add(self, batch: SampleBatch):
        episodes = batch.split_by_episode()
        for episode in episodes:
            self._add_single(episode)

    def _add_single(self, episode: SampleBatch):
        # Truncate if episode too long.
        # Note: sometimes this happens if the dataset shuffles such that the same episode
        # is concatenated together twice.
        if episode.env_steps() > self.max_ep_len:
            logger.warning(
                f"The maximum rollout length is {self.max_ep_len} but we tried to add a"
                f"rollout of {episode.env_steps()} steps to the SegmentationBuffer. "
                f"This could be due to incorrect data in the dataset, or random "
                f"shuffling that caused a duplicate rollout."
            )
            episode = episode[: self.max_ep_len]

        if len(self._buffer) < self.capacity:
            self._buffer.append(episode)
        else:
            # TODO(charlesjsun): replace proportional to episode length
            replace_ind = np.random.randint(0, self.capacity)
            self._buffer[replace_ind] = episode

    def sample(self, batch_size: int) -> SampleBatch:
        samples = [self._sample_single() for _ in range(batch_size)]
        return concat_samples(samples)

    def _sample_single(self) -> SampleBatch:
        # TODO(charlesjsun): sample proportional to episode length
        buffer_ind = np.random.randint(0, len(self._buffer))
        episode: SampleBatch = self._buffer[buffer_ind]
        ep_len = episode[SampleBatch.OBS].shape[0]
        si = np.random.randint(-self.max_seq_len + 1, ep_len - self.max_seq_len + 1)
        ei = si + self.max_seq_len
        si = max(si, 0)

        assert 0 <= si < ei <= ep_len, f"si={si}, ei={ei}, ep_len={ep_len}"

        obs = episode[SampleBatch.OBS][si:ei]
        actions = episode[SampleBatch.ACTIONS][si:ei]
        # Note that returns-to-go needs one extra as the target for the last action
        returns_to_go = episode[SampleBatch.RETURNS_TO_GO][si : ei + 1].reshape(-1, 1)

        length = obs.shape[0]
        timesteps = np.arange(si, si + length)
        masks = np.ones(length, dtype=returns_to_go.dtype)

        # Back pad returns-to-go if at end
        if returns_to_go.shape[0] == length:
            returns_to_go = np.concatenate(
                [returns_to_go, np.zeros((1, 1), dtype=returns_to_go.dtype)], axis=0
            )

        # Front-pad if at beginning
        pad_length = self.max_seq_len - length
        if pad_length > 0:
            obs = np.concatenate(
                [np.zeros((pad_length, *obs.shape[1:]), dtype=obs.dtype), obs], axis=0
            )
            actions = np.concatenate(
                [
                    np.zeros((pad_length, *actions.shape[1:]), dtype=actions.dtype),
                    actions,
                ],
                axis=0,
            )
            returns_to_go = np.concatenate(
                [np.zeros((pad_length, 1), dtype=returns_to_go.dtype), returns_to_go],
                axis=0,
            )
            timesteps = np.concatenate(
                [np.zeros(pad_length, dtype=timesteps.dtype), timesteps], axis=0
            )
            masks = np.concatenate(
                [np.zeros(pad_length, dtype=masks.dtype), masks], axis=0
            )

        # TODO(charlesjsun): debug only?
        assert obs.shape[0] == self.max_seq_len
        assert actions.shape[0] == self.max_seq_len
        assert timesteps.shape[0] == self.max_seq_len
        assert masks.shape[0] == self.max_seq_len
        assert returns_to_go.shape[0] == self.max_seq_len + 1

        return SampleBatch(
            **{
                SampleBatch.OBS: obs[None],
                SampleBatch.ACTIONS: actions[None],
                SampleBatch.RETURNS_TO_GO: returns_to_go[None],
                SampleBatch.T: timesteps[None],
                SampleBatch.ATTENTION_MASKS: masks[None],
            }
        )


class MultiAgentSegmentationBuffer:
    def __init__(
        self,
        capacity: int = 20,
        max_seq_len: int = 20,
        max_ep_len: int = 1000,
    ):
        def new_buffer():
            return SegmentationBuffer(capacity, max_seq_len, max_ep_len)

        self.buffers = defaultdict(new_buffer)

    def add(self, batch: SampleBatchType):
        # Make a copy so the replay buffer doesn't pin plasma memory.
        batch = batch.copy()
        # Handle everything as if multi-agent.
        batch = batch.as_multi_agent()

        for policy_id, sample_batch in batch.policy_batches:
            self.buffers[policy_id].add(sample_batch)

    def sample(self, batch_size: int) -> MultiAgentBatch:
        samples = {}
        for policy_id, buffer in self.buffers.items():
            samples[policy_id] = buffer.sample(batch_size)
        return MultiAgentBatch(samples, sum(len(s) for s in samples.values()))
