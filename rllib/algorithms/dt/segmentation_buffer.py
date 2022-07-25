import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, concat_samples


class SegmentationBuffer:
    def __init__(self, size, max_seq_len):
        self.size = size
        self.max_seq_len = max_seq_len

        self.buffer = []

    def add(self, batch: SampleBatch):
        episodes = batch.split_by_episode()
        for episode in episodes:
            self._add_single(episode)

    def _add_single(self, episode: SampleBatch):
        if len(self.buffer) < self.size:
            self.buffer.append(episode)
        else:
            # TODO(charlesjsun): replace proportional to episode length
            replace_ind = np.random.randint(0, self.size)
            self.buffer[replace_ind] = episode

    def sample(self, batch_size: int) -> SampleBatch:
        samples = [self._sample_single() for _ in range(batch_size)]
        return concat_samples(samples)

    def _sample_single(self) -> SampleBatch:
        # TODO(charlesjsun): sample proportional to episode length
        buffer_ind = np.random.randint(0, len(self.buffer))
        episode: SampleBatch = self.buffer[buffer_ind]
        si = np.random.randint(0, episode[SampleBatch.OBS])

        # TODO(charlesjsun): is this numpy or torch tensors?
        obs = episode[SampleBatch.OBS][si:si+self.max_seq_len]
        actions = episode[SampleBatch.OBS][si:si+self.max_seq_len]
        # Note that returns to go needs one extra as the target for the last action
        returns_to_go = episode[SampleBatch.RETURNS_TO_GO][si:si+self.max_seq_len+1].reshape(-1, 1)
        timesteps = episode[SampleBatch.T][si:si+self.max_seq_len].reshape(-1)

        length = obs.shape[0]
        masks = np.ones(length, dtype=returns_to_go.dtype)

        # Back pad returns to go if at end
        if returns_to_go.shape[0] == length:
            returns_to_go = np.concatenate([returns_to_go, np.zeros(1, 1, dtype=returns_to_go.dtype)], axis=0)

        # Front-pad
        pad_length = self.max_seq_len - length
        if pad_length > 0:
            obs = np.concatenate([np.zeros((pad_length, *obs.shape[1:]), dtype=obs.dtype), obs], axis=0)
            actions = np.concatenate([np.zeros((pad_length, *actions.shape[1:]), dtype=actions.dtype), actions], axis=0)
            returns_to_go = np.concatenate([np.zeros((pad_length, 1), dtype=returns_to_go.dtype), returns_to_go], axis=0)
            timesteps = np.concatenate([np.zeros(pad_length, dtype=timesteps.dtype), timesteps], axis=0)
            masks = np.concatenate([np.zeros(pad_length, dtype=masks.dtype), masks], axis=0)

        # TODO(charlesjsun): debug only?
        assert obs.shape[0] == self.max_seq_len
        assert actions.shape[0] == self.max_seq_len
        assert timesteps.shape[0] == self.max_seq_len
        assert masks.shape[0] == self.max_seq_len
        assert returns_to_go.shape[0] == self.max_seq_len + 1

        return SampleBatch(**{
            SampleBatch.OBS: obs[None],
            SampleBatch.ACTIONS: actions[None],
            SampleBatch.RETURNS_TO_GO: returns_to_go[None],
            SampleBatch.T: timesteps[None],
            SampleBatch.ATTENTION_MASKS: masks[None],
        })
