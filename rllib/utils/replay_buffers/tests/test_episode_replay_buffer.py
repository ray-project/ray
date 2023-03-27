import unittest

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, concat_samples
from ray.rllib.utils.replay_buffers.episode_replay_buffer import EpisodeReplayBuffer
from ray.rllib.utils.replay_buffers.utils import ReplayBufferEpisode


class TestEpisodeReplayBuffer(unittest.TestCase):

    """def _add_data_to_buffer(self, _buffer, batch_size, num_batches=5, **kwargs):
        def _generate_data():
            return SampleBatch(
                {
                    SampleBatch.EPS_ID: [],
                    SampleBatch.ACTIONS: [np.random.choice([0, 1])],
                    SampleBatch.OBS: [np.random.random((4,))],
                    SampleBatch.REWARDS: [np.random.rand()],
                    SampleBatch.TERMINATEDS: [np.random.choice([False, True])],
                    SampleBatch.TRUNCATEDS: [np.random.choice([False, False])],
                }
            )

        for i in range(num_batches):
            data = [_generate_data() for _ in range(batch_size)]
            batch = concat_samples(data)
            _buffer.add(batch, **kwargs)
    """

    def test_episode_replay_buffer(self):
        buffer = EpisodeReplayBuffer(capacity=10000)

        def _get_episode_sample_batch():
            eps = ReplayBufferEpisode(observations=[0.0])
            ts = np.random.randint(1, 200)
            for t in range(ts):
                eps.add_timestep(
                    observation=float(t + 1),
                    action=int(t),
                    reward=0.1 * (t + 1),
                )
            eps.is_terminated = np.random.random() > 0.5
            eps.is_truncated = False if eps.is_terminated else np.random.random() > 0.8
            sample_batch = eps.to_sample_batch()
            return sample_batch

        for _ in range(200):
            batch = _get_episode_sample_batch()
            buffer.add(batch)

        for _ in range(1000):
            sample = buffer.sample(
                batch_size_B=16, batch_length_T=64
            )
            obs, actions, rewards, is_first, is_last, is_terminated, is_truncated = (
                sample["obs"], sample["actions"], sample["rewards"],
                sample["is_first"], sample["is_last"], sample["is_terminated"],
                sample["is_truncated"]
            )
            # Make sure terminated and truncated are never both True.
            assert not np.any(np.logical_and(is_truncated, is_terminated))

            # Make sure, is_first and is_last are trivially correct.
            assert np.all(is_last[:, -1])
            assert np.all(is_first[:, 0])

            # All fields have same shape.
            assert obs.shape[
                   :2] == rewards.shape == actions.shape == is_first.shape == is_last.shape == is_terminated.shape

            # All rewards match obs.
            assert np.all(np.equal(obs * 0.1, rewards))
            # All actions are always the same as their obs, except when terminated (one
            # less).
            assert np.all(np.where(is_last, True, np.equal(obs, actions)))
            # All actions on is_terminated=True must be the same as the previous ones
            # (we repeat the action b/c the last one is anyways a dummy one (action
            # picked in terminal observation/state)).
            assert np.all(np.where(is_terminated[:, 1:],
                                   np.equal(actions[:, 1:], actions[:, :-1]), True))
            # Where is_terminated, the next rewards should always be 0.0 (reset rewards).
            assert np.all(np.where(is_terminated[:, :-1], rewards[:, 1:] == 0.0, True))
